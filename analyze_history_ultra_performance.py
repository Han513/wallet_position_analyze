#!/usr/bin/env python3
"""
优化后的超高性能历史数据分析器
解决SQL语句过长问题，优化内存使用和并发性能
"""

import os
import math
import time
import logging
import traceback
import pandas as pd
import numpy as np
import asyncio
import threading
import multiprocessing as mp
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
from config import engine_trade, engine_smartmoney, CHAIN_IDS, BATCH_SIZE, SHARD_COUNT, SHARD_INDEX
from models import Transaction
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map
from more_itertools import chunked
import psutil
import io

logging.basicConfig(
    level=logging.INFO,
    format='[HISTORY_ULTRA] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("solana").setLevel(logging.WARNING)

CHECKPOINT_FILE_TEMPLATE = 'batch_checkpoint_signer_{}_{}.txt'

WSOL = 'So11111111111111111111111111111111111111112'
USDT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
STABLES = {USDT, USDC}

# 全局异步事件循环
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

class OptimizedUltraProcessor:
    """优化后的超高性能处理器"""
    
    def __init__(self):
        # 动态调整参数 - 更保守的设置
        self.cpu_count = psutil.cpu_count()
        self.memory_gb = psutil.virtual_memory().total / (1024**3)
        
        # 根据硬件配置动态调整
        self.max_workers = min(6, max(2, self.cpu_count // 3))  # 进一步减少并发
        self.batch_size = min(200, max(30, int(self.memory_gb * 15)))  # 减小批次
        self.connection_pool_size = min(30, max(8, self.cpu_count * 2))
        
        # **关键优化：大幅减小SQL批次大小**
        self.sql_batch_size = 30  # 减小单个SQL语句处理记录数
        self.wallet_sql_batch_size = 20  # wallet_buy_data更小的批次
        
        logging.info(f"硬件配置: CPU={self.cpu_count}, 内存={self.memory_gb:.1f}GB")
        logging.info(f"性能参数: workers={self.max_workers}, batch_size={self.batch_size}")
        logging.info(f"SQL批次大小: transaction={self.sql_batch_size}, wallet={self.wallet_sql_batch_size}")
        
        # 高性能数据库引擎 - 优化连接池配置
        self.engine_ultra = engine_smartmoney.execution_options(
            pool_size=self.connection_pool_size,
            max_overflow=self.connection_pool_size * 2,  # 增加溢出连接数
            pool_pre_ping=True,  # 启用连接检查
            pool_recycle=1800,  # 30分钟回收连接
            pool_timeout=30,  # 增加连接超时时间
            pool_reset_on_return='commit',  # 提交后重置连接
            echo=False  # 关闭SQL日志
        )
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        
        # 内存缓存优化
        self.position_cache = defaultdict(lambda: {
            'holding_amount': 0.0,
            'holding_cost': 0.0,
            'transactions': deque(maxlen=500)  # 减少缓存大小
        })
        
        # 统计信息
        self.stats = {
            'processed_transactions': 0,
            'processed_wallet_data': 0,
            'start_time': time.time(),
            'db_write_time': 0,
            'cache_compute_time': 0,
            'sql_error_count': 0,
            'copy_fail_count': 0,  # 新增：COPY失败计数
            'regular_insert_count': 0  # 新增：常规插入计数
        }

    def get_chain_name(self, chain_id):
        if str(chain_id) == "501":
            return "SOLANA"
        elif str(chain_id) == "9006":
            return "BSC"
        else:
            return str(chain_id)

    def determine_transaction_type(self, side, amount, current_holding):
        """快速判断交易类型"""
        if side == 'buy':
            return "build" if current_holding <= 1e-9 else "buy"
        else:  # sell
            if current_holding <= 1e-9:
                return "sell"
            elif amount >= current_holding - 1e-9:
                return "clean"
            else:
                return "sell"

    def safe_bigint(self, val):
        try:
            if val is None:
                return 0
            if isinstance(val, float) and (math.isnan(val) or val > 9223372036854775807 or val < -9223372036854775808):
                return 0
            if isinstance(val, int) and (val > 9223372036854775807 or val < -9223372036854775808):
                return 0
            return int(val)
        except Exception:
            return 0

    def safe_timestamp(self, timestamp):
        """安全的时间戳处理"""
        try:
            if timestamp is None:
                return int(time.time())
            
            ts = int(timestamp)
            
            # 处理毫秒时间戳
            if ts > 1e12:
                ts = int(ts / 1000)
            
            # 验证时间戳合理性 (1970-2050年之间)
            if ts < 0 or ts > 2524608000:  # 2050年
                logging.warning(f"异常时间戳: {timestamp}, 使用当前时间")
                return int(time.time())
                
            return ts
        except Exception as e:
            logging.warning(f"时间戳处理错误: {timestamp}, 错误: {e}")
            return int(time.time())

    def truncate_string(self, value, limit):
        if isinstance(value, str):
            return value[:limit]
        return value

    def optimized_bulk_insert(self, transactions):
        """优化的批量插入 - 使用COPY FROM"""
        if not transactions:
            return 0
        
        start_time = time.time()
        
        # 去重
        unique_txs = {}
        for tx in transactions:
            key = (tx['signature'], tx['wallet_address'], tx['token_address'], tx['transaction_time'])
            unique_txs[key] = tx
        
        deduped = list(unique_txs.values())
        if not deduped:
            return 0
        
        logging.info(f"开始批量插入 {len(deduped)} 笔交易数据")
        
        # **关键优化：使用更小的批次避免SQL过长**
        successful_inserts = 0
        batch_size = self.sql_batch_size
        
        # 串行处理小批次，避免连接池耗尽
        for i in range(0, len(deduped), batch_size):
            batch = deduped[i:i + batch_size]
            try:
                batch_result = self._single_batch_insert_optimized(batch, i // batch_size + 1)
                successful_inserts += batch_result
            except Exception as e:
                logging.error(f"批次 {i//batch_size + 1} 插入失败: {e}")
                self.stats['sql_error_count'] += 1
                
                # 如果批量失败，尝试单条插入
                for j, single_tx in enumerate(batch):
                    try:
                        single_result = self._single_batch_insert_optimized([single_tx], f"{i//batch_size + 1}-{j}")
                        successful_inserts += single_result
                    except Exception as single_e:
                        logging.debug(f"单条插入也失败: {single_e}")
        
        elapsed = time.time() - start_time
        self.stats['db_write_time'] += elapsed
        
        logging.info(f"批量插入完成: {successful_inserts}/{len(deduped)} 笔，耗时 {elapsed:.2f}s，TPS={successful_inserts/elapsed:.0f}")
        return successful_inserts

    def _single_batch_insert_optimized(self, batch, batch_num):
        """优化的单批次插入"""
        if not batch:
            return 0
            
        try:
            # 预处理数据
            processed_batch = []
            for r in batch:
                processed_row = r.copy()
                if 'token_name' in processed_row and processed_row['token_name']:
                    processed_row['token_name'] = self.truncate_string(processed_row['token_name'], 255)
                if 'from_token_symbol' in processed_row and processed_row['from_token_symbol']:
                    processed_row['from_token_symbol'] = self.truncate_string(processed_row['from_token_symbol'], 100)
                if 'dest_token_symbol' in processed_row and processed_row['dest_token_symbol']:
                    processed_row['dest_token_symbol'] = self.truncate_string(processed_row['dest_token_symbol'], 100)
                
                # 安全处理时间戳
                processed_row['transaction_time'] = self.safe_timestamp(processed_row['transaction_time'])
                processed_batch.append(processed_row)
            
            # **关键优化：使用COPY FROM代替大量INSERT**
            if len(processed_batch) > 10:
                return self._copy_from_insert(processed_batch, 'dex_query_v1.wallet_transaction')
            else:
                return self._regular_insert(processed_batch)
                
        except Exception as e:
            logging.error(f"批次 {batch_num} 处理失败: {e}")
            return 0

    def _copy_from_insert(self, batch, table_name):
        max_retries = 3
        retry_delay = 1
        for attempt in range(max_retries):
            conn = None
            cursor = None
            try:
                columns = list(batch[0].keys())
                csv_buffer = io.StringIO()
                for row in batch:
                    values = []
                    for col in columns:
                        value = row[col]
                        if value is None:
                            values.append('\\N')
                        elif isinstance(value, str):
                            escaped = value.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                            values.append(escaped)
                        elif isinstance(value, datetime):
                            values.append(value.isoformat())
                        else:
                            values.append(str(value))
                    csv_buffer.write('\t'.join(values) + '\n')
                csv_buffer.seek(0)
                conn = self.engine_ultra.raw_connection()
                cursor = conn.cursor()
                temp_table = f"temp_{table_name.replace('.', '_')}_{int(time.time() * 1000)}"
                columns_def = ', '.join([f"{col} TEXT" for col in columns])
                cursor.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table_name} INCLUDING ALL)")
                cursor.copy_from(csv_buffer, temp_table, columns=columns, sep='\t', null='\\N')
                columns_str = ', '.join(columns)
                update_clauses = []
                for col in columns:
                    if col not in ['signature', 'wallet_address', 'token_address', 'transaction_time']:
                        update_clauses.append(f"{col} = EXCLUDED.{col}")
                update_clause = ', '.join(update_clauses)
                insert_sql = f'''
                    INSERT INTO {table_name} ({columns_str})
                    SELECT {columns_str} FROM {temp_table}
                    ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                    DO UPDATE SET {update_clause}
                '''
                cursor.execute(insert_sql)
                conn.commit()
                return len(batch)
            except Exception as e:
                if attempt < max_retries - 1:
                    logging.warning(f"COPY FROM 尝试 {attempt + 1} 失败，等待 {retry_delay} 秒后重试: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    logging.error(f"COPY FROM 插入失败: {e}")
                    self.stats['copy_fail_count'] += 1
                    result = self._regular_insert(batch)
                    self.stats['regular_insert_count'] += 1
                    return result
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def _regular_insert(self, batch):
        """常规的参数化插入"""
        try:
            with self.engine_ultra.begin() as conn:
                columns = list(batch[0].keys())
                columns_str = ', '.join(columns)
                
                for row in batch:
                    placeholders = ', '.join([f":{key}" for key in row.keys()])
                    
                    update_clauses = []
                    for col in columns:
                        if col not in ['signature', 'wallet_address', 'token_address', 'transaction_time']:
                            update_clauses.append(f"{col} = EXCLUDED.{col}")
                    
                    update_clause = ', '.join(update_clauses)
                    
                    upsert_sql = text(f'''
                        INSERT INTO dex_query_v1.wallet_transaction ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                        DO UPDATE SET {update_clause}
                    ''')
                    
                    conn.execute(upsert_sql, row)
                
                return len(batch)
                
        except Exception as e:
            logging.error(f"常规插入失败: {e}")
            return 0

    def optimized_wallet_buy_data_compute(self, transactions):
        """优化的wallet_buy_data计算"""
        start_time = time.time()
        
        # 按钱包-代币对分组
        wallet_token_groups = defaultdict(list)
        for tx in transactions:
            key = (tx['wallet_address'], tx['token_address'], tx['chain'])
            wallet_token_groups[key].append(tx)
        
        # 减少并发数，避免过多连接
        max_workers = min(3, len(wallet_token_groups) // 10 + 1)
        all_wallet_buy_data = []
        
        if len(wallet_token_groups) <= 20:
            # 小批量直接串行处理
            for key, group_txs in wallet_token_groups.items():
                try:
                    result = self._compute_wallet_stats_fast(key, group_txs)
                    if result:
                        all_wallet_buy_data.extend(result)
                except Exception as e:
                    logging.error(f"计算wallet统计失败: {e}")
        else:
            # 大批量并行处理
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for key, group_txs in wallet_token_groups.items():
                    future = executor.submit(self._compute_wallet_stats_fast, key, group_txs)
                    futures.append(future)
                
                for future in as_completed(futures, timeout=30):
                    try:
                        result = future.result()
                        if result:
                            all_wallet_buy_data.extend(result)
                    except Exception as e:
                        logging.error(f"计算wallet统计失败: {e}")
        
        elapsed = time.time() - start_time
        self.stats['cache_compute_time'] += elapsed
        
        logging.info(f"wallet_buy_data计算完成: {len(all_wallet_buy_data)} 笔，耗时 {elapsed:.2f}s")
        return all_wallet_buy_data

    def _compute_wallet_stats_fast(self, key, transactions):
        """快速计算单个钱包-代币对的统计"""
        wallet_address, token_address, chain = key
        
        # 时间戳安全处理
        for tx in transactions:
            tx['transaction_time'] = self.safe_timestamp(tx['transaction_time'])
        
        # 按时间排序
        transactions.sort(key=lambda x: x['transaction_time'])
        
        # 按日期分组
        date_groups = defaultdict(list)
        for tx in transactions:
            try:
                date_key = datetime.fromtimestamp(tx['transaction_time'], timezone(timedelta(hours=8))).date()
                date_groups[date_key].append(tx)
            except Exception as e:
                logging.warning(f"日期转换失败: {tx['transaction_time']}, 错误: {e}")
                continue
        
        results = []
        all_historical_txs = []
        
        for date_key in sorted(date_groups.keys()):
            all_historical_txs.extend(date_groups[date_key])
            
            # 快速统计计算
            buys = [tx for tx in all_historical_txs if tx['transaction_type'] in ["build", "buy"]]
            sells = [tx for tx in all_historical_txs if tx['transaction_type'] in ["clean", "sell"]]
            
            # 基本统计
            total_buy_amount = sum(tx['amount'] for tx in buys)
            total_buy_cost = sum(tx['amount'] * tx['price'] for tx in buys)
            total_sell_amount = sum(tx['amount'] for tx in sells)
            total_sell_value = sum(tx['amount'] * tx['price'] for tx in sells)
            
            avg_buy_price = total_buy_cost / total_buy_amount if total_buy_amount > 0 else 0
            avg_sell_price = total_sell_value / total_sell_amount if total_sell_amount > 0 else 0
            
            current_position = max(0, total_buy_amount - total_sell_amount)
            current_cost = current_position * avg_buy_price if current_position > 0 else 0
            
            # P&L计算
            cost_of_goods_sold = total_sell_amount * avg_buy_price
            realized_profit = total_sell_value - cost_of_goods_sold
            realized_profit_percentage = (realized_profit / cost_of_goods_sold) * 100 if cost_of_goods_sold > 0 else 0
            
            # 限制百分比范围，避免异常值
            realized_profit_percentage = min(1000000, max(-100, realized_profit_percentage))
            
            chain_id = 501 if chain == 'SOLANA' else 9006 if chain == 'BSC' else 501
            
            record = {
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain_id': chain_id,
                'date': date_key,
                'total_amount': current_position,
                'total_cost': current_cost,
                'avg_buy_price': avg_buy_price,
                'position_opened_at': 0,
                'historical_total_buy_amount': total_buy_amount,
                'historical_total_buy_cost': total_buy_cost,
                'historical_total_sell_amount': total_sell_amount,
                'historical_total_sell_value': total_sell_value,
                'last_active_position_closed_at': 0,
                'historical_avg_buy_price': avg_buy_price,
                'historical_avg_sell_price': avg_sell_price,
                'last_transaction_time': max(tx['transaction_time'] for tx in all_historical_txs),
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'total_buy_count': len(buys),
                'total_sell_count': len(sells),
                'total_holding_seconds': 0,
                'chain': chain,
                'updated_at': datetime.now(timezone(timedelta(hours=8)))
            }
            results.append(record)
        
        return results

    def optimized_wallet_buy_data_insert(self, records):
        """优化的wallet_buy_data插入"""
        if not records:
            return 0
        
        start_time = time.time()
        
        # 去重
        unique_records = {}
        for record in records:
            key = (record['wallet_address'], record['token_address'], record['chain'], record['date'])
            unique_records[key] = record
        
        deduped_records = list(unique_records.values())
        logging.info(f"开始插入 {len(deduped_records)} 笔wallet_buy_data")
        
        # **关键优化：使用非常小的批次大小**
        batch_size = self.wallet_sql_batch_size
        successful_inserts = 0
        
        # 串行处理，避免SQL语句过长
        for i in range(0, len(deduped_records), batch_size):
            batch = deduped_records[i:i + batch_size]
            try:
                batch_result = self._single_wallet_buy_data_insert_optimized(batch, i // batch_size + 1)
                successful_inserts += batch_result
            except Exception as e:
                logging.error(f"wallet_buy_data批次 {i//batch_size + 1} 插入失败: {e}")
                self.stats['sql_error_count'] += 1
                
                # 如果批量失败，尝试单条插入
                for j, single_record in enumerate(batch):
                    try:
                        single_result = self._single_wallet_buy_data_insert_optimized([single_record], f"{i//batch_size + 1}-{j}")
                        successful_inserts += single_result
                    except Exception as single_e:
                        logging.debug(f"单条wallet_buy_data插入也失败: {single_e}")
        
        elapsed = time.time() - start_time
        logging.info(f"wallet_buy_data插入完成: {successful_inserts}/{len(deduped_records)} 笔，耗时 {elapsed:.2f}s")
        return successful_inserts

    def _single_wallet_buy_data_insert_optimized(self, batch, batch_num):
        """优化的单个wallet_buy_data批次插入"""
        try:
            with self.engine_ultra.begin() as conn:
                # 设置较短的超时时间
                conn.execute(text("SET statement_timeout = '30s'"))
                
                columns = list(batch[0].keys())
                columns_str = ', '.join(columns)
                
                if len(batch) == 1:
                    # 单条记录使用简单插入
                    row = batch[0]
                    placeholders = ', '.join([f":{key}" for key in row.keys()])
                    
                    update_clauses = []
                    for col in columns:
                        if col not in ['wallet_address', 'token_address', 'chain', 'date']:
                            update_clauses.append(f"{col} = EXCLUDED.{col}")
                    
                    update_clause = ', '.join(update_clauses)
                    
                    upsert_sql = text(f'''
                        INSERT INTO dex_query_v1.wallet_buy_data ({columns_str})
                        VALUES ({placeholders})
                        ON CONFLICT (wallet_address, token_address, chain, date) 
                        DO UPDATE SET {update_clause}
                    ''')
                    
                    conn.execute(upsert_sql, row)
                else:
                    # **关键优化：限制VALUES子句的长度**
                    # 构建批量插入，但要非常小心SQL长度
                    values_parts = []
                    params = {}
                    
                    for idx, row in enumerate(batch):
                        placeholders = []
                        for key, value in row.items():
                            param_name = f"{key}_{idx}"
                            params[param_name] = value
                            placeholders.append(f":{param_name}")
                        values_parts.append(f"({', '.join(placeholders)})")
                    
                    values_str = ', '.join(values_parts)
                    
                    update_clauses = []
                    for col in columns:
                        if col not in ['wallet_address', 'token_address', 'chain', 'date']:
                            update_clauses.append(f"{col} = EXCLUDED.{col}")
                    
                    update_clause = ', '.join(update_clauses)
                    
                    upsert_sql = text(f'''
                        INSERT INTO dex_query_v1.wallet_buy_data ({columns_str})
                        VALUES {values_str}
                        ON CONFLICT (wallet_address, token_address, chain, date) 
                        DO UPDATE SET {update_clause}
                    ''')
                    
                    conn.execute(upsert_sql, params)
                
                return len(batch)
                
        except Exception as e:
            logging.error(f"wallet_buy_data批次 {batch_num} 插入失败 (大小: {len(batch)}): {e}")
            return 0

    def print_performance_stats(self):
        """打印性能统计信息"""
        elapsed = time.time() - self.stats['start_time']
        logging.info("性能统计:")
        logging.info(f"总运行时间: {elapsed:.2f}秒")
        logging.info(f"处理交易数: {self.stats['processed_transactions']}")
        logging.info(f"处理钱包数据: {self.stats['processed_wallet_data']}")
        logging.info(f"数据库写入时间: {self.stats['db_write_time']:.2f}秒")
        logging.info(f"缓存计算时间: {self.stats['cache_compute_time']:.2f}秒")
        logging.info(f"SQL错误次数: {self.stats['sql_error_count']}")
        logging.info(f"COPY失败次数: {self.stats['copy_fail_count']}")
        logging.info(f"常规插入次数: {self.stats['regular_insert_count']}")
        if self.stats['processed_transactions'] > 0:
            logging.info(f"平均TPS: {self.stats['processed_transactions']/elapsed:.2f}")

# 其他函数保持不变...
def fetch_signers_batch(chain_id, last_signer):
    sql = f'''
    SELECT DISTINCT signer, {chain_id} AS chain_id
    FROM dex_query_v1.trades
    WHERE chain_id = {chain_id}
      AND signer > %(last_signer)s
      AND mod(abs(hashtext(signer)), {SHARD_COUNT}) = {SHARD_INDEX}
    ORDER BY signer
    LIMIT {BATCH_SIZE}
    '''
    return pd.read_sql(sql, con=engine_trade, params={"last_signer": last_signer})

@retry(wait=wait_random_exponential(min=2, max=6), stop=stop_after_attempt(3), retry=retry_if_exception_type(Exception))
def fetch_trades_for_batch(batch, chain_id, table_name, engine_trade):
    where_clauses = []
    params = {"chain_id": int(chain_id)}
    for i, (signer, min_time) in enumerate(batch):
        where_clauses.append(f"(signer = :signer_{i} AND timestamp >= :time_{i})")
        params[f"signer_{i}"] = signer
        params[f"time_{i}"] = int(min_time)
    where_sql = " OR ".join(where_clauses)
    trades_sql = text(f"""
        SELECT 
            signer,
            token_in,
            token_out,
            CASE WHEN side = 0 THEN token_in ELSE token_out END AS token_address,
            CASE WHEN side = 0 THEN 'buy' ELSE 'sell' END AS side,
            amount_in,
            amount_out,
            price,
            price_usd,
            decimals_in,
            decimals_out,
            timestamp,
            tx_hash,
            chain_id
        FROM {table_name}
        WHERE chain_id = :chain_id AND ({where_sql})
    """)
    try:
        df = pd.read_sql(trades_sql, con=engine_trade, params=params)
        logging.info(f"成功查询 {len(df)} 笔交易资料。")
        return df
    except Exception as e:
        logging.error("查询 SQL 发生错误：%s", e)
        raise

def get_symbol(token_info_dict, addr):
    info = token_info_dict.get(addr)
    if info and info.get("symbol"):
        return info["symbol"][:100]
    return None
def get_name(token_info_dict, addr):
    info = token_info_dict.get(addr)
    if info and info.get("token_name"):
        return info["token_name"][:255]
    return None

def get_icon(token_info_dict, addr):
    info = token_info_dict.get(addr)
    if info and info.get("token_icon"):
        return info["token_icon"]
    return None

def main():
    logging.info('=== 超高性能歷史數據分析器啟動 ===')
    
    processor = OptimizedUltraProcessor()
    
    try:
        for chain_id in CHAIN_IDS:
            logging.info(f"開始處理鏈 {chain_id}")
            
            checkpoint_file = CHECKPOINT_FILE_TEMPLATE.format(chain_id, SHARD_INDEX)
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    last_signer = f.read().strip()
            else:
                last_signer = ''
            
            while True:
                cycle_start_time = time.time()
                
                signers_df = fetch_signers_batch(chain_id, last_signer)
                if signers_df.empty:
                    logging.info(f"chain_id={chain_id} signer 掃描完畢。")
                    break
                    
                if signers_df['signer'].max() <= last_signer:
                    logging.warning(f'Checkpoint未推進，跳出避免死循環')
                    break
                
                logging.info(f"處理 signer: {signers_df['signer'].min()} ~ {signers_df['signer'].max()}，共 {len(signers_df)} 筆")
                
                try:
                    trades = fetch_trades_for_batch(
                        batch=signers_df.itertuples(index=False, name=None), 
                        chain_id=chain_id, 
                        table_name='dex_query_v1.trades', 
                        engine_trade=engine_trade
                    )
                except RetryError as re:
                    logging.error("fetch_trades_for_batch 最終失敗：%s", re)
                    continue
                
                if trades.empty:
                    last_signer = signers_df['signer'].max()
                    with open(checkpoint_file, 'w') as f:
                        f.write(last_signer)
                    continue

                # 數據處理和標準化（簡化版）
                def normalize_trade_fast(row):
                    token_in = row['token_in']
                    token_out = row['token_out']
                    side = row['side']
                    price = row['price_usd'] if 'price_usd' in row and not pd.isnull(row['price_usd']) else row['price']
                    
                    in_is_stable_or_wsol = token_in in STABLES or token_in == WSOL
                    out_is_stable_or_wsol = token_out in STABLES or token_out == WSOL
                    
                    if in_is_stable_or_wsol and not out_is_stable_or_wsol:
                        token_address = token_out
                        direction = 'buy'
                        amount = row['amount_out'] / (10 ** (row.get('decimals_out', 0) or 0))
                    elif out_is_stable_or_wsol and not in_is_stable_or_wsol:
                        token_address = token_in
                        direction = 'sell'
                        amount = row['amount_in'] / (10 ** (row.get('decimals_in', 0) or 0))
                    else:
                        token_address = token_out if side == 'buy' else token_in
                        direction = side
                        amount = (row['amount_out'] if side == 'buy' else row['amount_in']) / (10 ** (row.get('decimals_out' if side == 'buy' else 'decimals_in', 0) or 0))
                    
                    return {
                        'tx_hash': row['tx_hash'],
                        'signer': row['signer'],
                        'token_address': token_address,
                        'side': direction,
                        'amount': amount,
                        'price': price,
                        'timestamp': row['timestamp'],
                        'chain_id': row['chain_id'],
                        'token_in': token_in,
                        'token_out': token_out,
                        'amount_in': row['amount_in'] / (10 ** (row.get('decimals_in', 0) or 0)),
                        'amount_out': row['amount_out'] / (10 ** (row.get('decimals_out', 0) or 0))
                    }

                trades = trades.sort_values('timestamp')
                trades = trades.apply(normalize_trade_fast, axis=1, result_type='expand')
                trades = trades[trades['token_address'] != WSOL]
                
                # 快速獲取代幣信息（批量）
                all_tokens = pd.concat([trades['token_in'], trades['token_out'], trades['token_address']]).unique().tolist()
                token_info_dict = {}
                
                if all_tokens:
                    # 修復事件循環問題 - 使用同步方式獲取代幣信息
                    try:
                        # 分批處理，避免一次性處理太多
                        chunk_size = 50  # 減小批次大小
                        for chunk in chunked(all_tokens, chunk_size):
                            try:
                                # 直接在主事件循環中運行
                                chunk_result = loop.run_until_complete(get_token_info_batch_map(list(chunk), chain_id))
                                token_info_dict.update(chunk_result)
                            except Exception as e:
                                logging.warning(f"獲取代幣信息塊失敗: {e}")
                                # 如果批量失敗，嘗試單個獲取
                                for token in chunk:
                                    try:
                                        single_result = loop.run_until_complete(get_token_info_batch_map([token], chain_id))
                                        token_info_dict.update(single_result)
                                    except Exception as single_e:
                                        logging.debug(f"單個代幣 {token} 信息獲取失敗: {single_e}")
                    except Exception as e:
                        logging.error(f"代幣信息獲取整體失敗: {e}")
                        # 如果完全失敗，使用空字典繼續處理
                        token_info_dict = {}

                # 快速處理交易數據
                transaction_rows = []
                for (signer, token_address), group in trades.groupby(['signer', 'token_address']):
                    group = group.sort_values('timestamp')
                    holding_amount = 0
                    holding_cost = 0
                    original_holding_amount = 0
                    for trade_row in group.itertuples(index=False):
                        ts = int(trade_row.timestamp)
                        if ts > 1e12:
                            ts = int(ts / 1000)
                        # 判斷交易型態
                        transaction_type = processor.determine_transaction_type(
                            trade_row.side, trade_row.amount, holding_amount
                        )
                        # 查詢餘額（僅SOLANA）
                        wallet_balance = 0
                        if str(trade_row.chain_id) == "501":
                            # 這裡可根據實際情況查詢餘額，暫時設為0或可加快取法
                            wallet_balance = 0
                        token_info = token_info_dict.get(trade_row.token_address, {})
                        price = trade_row.price
                        supply = token_info.get('supply') or 0
                        try:
                            marketcap = price * float(supply)
                        except (ValueError, TypeError):
                            marketcap = 0
                        value = price * trade_row.amount
                        realized_profit = 0
                        realized_profit_percentage = 0
                        holding_percentage = 0
                        if trade_row.side == 'sell':
                            sell_amount = trade_row.amount
                            avg_cost = (holding_cost / holding_amount) if holding_amount > 0 else 0
                            realized_profit = (price - avg_cost) * sell_amount if avg_cost > 0 else 0
                            realized_profit_percentage = ((price / avg_cost - 1) * 100) if avg_cost > 0 else 0
                            original_holding_amount = holding_amount
                            holding_amount -= sell_amount
                            holding_cost -= avg_cost * sell_amount
                            if holding_amount < 0:
                                holding_amount = 0
                                holding_cost = 0
                            if original_holding_amount > 0:
                                holding_percentage = min(100, (sell_amount / original_holding_amount) * 100)
                            else:
                                holding_percentage = 0
                        elif trade_row.side == 'buy':
                            holding_amount += trade_row.amount
                            holding_cost += trade_row.amount * price
                            if wallet_balance > 0:
                                holding_percentage = min(100, (value / wallet_balance) * 100)
                            else:
                                holding_percentage = 0
                        token_icon = get_icon(token_info_dict, trade_row.token_address)
                        token_name = get_name(token_info_dict, trade_row.token_address)
                        from_token_symbol = get_symbol(token_info_dict, trade_row.token_in)
                        dest_token_symbol = get_symbol(token_info_dict, trade_row.token_out)
                        tx_data = {
                            'wallet_address': trade_row.signer,
                            'wallet_balance': wallet_balance,
                            'token_address': trade_row.token_address,
                            'token_icon': token_icon,
                            'token_name': token_name,
                            'price': price,
                            'amount': trade_row.amount,
                            'marketcap': marketcap,
                            'value': value,
                            'holding_percentage': holding_percentage,
                            'chain': processor.get_chain_name(trade_row.chain_id),
                            'chain_id': str(trade_row.chain_id),
                            'realized_profit': realized_profit,
                            'realized_profit_percentage': realized_profit_percentage,
                            'transaction_type': transaction_type,
                            'transaction_time': ts,
                            'time': datetime.now(timezone(timedelta(hours=8))),
                            'signature': trade_row.tx_hash,
                            'from_token_address': trade_row.token_in,
                            'from_token_symbol': from_token_symbol,
                            'from_token_amount': trade_row.amount_in,
                            'dest_token_address': trade_row.token_out,
                            'dest_token_symbol': dest_token_symbol,
                            'dest_token_amount': trade_row.amount_out,
                        }
                        transaction_rows.append(tx_data)

                # 超高速批量處理
                if transaction_rows:
                    logging.info(f'[超高速處理] 準備處理 {len(transaction_rows)} 筆交易')
                    
                    # 並行執行插入和計算
                    insert_future = processor.executor.submit(processor.optimized_bulk_insert, transaction_rows)
                    compute_future = processor.executor.submit(processor.optimized_wallet_buy_data_compute, transaction_rows)
                    
                    # 等待插入完成
                    insert_count = insert_future.result()
                    processor.stats['processed_transactions'] += insert_count
                    
                    # 等待計算完成並插入wallet_buy_data
                    wallet_buy_data = compute_future.result()
                    if wallet_buy_data:
                        wallet_insert_count = processor.optimized_wallet_buy_data_insert(wallet_buy_data)
                        processor.stats['processed_wallet_data'] += wallet_insert_count
                
                # 更新checkpoint
                last_signer = signers_df['signer'].max()
                with open(checkpoint_file, 'w') as f:
                    f.write(last_signer)
                
                cycle_time = time.time() - cycle_start_time
                logging.info(f"處理完成 signer <= {last_signer}，本輪耗時: {cycle_time:.2f}s")
        
        processor.print_performance_stats()
        
    finally:
        processor.executor.shutdown(wait=True)
        processor.engine_ultra.dispose()
    
    logging.info("超高性能歷史分析完成")

if __name__ == '__main__':
    main() 