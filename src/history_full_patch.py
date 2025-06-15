import os
import math
import time
import logging
import pandas as pd
import numpy as np
import asyncio
import psutil
import io
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
from queue import Queue
from threading import Thread, Event
import queue  # 修正1: 補 import
import threading

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

# 全局异步事件循环（主線程專用，並用 thread 跑 forever）
loop = asyncio.new_event_loop()
def start_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()
threading.Thread(target=start_loop, args=(loop,), daemon=True).start()

# 新增：自動判斷事件循環狀態的工具函數

def run_async(coro):
    # 只用主線程創建的 loop
    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()
    else:
        return loop.run_until_complete(coro)

class HistoryFullPatch:
    """历史数据全量+补丁分析器"""
    
    def __init__(self, start_date=None, end_date=None):
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
        
        # 添加停止事件
        self.stop_event = threading.Event()
        
        # 设置时间范围
        self.start_date = start_date
        self.end_date = end_date
        
        # 转换日期为毫秒时间戳
        if self.start_date:
            self.start_timestamp = int(datetime.combine(self.start_date, datetime.min.time()).timestamp() * 1000)
        else:
            self.start_timestamp = None
            
        if self.end_date:
            self.end_timestamp = int(datetime.combine(self.end_date, datetime.max.time()).timestamp() * 1000)
        else:
            self.end_timestamp = None
        
        logging.info(f"硬件配置: CPU={self.cpu_count}, 内存={self.memory_gb:.1f}GB")
        logging.info(f"性能参数: workers={self.max_workers}, batch_size={self.batch_size}")
        logging.info(f"SQL批次大小: transaction={self.sql_batch_size}, wallet={self.wallet_sql_batch_size}")
        if self.start_date and self.end_date:
            logging.info(f"分析时间范围: {self.start_date} 至 {self.end_date}")
        
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
        for date_key in sorted(date_groups.keys()):
            day_txs = date_groups[date_key]
            # 只用當天的交易
            buys = [tx for tx in day_txs if tx['transaction_type'] in ["build", "buy"]]
            sells = [tx for tx in day_txs if tx['transaction_type'] in ["clean", "sell"]]

            # --- 新版加權平均法持倉與利潤計算 ---
            current_position = 0
            current_cost = 0
            avg_buy_price = 0
            realized_profit = 0
            for tx in sorted(day_txs, key=lambda x: x['transaction_time']):
                if tx['transaction_type'] in ["build", "buy"]:
                    current_cost += tx['amount'] * tx['price']
                    current_position += tx['amount']
                    avg_buy_price = current_cost / current_position if current_position > 0 else 0
                elif tx['transaction_type'] in ["clean", "sell"]:
                    if current_position > 0:
                        realized_profit += (tx['price'] - avg_buy_price) * tx['amount']
                        current_cost -= avg_buy_price * tx['amount']
                        current_position -= tx['amount']
                        if current_position <= 0:
                            current_position = 0
                            current_cost = 0
                            avg_buy_price = 0
                    else:
                        pass  # 沒有持倉還賣出，不計算利潤
            realized_profit_percentage = (realized_profit / (abs(current_cost) if current_cost else 1)) * 100 if realized_profit != 0 else 0

            # 當天持倉秒數計算
            position = 0
            position_start_time = None
            total_holding_seconds = 0
            last_active_position_closed_at = None
            for tx in sorted(day_txs, key=lambda x: x['transaction_time']):
                if tx['transaction_type'] in ["build", "buy"]:
                    if position == 0:
                        position_start_time = tx['transaction_time']
                    position += tx['amount']
                elif tx['transaction_type'] in ["clean", "sell"]:
                    position -= tx['amount']
                    if position <= 0 and position_start_time is not None:
                        total_holding_seconds += tx['transaction_time'] - position_start_time
                        last_active_position_closed_at = tx['transaction_time']
                        position_start_time = None
                    if position < 0:
                        position = 0
            if position > 0 and position_start_time is not None:
                max_time = max(tx['transaction_time'] for tx in day_txs)
                total_holding_seconds += max_time - position_start_time

            # 歷史第一筆買入時間
            all_buys = [tx for tx in transactions if tx['transaction_type'] in ["build", "buy"]]
            first_buy_time = min([tx['transaction_time'] for tx in all_buys]) if all_buys else 0

            chain_id = 501 if chain == 'SOLANA' else 9006 if chain == 'BSC' else 501

            record = {
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain_id': chain_id,
                'date': date_key,
                'total_amount': current_position,
                'total_cost': current_cost,
                'avg_buy_price': avg_buy_price,
                'position_opened_at': first_buy_time,
                'historical_total_buy_amount': sum(tx['amount'] for tx in buys),
                'historical_total_buy_cost': sum(tx['amount'] * tx['price'] for tx in buys),
                'historical_total_sell_amount': sum(tx['amount'] for tx in sells),
                'historical_total_sell_value': sum(tx['amount'] * tx['price'] for tx in sells),
                'last_active_position_closed_at': 0,
                'historical_avg_buy_price': avg_buy_price,
                'historical_avg_sell_price': realized_profit_percentage,
                'last_transaction_time': max(tx['transaction_time'] for tx in day_txs),
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'total_buy_count': len(buys),
                'total_sell_count': len(sells),
                'total_holding_seconds': total_holding_seconds,
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
def fetch_trades_for_batch(batch, chain_id, table_name, engine_trade, start_timestamp=None, end_timestamp=None):
    where_clauses = []
    params = {"chain_id": int(chain_id)}
    
    # 添加时间范围条件
    time_conditions = []
    if start_timestamp is not None:
        time_conditions.append("timestamp >= :start_timestamp")
        params["start_timestamp"] = start_timestamp
    if end_timestamp is not None:
        time_conditions.append("timestamp <= :end_timestamp")
        params["end_timestamp"] = end_timestamp
    
    for i, (signer, min_time) in enumerate(batch):
        where_clauses.append(f"(signer = :signer_{i} AND timestamp >= :time_{i})")
        params[f"signer_{i}"] = signer
        params[f"time_{i}"] = int(min_time)
    
    where_sql = " OR ".join(where_clauses)
    if time_conditions:
        where_sql = f"({where_sql}) AND {' AND '.join(time_conditions)}"
    
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
        WHERE chain_id = :chain_id AND {where_sql}
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

def create_processed_signers_table(engine):
    """創建臨時表用於記錄已處理的signer"""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dex_query_v1.temp_processed_signers (
                signer TEXT PRIMARY KEY,
                processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                chain_id INTEGER,
                batch_id INTEGER
            )
        """))
        # 創建索引以加速查詢
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_temp_processed_signers_chain_id 
            ON dex_query_v1.temp_processed_signers(chain_id)
        """))

def get_processed_signers(engine, chain_id):
    """獲取已處理的signer列表"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT signer 
            FROM dex_query_v1.temp_processed_signers 
            WHERE chain_id = :chain_id
        """), {"chain_id": chain_id})
        return {row[0] for row in result}

def mark_signers_as_processed(engine, signers, chain_id, batch_id):
    """標記signer為已處理"""
    if not signers:
        return
    
    with engine.begin() as conn:
        for signer in signers:
            conn.execute(text("""
                INSERT INTO dex_query_v1.temp_processed_signers (signer, chain_id, batch_id)
                VALUES (:signer, :chain_id, :batch_id)
                ON CONFLICT (signer) DO UPDATE 
                SET processed_at = CURRENT_TIMESTAMP,
                    batch_id = :batch_id
            """), {
                "signer": signer,
                "chain_id": chain_id,
                "batch_id": batch_id
            })

def fetch_trades_batch(engine, last_id, batch_size, chain_id):
    """高效主鍵分頁掃描trades表，只查id, signer"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT id, signer
            FROM dex_query_v1.trades
            WHERE chain_id = :chain_id
              AND id > :last_id
            ORDER BY id
            LIMIT :batch_size
        """), {
            "chain_id": int(chain_id),
            "last_id": int(last_id),
            "batch_size": int(batch_size)
        })
        return pd.DataFrame(result.fetchall(), columns=result.keys())

def signer_collector(chain_id, signer_queue, stop_event):
    """高效主鍵分頁掃描trades表，累積新signer夠一批就分析"""
    try:
        last_id = 0
        batch_size = 1000
        signer_batch_size = 500
        processed_signers = get_processed_signers(engine_smartmoney, chain_id)
        logging.info(f"已處理的signer數量: {len(processed_signers)}")
        current_signers = set()
        while not stop_event.is_set():
            trades_df = fetch_trades_batch(
                engine=engine_trade,
                last_id=last_id,
                batch_size=batch_size,
                chain_id=chain_id
            )
            if trades_df.empty:
                # 處理剩餘的signer
                if current_signers:
                    signer_queue.put(list(current_signers))
                    logging.info(f"最後一批 {len(current_signers)} 個signer 已送出分析")
                logging.info(f"完成所有signer收集，最後ID: {last_id}")
                break
            last_id = int(trades_df['id'].max())
            new_signers = set(trades_df['signer'].unique()) - processed_signers - current_signers
            if new_signers:
                current_signers.update(new_signers)
                logging.info(f"已處理到ID: {last_id}, 新增signer數: {len(new_signers)}, 當前累積: {len(current_signers)}")
            # 累積夠一批就送分析
            while len(current_signers) >= signer_batch_size:
                batch = set(list(current_signers)[:signer_batch_size])
                signer_queue.put(list(batch))
                current_signers -= batch
                logging.info(f"送出一批 {len(batch)} 個signer 進行分析，剩餘累積: {len(current_signers)}")
    except Exception as e:
        logging.error(f"收集signer時發生錯誤: {e}")
        stop_event.set()

def signer_processor(chain_id, signer_queue, stop_event, processor):
    batch_id = 0
    batch_size = 5  # 小批量
    current_batch = []
    max_workers = 4  # 可根據硬體調整
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            while not stop_event.is_set() or not signer_queue.empty():
                try:
                    new_signers = signer_queue.get(timeout=0.1)
                    if new_signers is None:
                        break
                    current_batch.extend(new_signers)
                    while len(current_batch) >= batch_size:
                        batch_id += 1
                        signers_to_process = current_batch[:batch_size]
                        current_batch = current_batch[batch_size:]
                        logging.info(f"送出第 {batch_id} 批 {len(signers_to_process)} 個signer 進行分析")
                        future = executor.submit(
                            process_signers_batch,
                            signers_to_process,
                            chain_id,
                            processor.start_timestamp,
                            processor.end_timestamp,
                            processor
                        )
                        futures.append((future, signers_to_process, batch_id))
                    # 處理已完成的future
                    done_futures = [f for f, _, _ in futures if f.done()]
                    for f, signers, bid in list(futures):
                        if f.done():
                            if f.result():
                                mark_signers_as_processed(
                                    engine_smartmoney,
                                    signers,
                                    chain_id,
                                    bid
                                )
                                logging.info(f"第 {bid} 批處理完成，已寫入temp_processed_signers")
                            futures.remove((f, signers, bid))
                except queue.Empty:
                    continue
                except KeyboardInterrupt:
                    logging.warning("消費者收到中斷訊號，準備退出...")
                    stop_event.set()
                    break
                except Exception as e:
                    logging.error(f"處理signer時發生錯誤: {e}")
                    continue
            # 處理剩餘的
            if current_batch:
                batch_id += 1
                logging.info(f"處理最後一批 {len(current_batch)} 個signer")
                future = executor.submit(
                    process_signers_batch,
                    current_batch,
                    chain_id,
                    processor.start_timestamp,
                    processor.end_timestamp,
                    processor
                )
                futures.append((future, current_batch, batch_id))
            # 等所有future完成
            for f, signers, bid in futures:
                if f.result():
                    mark_signers_as_processed(
                        engine_smartmoney,
                        signers,
                        chain_id,
                        bid
                    )
                    logging.info(f"第 {bid} 批處理完成，已寫入temp_processed_signers")
    except Exception as e:
        logging.error(f"處理signer時發生錯誤: {e}")
        stop_event.set()

def process_signers_batch(signers, chain_id, start_ts, end_ts, processor):
    try:
        logging.info(f"分析批次: {len(signers)} 個signer")
        if not signers:
            return True
            
        # 創建線程池，使用CPU核心數的2倍作為線程數
        max_workers = min(32, os.cpu_count() * 2)  # 最多32個線程
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            # 為每個signer創建查詢任務
            for signer in signers:
                if processor.stop_event.is_set():  # 檢查是否需要停止
                    logging.info("收到停止信號，終止處理")
                    return False
                    
                future = executor.submit(
                    _process_single_signer,
                    signer,
                    start_ts,
                    end_ts,
                    processor
                )
                futures.append(future)
            
            # 等待所有任務完成
            all_transactions = []
            for future in as_completed(futures):
                if processor.stop_event.is_set():  # 再次檢查是否需要停止
                    logging.info("收到停止信號，終止處理")
                    return False
                    
                try:
                    result = future.result()
                    if result:
                        all_transactions.extend(result)
                except Exception as e:
                    logging.error(f"處理signer時發生錯誤: {e}")
                    continue
            
            # 如果有交易數據，進行批量處理
            if all_transactions and not processor.stop_event.is_set():
                logging.info(f"本批次共處理 {len(all_transactions)} 筆交易")
                try:
                    # 批量插入交易數據
                    insert_count = processor.optimized_bulk_insert(all_transactions)
                    logging.info(f"成功插入 {insert_count} 筆交易數據")
                    
                    # 計算並插入wallet_buy_data
                    wallet_buy_data = processor.optimized_wallet_buy_data_compute(all_transactions)
                    if wallet_buy_data:
                        wallet_insert_count = processor.optimized_wallet_buy_data_insert(wallet_buy_data)
                        logging.info(f"成功插入 {wallet_insert_count} 筆wallet_buy_data")
                except Exception as e:
                    logging.error(f"批量處理數據時發生錯誤: {e}")
                    return False
            
            return True
            
    except Exception as e:
        logging.error(f"處理signer批次時發生錯誤: {e}")
        return False

def _process_single_signer(signer, start_ts, end_ts, processor):
    try:
        logging.info(f"處理signer: {signer}")
        trades_sql = text('''
            SELECT
                signer, token_in, token_out,
                CASE WHEN side = 0 THEN token_in ELSE token_out END AS token_address,
                CASE WHEN side = 0 THEN 'buy' ELSE 'sell' END AS side,
                amount_in, amount_out, price, price_usd, decimals_in, decimals_out,
                timestamp, tx_hash, chain_id
            FROM dex_query_v1.trades
            WHERE signer = :signer
              AND timestamp BETWEEN :start_ts AND :end_ts
        ''')
        
        params = {
            "signer": signer,
            "start_ts": int(start_ts) if start_ts else 0,
            "end_ts": int(end_ts) if end_ts else 9999999999
        }
        
        # 執行查詢
        with engine_trade.connect() as conn:
            df = pd.read_sql(trades_sql, conn, params=params)
            
        if df.empty:
            logging.info(f"signer {signer} 沒有交易數據")
            return []
            
        logging.info(f"查詢到 {len(df)} 筆交易")
        
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
        
        # 標準化交易數據
        df = df.sort_values('timestamp')
        df = df.apply(normalize_trade_fast, axis=1, result_type='expand')
        df = df[df['token_address'] != WSOL]
        
        # 獲取代幣信息
        all_tokens = pd.concat([df['token_in'], df['token_out'], df['token_address']]).unique().tolist()
        token_info_dict = {}
        
        if all_tokens:
            try:
                chunk_size = 50
                for chunk in chunked(all_tokens, chunk_size):
                    if processor.stop_event.is_set():  # 檢查是否需要停止
                        return []
                    # 從df中獲取chain_id
                    chain_id = df['chain_id'].iloc[0]
                    chunk_result = run_async(get_token_info_batch_map(list(chunk), chain_id))
                    token_info_dict.update(chunk_result)
            except Exception as e:
                logging.warning(f"獲取代幣信息失敗: {e}")
        
        # 處理交易數據
        transaction_rows = []
        for (_, token_address), group in df.groupby(['signer', 'token_address']):
            if processor.stop_event.is_set():  # 檢查是否需要停止
                return []
                
            group = group.sort_values('timestamp')
            holding_amount = 0
            holding_cost = 0
            original_holding_amount = 0
            
            for trade_row in group.itertuples(index=False):
                if processor.stop_event.is_set():  # 檢查是否需要停止
                    return []
                    
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
                
        return transaction_rows
        
    except Exception as e:
        logging.error(f"處理signer {signer} 時發生錯誤: {e}")
        return []

def main(start_date=None, end_date=None):
    logging.info('=== 历史数据全量+补丁分析器启动 ===')
    processor = HistoryFullPatch(start_date, end_date)
    try:
        create_processed_signers_table(engine_smartmoney)
        for chain_id in CHAIN_IDS:
            logging.info(f"開始處理鏈 {chain_id}")
            signer_queue = Queue()
            stop_event = Event()
            collector = Thread(
                target=signer_collector,
                args=(chain_id, signer_queue, stop_event)
            )
            processor_thread = Thread(
                target=signer_processor,
                args=(chain_id, signer_queue, stop_event, processor)
            )
            collector.start()
            processor_thread.start()
            try:
                collector.join()
            except KeyboardInterrupt:
                logging.warning("主線程收到中斷訊號，準備退出...")
                stop_event.set()
                signer_queue.put(None)
                processor_thread.join()
                return
            stop_event.set()
            signer_queue.put(None)
            processor_thread.join()
        processor.print_performance_stats()
    finally:
        processor.engine_ultra.dispose()
    logging.info("历史数据全量+补丁分析完成")

if __name__ == '__main__':
    main()