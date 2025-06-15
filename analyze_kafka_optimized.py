import logging
import traceback
import json
import math
import pandas as pd
import asyncio
import time
from collections import defaultdict
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from datetime import datetime, timezone, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text, select
from threading import Lock
import threading
import queue
import calendar
import psycopg2

from config import engine_smartmoney, KAFKA_BROKER, KAFKA_TOPIC
from models import Transaction, WalletBuyData
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map

logging.basicConfig(
    level=logging.INFO,
    format='[KAFKA_OPT] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("solana").setLevel(logging.WARNING)

# 創建一個帶連接池的引擎
engine_optimized = engine_smartmoney.execution_options(
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True,
    pool_recycle=3600
)

Session = sessionmaker(bind=engine_optimized)

KAFKA_QUEUE_MAXSIZE = 20000  # 根據內存和流量調整
BATCH_SIZE = 100
FLUSH_INTERVAL = 2  # 秒
PROCESS_THREAD_NUM = 4  # 可根據CPU核數和DB壓力調整

kafka_queue = queue.Queue(maxsize=KAFKA_QUEUE_MAXSIZE)

# ========== 新增：分流buffer ===========
partition_buffers = [queue.Queue(maxsize=KAFKA_QUEUE_MAXSIZE) for _ in range(PROCESS_THREAD_NUM)]

def get_partition_idx(chain, date):
    """根據chain和月份自動分配到不同線程"""
    # date: datetime.date or str (YYYY-MM-DD)
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d").date()
    # 以鏈和月份hash分配
    month = date.month if hasattr(date, 'month') else int(str(date)[5:7])
    chain_hash = hash(chain)
    idx = (chain_hash + month) % PROCESS_THREAD_NUM
    return idx

class OptimizedKafkaProcessor:
    """優化版Kafka處理器"""
    
    def __init__(self):
        self.transaction_buffer = []
        self.wallet_buy_data_buffer = []
        self.buffer_lock = Lock()
        self.max_buffer_size = 100
        self.last_flush_time = time.time()
        self.flush_interval = 30  # 30秒強制刷新一次
        
        # 內存緩存用於快速計算
        self.position_cache = defaultdict(lambda: {
            'holding_amount': 0,
            'holding_cost': 0,
            'last_update': 0
        })
        
    def truncate_string(self, value, limit):
        if isinstance(value, str):
            return value[:limit]
        return value

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

    def determine_transaction_type(self, side, amount, current_holding):
        """
        判斷交易型態
        
        Args:
            side: 'buy' 或 'sell'
            amount: 交易數量
            current_holding: 當前持倉數量
        
        Returns:
            str: "clean"=清倉, "build"=建倉, "buy"=加倉, "sell"=減倉
        """
        if side == 'buy':
            if current_holding <= 1e-9:  # 基本上沒有持倉 (考慮浮點精度)
                return "build"  # 建倉
            else:
                return "buy"  # 加倉
        elif side == 'sell':
            if current_holding <= 1e-9:  # 沒有持倉卻在賣出 (理論上不應該發生，但防禦性處理)
                return "sell"  # 減倉
            elif amount >= current_holding - 1e-9:  # 賣出數量 >= 持倉數量 (考慮浮點精度)
                return "clean"  # 清倉
            else:
                return "sell"  # 減倉
        else:
            # 未知交易類型，默認返回建倉
            return "build"

    def calculate_realized_profit(self, wallet_address, token_address, chain, transaction_type, amount, price, transaction_time):
        """計算已實現收益，使用內存緩存提高性能"""
        key = (wallet_address, token_address, chain)
        position = self.position_cache[key]
        
        realized_profit = 0
        realized_profit_percentage = 0
        
        if transaction_type in ["build", "buy"]:  # 建倉或加倉 (買入)
            position['holding_amount'] += amount
            position['holding_cost'] += amount * price
        elif transaction_type in ["clean", "sell"]:  # 清倉或減倉 (賣出)
            if position['holding_amount'] > 0:
                avg_cost = position['holding_cost'] / position['holding_amount']
                realized_profit = (price - avg_cost) * amount
                realized_profit_percentage = ((price / avg_cost - 1) * 100) if avg_cost > 0 else 0
                
                # 更新持倉
                position['holding_amount'] -= amount
                position['holding_cost'] -= avg_cost * amount
                
                if position['holding_amount'] < 0:
                    position['holding_amount'] = 0
                    position['holding_cost'] = 0
        
        position['last_update'] = transaction_time
        
        return realized_profit, realized_profit_percentage

    def upsert_transaction_optimized(self, tx_data):
        """優化的單筆交易插入"""
        session = Session()
        try:
            stmt = insert(Transaction).values(**tx_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['signature', 'wallet_address', 'token_address', 'transaction_time'],
                set_={k: v for k, v in tx_data.items() if k not in ['signature', 'wallet_address', 'token_address', 'transaction_time']}
            )
            session.execute(stmt)
            session.commit()
            logging.debug(f"成功插入transaction: {tx_data['signature'][:20]}...")
        except Exception as e:
            session.rollback()
            logging.error(f"插入transaction失敗: {str(e)}")
            raise
        finally:
            session.close()

    def bulk_upsert_transactions(self, transactions):
        """批量插入交易記錄"""
        if not transactions:
            return
        
        start_time = time.time()
        logging.info(f"開始批量插入 {len(transactions)} 筆交易記錄")
        
        # 去重處理 - 按主鍵去重
        unique_transactions = {}
        for tx in transactions:
            key = (tx['signature'], tx['wallet_address'], tx['token_address'], tx['transaction_time'])
            unique_transactions[key] = tx
        
        deduped_transactions = list(unique_transactions.values())
        
        if len(deduped_transactions) != len(transactions):
            logging.warning(f"發現重複交易記錄: 原始{len(transactions)}筆 -> 去重後{len(deduped_transactions)}筆")
        
        batch_size = 50
        successful_inserts = 0
        
        for i in range(0, len(deduped_transactions), batch_size):
            batch = deduped_transactions[i:i + batch_size]
            
            try:
                with engine_optimized.begin() as conn:
                    # 設置search_path確保操作在正確的schema中
                    conn.execute(text("SET search_path TO dex_query_v1, public"))
                    
                    # 預處理數據
                    for r in batch:
                        if 'token_name' in r and r['token_name']:
                            r['token_name'] = self.truncate_string(r['token_name'], 255)
                        if 'from_token_symbol' in r and r['from_token_symbol']:
                            r['from_token_symbol'] = self.truncate_string(r['from_token_symbol'], 100)
                        if 'dest_token_symbol' in r and r['dest_token_symbol']:
                            r['dest_token_symbol'] = self.truncate_string(r['dest_token_symbol'], 100)
                    
                    # 構建批量插入SQL
                    columns = list(batch[0].keys())
                    columns_str = ', '.join(columns)
                    
                    values_clauses = []
                    params = {}
                    
                    for idx, row in enumerate(batch):
                        placeholders = []
                        for key, value in row.items():
                            param_name = f"{key}_{idx}"
                            params[param_name] = value
                            placeholders.append(f":{param_name}")
                        values_clauses.append(f"({', '.join(placeholders)})")
                    
                    values_str = ', '.join(values_clauses)
                    
                    # 構建UPDATE子句
                    update_clauses = []
                    for col in columns:
                        if col not in ['signature', 'wallet_address', 'token_address', 'transaction_time']:
                            update_clauses.append(f"{col} = EXCLUDED.{col}")
                    
                    update_clause = ', '.join(update_clauses)
                    
                    upsert_sql = text(f'''
                        INSERT INTO dex_query_v1.wallet_transaction ({columns_str})
                        VALUES {values_str}
                        ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                        DO UPDATE SET {update_clause}
                    ''')
                    
                    conn.execute(upsert_sql, params)
                    successful_inserts += len(batch)
                    
            except Exception as e:
                logging.error(f"批次 {i//batch_size + 1} 插入失敗: {str(e)}")
                logging.error(f"失敗批次包含主鍵: {[(r['signature'][:20], r['wallet_address'][:20], r['token_address'][:20], r['transaction_time']) for r in batch[:3]]}")
                raise
        
        total_time = time.time() - start_time
        logging.info(f"批量插入完成！成功插入/更新記錄數: {successful_inserts}，總耗時: {total_time:.2f}s")

    def calculate_wallet_buy_data_incremental(self, wallet_address, token_address, chain, new_transaction):
        """增量計算wallet_buy_data，避免重新查詢整個歷史"""
        session = None
        try:
            # 獲取當前日期的統計
            ts = new_transaction['transaction_time']
            date_key = datetime.fromtimestamp(ts, timezone(timedelta(hours=8))).date()
            
            session = Session()
            
            # 查詢該日期及之前的所有交易（改進SQL查詢）
            try:
                # 計算日期範圍，避免使用複雜的SQL函數
                date_start = int(datetime.combine(date_key, datetime.min.time()).replace(tzinfo=timezone(timedelta(hours=8))).timestamp())
                date_end = int(datetime.combine(date_key + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone(timedelta(hours=8))).timestamp())
                
                # 分步查詢，先查詢當日之前的所有交易
                historical_transactions = session.query(Transaction).filter(
                    Transaction.wallet_address == wallet_address,
                    Transaction.token_address == token_address,
                    Transaction.chain == chain,
                    Transaction.transaction_time <= ts
                ).order_by(Transaction.transaction_time).all()
                
                if not historical_transactions:
                    # 如果沒有歷史交易，只處理當前交易
                    daily_df = pd.DataFrame([{
                        'wallet_address': new_transaction['wallet_address'],
                        'token_address': new_transaction['token_address'],
                        'chain': new_transaction['chain'],
                        'chain_id': new_transaction['chain_id'],
                        'transaction_type': new_transaction['transaction_type'],
                        'amount': new_transaction['amount'],
                        'price': new_transaction['price'],
                        'transaction_time': new_transaction['transaction_time'],
                        'date': date_key
                    }])
                else:
                    # 將歷史交易轉換為DataFrame
                    daily_df = pd.DataFrame([{
                        'wallet_address': tx.wallet_address,
                        'token_address': tx.token_address,
                        'chain': tx.chain,
                        'chain_id': tx.chain_id,
                        'transaction_type': tx.transaction_type,
                        'amount': tx.amount,
                        'price': tx.price,
                        'transaction_time': tx.transaction_time,
                        'date': datetime.fromtimestamp(tx.transaction_time, timezone(timedelta(hours=8))).date()
                    } for tx in historical_transactions])
                    
                    # 加入新交易
                    new_tx_df = pd.DataFrame([{
                        'wallet_address': new_transaction['wallet_address'],
                        'token_address': new_transaction['token_address'],
                        'chain': new_transaction['chain'],
                        'chain_id': new_transaction['chain_id'],
                        'transaction_type': new_transaction['transaction_type'],
                        'amount': new_transaction['amount'],
                        'price': new_transaction['price'],
                        'transaction_time': new_transaction['transaction_time'],
                        'date': date_key
                    }])
                    
                    daily_df = pd.concat([daily_df, new_tx_df], ignore_index=True)
                
                # 按日期分組計算各日統計
                all_stats = []
                unique_dates = sorted(daily_df['date'].unique())
                
                for d in unique_dates:
                    day_transactions = daily_df[daily_df['date'] <= d].copy()
                    if not day_transactions.empty:
                        stats = self.calc_wallet_token_stats(day_transactions)
                        if stats is not None:
                            all_stats.append(stats.to_dict())
                
                return all_stats if all_stats else None
                
            except Exception as sql_error:
                logging.error(f"SQL查詢錯誤: {str(sql_error)}")
                session.rollback()
                # 如果SQL查詢失敗，關閉當前session並創建新的
                session.close()
                session = Session()
                
                # 嘗試簡化的查詢方式
                logging.info(f"嘗試簡化查詢方式處理 {wallet_address[:10]}.../{token_address[:10]}...")
                return self._fallback_calculation(session, wallet_address, token_address, chain, new_transaction, date_key)
            
        except Exception as e:
            logging.error(f"計算wallet_buy_data時發生錯誤: {str(e)}")
            logging.error(traceback.format_exc())
            if session:
                try:
                    session.rollback()
                except Exception:
                    pass
            return None
            
        finally:
            if session:
                try:
                    session.close()
                except Exception:
                    pass
    
    def _fallback_calculation(self, session, wallet_address, token_address, chain, new_transaction, date_key):
        """備用計算方法，當主要方法失敗時使用"""
        try:
            # 只使用新交易數據進行基本計算
            daily_df = pd.DataFrame([{
                'wallet_address': new_transaction['wallet_address'],
                'token_address': new_transaction['token_address'],
                'chain': new_transaction['chain'],
                'chain_id': new_transaction['chain_id'],
                'transaction_type': new_transaction['transaction_type'],
                'amount': new_transaction['amount'],
                'price': new_transaction['price'],
                'transaction_time': new_transaction['transaction_time'],
                'date': date_key
            }])
            
            stats = self.calc_wallet_token_stats(daily_df)
            return [stats.to_dict()] if stats is not None else None
            
        except Exception as e:
            logging.error(f"備用計算方法也失敗: {str(e)}")
            return None

    def calc_wallet_token_stats(self, group):
        """計算錢包代幣統計數據"""
        if group.empty:
            return None

        # 確保數據類型正確
        group['price'] = pd.to_numeric(group['price'], errors='coerce')
        group['amount'] = pd.to_numeric(group['amount'], errors='coerce')
        group.dropna(subset=['price'], inplace=True)
        group['amount'] = group['amount'].fillna(0)
        
        buys = group[group['transaction_type'].isin(["build", "buy"])].copy()  # 建倉或加倉
        sells = group[group['transaction_type'].isin(["clean", "sell"])].copy()  # 清倉或減倉
        all_trades = pd.concat([buys, sells]).sort_values('transaction_time')

        # 歷史統計
        historical_total_buy_amount = buys['amount'].sum()
        historical_total_buy_cost = (buys['amount'] * buys['price']).sum()
        historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0

        historical_total_sell_amount = sells['amount'].sum()
        historical_total_sell_value = (sells['amount'] * sells['price']).sum()
        historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0
        
        # P&L計算
        cost_of_goods_sold = historical_total_sell_amount * historical_avg_buy_price
        realized_profit = historical_total_sell_value - cost_of_goods_sold
        realized_profit_percentage = (realized_profit / cost_of_goods_sold) * 100 if cost_of_goods_sold > 0 else 0
        
        if pd.notna(realized_profit_percentage):
            realized_profit_percentage = min(1000000, max(-100, realized_profit_percentage))
        else:
            realized_profit_percentage = 0

        # 當前持倉
        current_position = historical_total_buy_amount - historical_total_sell_amount
        current_cost = 0
        current_avg_buy_price = 0
        
        if current_position > 1e-9:
            current_cost = current_position * historical_avg_buy_price
            current_avg_buy_price = historical_avg_buy_price
        else:
            current_position = 0

        # 計算持倉時間
        position = 0
        position_start_time = None
        total_holding_seconds = 0
        last_active_position_closed_at = None

        for _, trade in all_trades.iterrows():
            if trade['transaction_type'] in ["build", "buy"]:  # 建倉或加倉 (買入)
                if position == 0:
                    position_start_time = trade['transaction_time']
                position += trade['amount']
            else:  # 清倉或減倉 (賣出)
                position -= trade['amount']
                if position <= 0 and position_start_time is not None:
                    total_holding_seconds += trade['transaction_time'] - position_start_time
                    last_active_position_closed_at = trade['transaction_time']
                    position_start_time = None
                if position < 0:
                    position = 0

        if position > 0 and position_start_time is not None:
            total_holding_seconds += all_trades['transaction_time'].max() - position_start_time

        chain_id = group['chain_id'].iloc[0]
        chain = 'SOLANA' if chain_id == 501 else 'BSC' if chain_id == 9006 else str(chain_id)

        return pd.Series({
            'wallet_address': group['wallet_address'].iloc[0],
            'token_address': group['token_address'].iloc[0],
            'chain_id': chain_id,
            'date': group['date'].max(),
            'total_amount': current_position,
            'total_cost': current_cost,
            'avg_buy_price': current_avg_buy_price,
            'position_opened_at': self.safe_bigint(position_start_time),
            'historical_total_buy_amount': historical_total_buy_amount,
            'historical_total_buy_cost': historical_total_buy_cost,
            'historical_total_sell_amount': historical_total_sell_amount,
            'historical_total_sell_value': historical_total_sell_value,
            'last_active_position_closed_at': self.safe_bigint(last_active_position_closed_at),
            'historical_avg_buy_price': historical_avg_buy_price,
            'historical_avg_sell_price': historical_avg_sell_price,
            'last_transaction_time': group['transaction_time'].max(),
            'realized_profit': realized_profit,
            'realized_profit_percentage': realized_profit_percentage,
            'total_buy_count': len(buys),
            'total_sell_count': len(sells),
            'total_holding_seconds': self.safe_bigint(total_holding_seconds),
            'chain': chain,
            'updated_at': datetime.now(timezone(timedelta(hours=8)))
        })

    def merge_wallet_buy_data(self, session, new_data):
        # 查詢舊資料
        stmt = select([
            WalletBuyData
        ]).where(
            (WalletBuyData.wallet_address == new_data['wallet_address']) &
            (WalletBuyData.token_address == new_data['token_address']) &
            (WalletBuyData.chain == new_data['chain']) &
            (WalletBuyData.date == new_data['date'])
        )
        old = session.execute(stmt).scalar_one_or_none()
        if old:
            # 累加型欄位
            new_data['total_amount'] += old.total_amount
            new_data['total_cost'] += old.total_cost
            new_data['historical_total_buy_amount'] += old.historical_total_buy_amount
            new_data['historical_total_buy_cost'] += old.historical_total_buy_cost
            new_data['historical_total_sell_amount'] += old.historical_total_sell_amount
            new_data['historical_total_sell_value'] += old.historical_total_sell_value
            new_data['realized_profit'] += old.realized_profit
            new_data['total_buy_count'] += old.total_buy_count
            new_data['total_sell_count'] += old.total_sell_count
            new_data['total_holding_seconds'] += old.total_holding_seconds
            # 平均型欄位
            def avg_merge(new_val, old_val, new_cnt, old_cnt):
                total_cnt = new_cnt + old_cnt
                if total_cnt == 0:
                    return 0
                return (new_val * new_cnt + old_val * old_cnt) / total_cnt
            new_data['avg_buy_price'] = avg_merge(
                new_data['avg_buy_price'], old.avg_buy_price,
                new_data['total_buy_count'], old.total_buy_count
            )
            new_data['historical_avg_buy_price'] = avg_merge(
                new_data['historical_avg_buy_price'], old.historical_avg_buy_price,
                new_data['total_buy_count'], old.total_buy_count
            )
            new_data['historical_avg_sell_price'] = avg_merge(
                new_data['historical_avg_sell_price'], old.historical_avg_sell_price,
                new_data['total_sell_count'], old.total_sell_count
            )
            # 取最大/最新
            new_data['last_transaction_time'] = max(new_data['last_transaction_time'], old.last_transaction_time)
            new_data['last_active_position_closed_at'] = max(new_data['last_active_position_closed_at'], old.last_active_position_closed_at)
            new_data['updated_at'] = max(new_data['updated_at'], old.updated_at)
            # realized_profit_percentage重新計算
            if new_data['historical_total_buy_cost'] > 0:
                new_data['realized_profit_percentage'] = (new_data['realized_profit'] / new_data['historical_total_buy_cost']) * 100
            else:
                new_data['realized_profit_percentage'] = 0
            # position_opened_at: 取較早的
            new_data['position_opened_at'] = min(new_data['position_opened_at'], old.position_opened_at) if old.position_opened_at else new_data['position_opened_at']
            # chain_id/chain/token_address/wallet_address不變
        return new_data

    def bulk_upsert_wallet_buy_data(self, records):
        if not records:
            return
        start_time = time.time()
        logging.info(f"開始批量更新 wallet_buy_data，記錄數: {len(records)}")
        unique_map = {}
        for r in records:
            key = (r['wallet_address'], r['token_address'], r['chain'], r['date'])
            unique_map[key] = r
        deduped_rows = list(unique_map.values())
        batch_size = 50
        successful_upserts = 0
        session = Session()
        for i in range(0, len(deduped_rows), batch_size):
            batch = deduped_rows[i:i + batch_size]
            merged_batch = []
            for row in batch:
                merged = self.merge_wallet_buy_data(session, row)
                merged_batch.append(merged)
            retry = 0
            while retry < 5:
                try:
                    with engine_optimized.begin() as conn:
                        values_clause = []
                        params = {}
                        for idx, row in enumerate(merged_batch):
                            placeholders = []
                            for key, value in row.items():
                                param_name = f"{key}_{idx}"
                                params[param_name] = value
                                placeholders.append(f":{param_name}")
                            values_clause.append(f"({', '.join(placeholders)})")
                        columns = list(batch[0].keys())
                        columns_str = ', '.join(columns)
                        values_str = ', '.join(values_clause)
                        update_clause = ', '.join(
                            f"{col} = EXCLUDED.{col}"
                            for col in columns
                            if col not in ['wallet_address', 'token_address', 'chain', 'date']
                        )
                        upsert_sql = text(f'''
                            INSERT INTO dex_query_v1.wallet_buy_data ({columns_str})
                            VALUES {values_str}
                            ON CONFLICT (wallet_address, token_address, chain, date) 
                            DO UPDATE SET {update_clause}
                        ''')
                        conn.execute(upsert_sql, params)
                        successful_upserts += len(batch)
                    break
                except psycopg2.errors.DeadlockDetected as e:
                    retry += 1
                    logging.warning(f"wallet_buy_data 批次 {i//batch_size + 1} 死鎖，{retry}次重試: {str(e)}")
                    time.sleep(1 + retry)
                except Exception as e:
                    logging.error(f"wallet_buy_data 批次 {i//batch_size + 1} 寫入失敗: {str(e)}")
                    raise
        session.close()
        total_time = time.time() - start_time
        logging.info(f"wallet_buy_data 批量更新完成！成功更新記錄數: {successful_upserts}，總耗時: {total_time:.2f}s")

    def add_to_buffer(self, transaction_data, wallet_buy_data=None):
        """添加數據到緩衝區，確保不會重複添加相同的交易"""
        with self.buffer_lock:
            # 檢查交易是否已存在於緩衝區中
            if transaction_data is not None:
                tx_key = (
                    transaction_data['signature'], 
                    transaction_data['wallet_address'], 
                    transaction_data['token_address'], 
                    transaction_data['transaction_time']
                )
                
                # 檢查是否已經在緩衝區中
                exists = any(
                    (tx['signature'], tx['wallet_address'], tx['token_address'], tx['transaction_time']) == tx_key
                    for tx in self.transaction_buffer
                )
                
                if not exists:
                    self.transaction_buffer.append(transaction_data)
                else:
                    logging.debug(f"交易已存在於緩衝區: {tx_key}")
            
            if wallet_buy_data is not None:
                self.wallet_buy_data_buffer.append(wallet_buy_data)
            
            # 檢查是否需要刷新
            current_time = time.time()
            should_flush = (
                len(self.transaction_buffer) >= self.max_buffer_size or
                len(self.wallet_buy_data_buffer) >= self.max_buffer_size or
                current_time - self.last_flush_time >= self.flush_interval
            )
            
            if should_flush:
                self.flush_buffers()

    def flush_buffers(self):
        """刷新緩衝區，批量寫入數據庫"""
        if not self.transaction_buffer and not self.wallet_buy_data_buffer:
            return
        
        try:
            # 批量寫入transactions
            if self.transaction_buffer:
                self.bulk_upsert_transactions(self.transaction_buffer.copy())
                logging.info(f"刷新緩衝區：處理了 {len(self.transaction_buffer)} 筆交易記錄")
                self.transaction_buffer.clear()
            
            # 批量寫入wallet_buy_data
            if self.wallet_buy_data_buffer:
                self.bulk_upsert_wallet_buy_data(self.wallet_buy_data_buffer.copy())
                logging.info(f"刷新緩衝區：處理了 {len(self.wallet_buy_data_buffer)} 筆wallet_buy_data記錄")
                self.wallet_buy_data_buffer.clear()
            
            self.last_flush_time = time.time()
            
        except Exception as e:
            logging.error(f"刷新緩衝區失敗: {str(e)}")
            logging.error(traceback.format_exc())

    def process_kafka_trade(self, trade_data):
        """處理單筆Kafka交易數據"""
        try:
            # 輸入驗證
            if not trade_data or 'event' not in trade_data:
                logging.warning("無效的trade_data，跳過處理")
                return
                
            event = trade_data['event']
            required_fields = ['address', 'tokenAddress', 'network', 'time', 'side', 'txnValue', 'price']
            for field in required_fields:
                if field not in event:
                    logging.warning(f"缺少必要字段 {field}，跳過處理")
                    return
            
            wallet_address = event['address']
            token_address = event['tokenAddress']
            network = event['network']
            
            if network == 'SOLANA':
                chain_id = 501
                chain = 'SOLANA'
            elif network == 'BSC':
                chain_id = 9006
                chain = 'BSC'
            else:
                logging.warning(f"無效的網絡: {network}，跳過此記錄")
                return

            # 數據類型驗證和轉換
            try:
                timestamp = event['time']
                side = event['side']
                amount = float(event['txnValue'])
                price = float(event['price'])
                baseMint = event.get('baseMint', None)
                quoteMint = event.get('quoteMint', None)
                fromTokenAmount = event.get('fromTokenAmount', None)
                toTokenAmount = event.get('toTokenAmount', None)
                signature = event.get('hash', None)
            except (ValueError, TypeError) as e:
                logging.error(f"數據類型轉換失敗: {str(e)}")
                return

            # 統一時間戳處理邏輯
            ts = int(timestamp)
            if ts > 1e12:  # 如果是毫秒，轉成秒
                ts = int(ts / 1000)

            # 獲取代幣信息
            token_info = asyncio.run(get_token_info_batch_map([token_address], chain_id)).get(token_address, {})
            
            # 獲取from_token和dest_token信息
            from_token_info = {}
            dest_token_info = {}
            if baseMint:
                from_token_info = asyncio.run(get_token_info_batch_map([baseMint], chain_id)).get(baseMint, {})
            if quoteMint:
                dest_token_info = asyncio.run(get_token_info_batch_map([quoteMint], chain_id)).get(quoteMint, {})

            # 只對Solana地址獲取SOL餘額
            wallet_balance = 0
            if chain == 'SOLANA':
                try:
                    balance_result = asyncio.run(get_sol_balance(wallet_address))
                    wallet_balance = balance_result['balance']['int']
                except Exception as e:
                    logging.error(f"獲取SOL餘額失敗 {wallet_address}: {e}")
                    wallet_balance = 0
            
            # 獲取當前持倉（用於交易型態判斷）
            cache_key = (wallet_address, token_address, chain)
            current_holding = self.position_cache[cache_key]['holding_amount']
            
            # 判斷交易型態
            transaction_type = self.determine_transaction_type(side, amount, current_holding)
            
            # 計算已實現收益
            realized_profit, realized_profit_percentage = self.calculate_realized_profit(
                wallet_address, token_address, chain, side, amount, price, ts
            )
            
            # 計算 holding_percentage
            holding_percentage = 0.0  # 預設值為0
            
            if transaction_type in ["build", "buy"]:  # 建倉或加倉 (買入)
                # 買入：計算買入value佔wallet_balance的百分比
                value = price * amount
                if wallet_balance > 0:
                    holding_percentage = min(100.0, max(0.0, (value / wallet_balance) * 100))
                else:
                    holding_percentage = 0.0
            elif transaction_type in ["clean", "sell"]:  # 清倉或減倉 (賣出)
                # 賣出：計算賣出數量佔原持倉數量的百分比
                if current_holding > 0:
                    sell_percentage = (amount / current_holding) * 100
                    holding_percentage = min(100.0, max(0.0, sell_percentage))
                else:
                    holding_percentage = 0.0
            
            # 計算marketcap
            try:
                marketcap = price * float(token_info.get('supply', 0))
            except Exception:
                marketcap = 0

            # 構建交易數據
            tx_data = {
                'wallet_address': wallet_address,
                'wallet_balance': wallet_balance,
                'token_address': token_address,
                'token_icon': token_info.get('token_icon'),
                'token_name': self.truncate_string(token_info.get('token_name'), 255),
                'price': price,
                'amount': amount,
                'marketcap': marketcap,
                'value': price * amount,
                'holding_percentage': holding_percentage,  # 使用計算好的百分比
                'chain': chain,
                'chain_id': chain_id,
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'transaction_type': transaction_type,
                'transaction_time': ts,
                'time': datetime.now(timezone(timedelta(hours=8))),
                'signature': signature,
                'from_token_address': baseMint,
                'from_token_symbol': self.truncate_string(from_token_info.get('symbol'), 100),
                'from_token_amount': fromTokenAmount,
                'dest_token_address': quoteMint,
                'dest_token_symbol': self.truncate_string(dest_token_info.get('symbol'), 100),
                'dest_token_amount': toTokenAmount,
            }
            
            logging.info(f'[KAFKA_OPT] 處理交易: {signature[:20]}..., {wallet_address[:10]}..., {token_address[:10]}..., {side}, {amount}')
            
            # 計算wallet_buy_data（增量方式）
            wallet_buy_data = self.calculate_wallet_buy_data_incremental(
                wallet_address, token_address, chain, tx_data
            )
            
            # 添加到緩衝區 - 先添加交易記錄，再添加wallet_buy_data記錄
            self.add_to_buffer(tx_data, None)  # 只添加一次交易記錄
            
            # 處理wallet_buy_data記錄
            if wallet_buy_data:
                if isinstance(wallet_buy_data, list):
                    # 如果返回的是列表，添加所有wallet_buy_data記錄
                    for record in wallet_buy_data:
                        with self.buffer_lock:
                            self.wallet_buy_data_buffer.append(record)
                else:
                    # 如果返回的是單個字典
                    with self.buffer_lock:
                        self.wallet_buy_data_buffer.append(wallet_buy_data)
                
                # 檢查是否需要刷新緩衝區
                current_time = time.time()
                should_flush = (
                    len(self.transaction_buffer) >= self.max_buffer_size or
                    len(self.wallet_buy_data_buffer) >= self.max_buffer_size or
                    current_time - self.last_flush_time >= self.flush_interval
                )
                
                if should_flush:
                    with self.buffer_lock:
                        self.flush_buffers()
            
        except Exception as e:
            logging.error(f"處理Kafka交易時發生錯誤: {str(e)}")
            logging.error(traceback.format_exc())

def kafka_consumer_thread():
    """Kafka 消費線程，只負責拉取消息放入隊列"""
    try:
        consumer = KafkaConsumer(
            'web3_trade_events',
            bootstrap_servers=KAFKA_BROKER,
            group_id = f'wallet_position_analyze_{int(time.time())}',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            session_timeout_ms=60000,
            heartbeat_interval_ms=20000,
            max_poll_interval_ms=300000,
            max_poll_records=50,
            fetch_min_bytes=1,
            fetch_max_wait_ms=500,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info("Kafka 消費線程已啟動")
        while True:
            message_batch = consumer.poll(timeout_ms=1000)
            if message_batch:
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        # 解析分區鍵
                        try:
                            event = message.value.get('event', {})
                            chain = event.get('network', 'SOLANA')
                            ts = event.get('time', int(time.time()))
                            if ts > 1e12:
                                ts = int(ts / 1000)
                            date = datetime.fromtimestamp(ts, timezone(timedelta(hours=8))).date()
                            idx = get_partition_idx(chain, date)
                        except Exception:
                            idx = 0  # fallback
                        while True:
                            try:
                                partition_buffers[idx].put(message.value, timeout=1)
                                break
                            except queue.Full:
                                logging.warning(f"[KAFKA_OPT] 分區隊列{idx}已滿，消費被阻塞，請關注DB寫入壓力！")
                                time.sleep(0.5)
    except Exception as e:
        logging.error(f"Kafka 消費線程異常: {str(e)}")
        logging.error(traceback.format_exc())

def db_writer_thread(thread_id):
    processor = OptimizedKafkaProcessor()
    buffer = []
    last_flush = time.time()
    while True:
        try:
            q = partition_buffers[thread_id]
            qsize = q.qsize()
            dynamic_batch_size = min(BATCH_SIZE + qsize // 500, 1000)
            dynamic_flush_interval = max(0.5, FLUSH_INTERVAL - min(qsize // 2000, FLUSH_INTERVAL - 0.5))
            while len(buffer) < dynamic_batch_size:
                try:
                    msg = q.get(timeout=0.1)
                    buffer.append(msg)
                except queue.Empty:
                    break
            now = time.time()
            if len(buffer) >= dynamic_batch_size or (buffer and (now - last_flush) > dynamic_flush_interval):
                for trade_data in buffer:
                    try:
                        processor.process_kafka_trade(trade_data)
                    except Exception as e:
                        logging.error(f"[KAFKA_OPT] 線程{thread_id} 處理消息異常: {str(e)}")
                with processor.buffer_lock:
                    processor.flush_buffers()
                logging.info(f"[KAFKA_OPT] 線程{thread_id} 批量處理 {len(buffer)} 筆，分區隊列剩餘: {q.qsize()}")
                buffer.clear()
                last_flush = now
        except Exception as e:
            logging.error(f"[KAFKA_OPT] 線程{thread_id} DB寫入異常: {str(e)}")
            logging.error(traceback.format_exc())
            time.sleep(1)

def main():
    logging.info('=== 多線程高性能Kafka消費者啟動 ===')
    threading.Thread(target=kafka_consumer_thread, daemon=True).start()
    for i in range(PROCESS_THREAD_NUM):
        threading.Thread(target=db_writer_thread, args=(i,), daemon=True).start()
    while True:
        qlens = [q.qsize() for q in partition_buffers]
        logging.info(f"[KAFKA_OPT] 監控: 各分區隊列長度={qlens}")
        time.sleep(10)

if __name__ == '__main__':
    main() 