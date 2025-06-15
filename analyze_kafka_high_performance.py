#!/usr/bin/env python3
"""
高性能Kafka處理器
專為高頻交易場景優化，支持每秒數千筆交易
"""

import logging
import traceback
import json
import math
import pandas as pd
import asyncio
import time
import threading
from collections import defaultdict, deque
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from datetime import datetime, timezone, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
import psutil

from config import engine_smartmoney, KAFKA_BROKER, KAFKA_TOPIC
from models import Transaction
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map

logging.basicConfig(
    level=logging.INFO,
    format='[KAFKA_HP] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

class HighPerformanceKafkaProcessor:
    """高性能Kafka處理器"""
    
    def __init__(self):
        # 多級緩衝配置
        self.transaction_buffer = deque(maxlen=1000)
        self.wallet_buy_data_buffer = deque(maxlen=500)
        self.processing_queue = Queue(maxsize=5000)
        
        # 性能調優參數
        self.max_buffer_size = 500  # 增大緩衝區
        self.flush_interval = 10    # 縮短刷新間隔
        self.max_workers = min(8, psutil.cpu_count())  # 動態工作線程數
        
        # 高性能數據庫引擎
        self.engine_hp = engine_smartmoney.execution_options(
            pool_size=50,           # 大幅增加連接池
            max_overflow=100,
            pool_pre_ping=True,
            pool_recycle=1800,
            pool_timeout=30
        )
        self.Session = sessionmaker(bind=self.engine_hp)
        
        # 內存緩存 - 用於快速計算
        self.position_cache = defaultdict(lambda: {
            'holding_amount': 0.0,
            'holding_cost': 0.0,
            'last_update': 0,
            'transactions': deque(maxlen=100)  # 保留最近100筆交易
        })
        
        # 線程安全鎖
        self.cache_lock = threading.RLock()
        self.buffer_lock = threading.RLock()
        
        # 統計信息
        self.stats = {
            'processed_count': 0,
            'error_count': 0,
            'start_time': time.time(),
            'last_flush_time': time.time()
        }
        
        # 啟動工作線程
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        self.running = True
        
        # 啟動後台任務
        self._start_background_tasks()
    
    def _start_background_tasks(self):
        """啟動後台任務"""
        # 定時刷新緩衝區
        threading.Thread(target=self._buffer_flusher, daemon=True).start()
        # 性能監控
        threading.Thread(target=self._performance_monitor, daemon=True).start()
        # 緩存清理
        threading.Thread(target=self._cache_cleaner, daemon=True).start()
    
    def _buffer_flusher(self):
        """後台緩衝區刷新器"""
        while self.running:
            try:
                time.sleep(self.flush_interval)
                self._force_flush_buffers()
            except Exception as e:
                logging.error(f"緩衝區刷新器錯誤: {e}")
    
    def _performance_monitor(self):
        """性能監控器"""
        while self.running:
            try:
                time.sleep(60)  # 每分鐘報告一次
                current_time = time.time()
                elapsed = current_time - self.stats['start_time']
                
                if elapsed > 0:
                    tps = self.stats['processed_count'] / elapsed
                    error_rate = self.stats['error_count'] / max(1, self.stats['processed_count']) * 100
                    
                    logging.info(f"性能報告: TPS={tps:.2f}, 錯誤率={error_rate:.2f}%, "
                               f"緩存大小={len(self.position_cache)}, "
                               f"隊列大小={self.processing_queue.qsize()}")
                    
            except Exception as e:
                logging.error(f"性能監控錯誤: {e}")
    
    def _cache_cleaner(self):
        """緩存清理器"""
        while self.running:
            try:
                time.sleep(300)  # 每5分鐘清理一次
                current_time = time.time()
                
                with self.cache_lock:
                    # 清理超過1小時未更新的緩存
                    expired_keys = []
                    for key, cache_data in self.position_cache.items():
                        if current_time - cache_data['last_update'] > 3600:
                            expired_keys.append(key)
                    
                    for key in expired_keys:
                        del self.position_cache[key]
                    
                    if expired_keys:
                        logging.info(f"清理了 {len(expired_keys)} 個過期緩存項")
                        
            except Exception as e:
                logging.error(f"緩存清理錯誤: {e}")
    
    def fast_realized_profit_calculation(self, wallet_address, token_address, chain, 
                                       transaction_type, amount, price, transaction_time):
        """快速已實現收益計算"""
        key = (wallet_address, token_address, chain)
        
        with self.cache_lock:
            cache_data = self.position_cache[key]
            
            realized_profit = 0.0
            realized_profit_percentage = 0.0
            
            if transaction_type in ['build', 'buy']:  # 建倉或加倉
                cache_data['holding_amount'] += amount
                cache_data['holding_cost'] += amount * price
            elif transaction_type in ['clean', 'sell']:  # 清倉或減倉
                if cache_data['holding_amount'] > 1e-9:
                    avg_cost = cache_data['holding_cost'] / cache_data['holding_amount']
                    realized_profit = (price - avg_cost) * amount
                    realized_profit_percentage = ((price / avg_cost - 1) * 100) if avg_cost > 0 else 0
                    
                    # 更新持倉
                    cache_data['holding_amount'] -= amount
                    cache_data['holding_cost'] -= avg_cost * amount
                    
                    if cache_data['holding_amount'] < 1e-9:
                        cache_data['holding_amount'] = 0.0
                        cache_data['holding_cost'] = 0.0
            
            cache_data['last_update'] = transaction_time
            
            # 記錄交易到緩存（用於後續wallet_buy_data計算）
            cache_data['transactions'].append({
                'type': transaction_type,
                'amount': amount,
                'price': price,
                'time': transaction_time
            })
            
            return realized_profit, realized_profit_percentage
    
    def process_trade_fast(self, trade_data):
        """快速處理單筆交易"""
        try:
            # 基本驗證
            if not trade_data or 'event' not in trade_data:
                return None
                
            event = trade_data['event']
            
            # 快速提取核心數據
            wallet_address = event.get('address')
            token_address = event.get('tokenAddress')
            network = event.get('network')
            
            if not all([wallet_address, token_address, network]):
                return None
            
            # 網絡映射
            chain_map = {'SOLANA': (501, 'SOLANA'), 'BSC': (9006, 'BSC')}
            if network not in chain_map:
                return None
            
            chain_id, chain = chain_map[network]
            
            # 數據轉換
            timestamp = int(event.get('time', 0))
            if timestamp > 1e12:
                timestamp = int(timestamp / 1000)
            
            side = event.get('side')
            amount = float(event.get('txnValue', 0))
            price = float(event.get('price', 0))
            signature = event.get('hash', '')
            
            # 快速已實現收益計算
            realized_profit, realized_profit_percentage = self.fast_realized_profit_calculation(
                wallet_address, token_address, chain, side, amount, price, timestamp
            )
            
            # 計算 holding_percentage
            holding_percentage = 0.0  # 預設值為0
            
            if side == 'buy':
                # 買入：計算買入value佔wallet_balance的百分比（高頻場景下跳過餘額查詢，設為0）
                holding_percentage = 0.0  # 高頻場景下為了性能跳過餘額查詢
            elif side == 'sell':
                # 賣出：計算賣出數量佔原持倉數量的百分比
                key = (wallet_address, token_address, chain)
                with self.cache_lock:
                    if key in self.position_cache:
                        cache_data = self.position_cache[key]
                        # 使用賣出前的持倉數量
                        original_holding = cache_data['holding_amount'] + amount
                        if original_holding > 0:
                            sell_percentage = (amount / original_holding) * 100
                            holding_percentage = min(100.0, max(0.0, sell_percentage))
                        else:
                            holding_percentage = 0.0
                    else:
                        holding_percentage = 0.0
            
            # 構建交易數據（簡化版）
            tx_data = {
                'wallet_address': wallet_address,
                'wallet_balance': 0,  # 高頻場景下跳過餘額查詢
                'token_address': token_address,
                'token_icon': None,
                'token_name': None,
                'price': price,
                'amount': amount,
                'marketcap': 0,
                'value': price * amount,
                'holding_percentage': holding_percentage,  # 使用計算好的百分比
                'chain': chain,
                'chain_id': chain_id,
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'transaction_type': side,
                'transaction_time': timestamp,
                'time': datetime.now(timezone(timedelta(hours=8))),
                'signature': signature,
                'from_token_address': event.get('baseMint', ''),
                'from_token_symbol': None,
                'from_token_amount': float(event.get('fromTokenAmount', 0)),
                'dest_token_address': event.get('quoteMint', ''),
                'dest_token_symbol': None,
                'dest_token_amount': float(event.get('toTokenAmount', 0)),
            }
            
            return tx_data
            
        except Exception as e:
            self.stats['error_count'] += 1
            logging.error(f"快速處理交易失敗: {str(e)}")
            return None
    
    def add_to_buffer_fast(self, tx_data):
        """快速添加到緩衝區，避免重複"""
        if not tx_data:
            return
            
        with self.buffer_lock:
            # 快速重複檢查 - 只檢查最近的一些記錄以提高性能
            tx_key = (tx_data['signature'], tx_data['wallet_address'], 
                     tx_data['token_address'], tx_data['transaction_time'])
            
            # 檢查最近50筆記錄是否有重複（平衡性能和準確性）
            recent_keys = [
                (tx['signature'], tx['wallet_address'], tx['token_address'], tx['transaction_time'])
                for tx in list(self.transaction_buffer)[-50:]
            ]
            
            if tx_key not in recent_keys:
                self.transaction_buffer.append(tx_data)
                self.stats['processed_count'] += 1
            else:
                logging.debug(f"跳過重複交易: {tx_key}")
            
            # 檢查是否需要刷新
            if len(self.transaction_buffer) >= self.max_buffer_size:
                self._force_flush_buffers()
    
    def _force_flush_buffers(self):
        """強制刷新緩衝區"""
        with self.buffer_lock:
            if not self.transaction_buffer:
                return
            
            # 複製並清空緩衝區
            transactions_to_process = list(self.transaction_buffer)
            self.transaction_buffer.clear()
            
            # 異步批量處理
            self.executor.submit(self._bulk_process_transactions, transactions_to_process)
            
            self.stats['last_flush_time'] = time.time()
    
    def _bulk_process_transactions(self, transactions):
        """批量處理交易"""
        if not transactions:
            return
            
        start_time = time.time()
        
        try:
            # 批量插入交易記錄
            self._bulk_insert_transactions_hp(transactions)
            
            # 批量計算和更新wallet_buy_data（簡化版）
            self._bulk_update_wallet_buy_data_hp(transactions)
            
            elapsed = time.time() - start_time
            tps = len(transactions) / elapsed if elapsed > 0 else 0
            
            logging.info(f"批量處理完成: {len(transactions)} 筆交易, "
                        f"耗時 {elapsed:.3f}s, TPS={tps:.2f}")
            
        except Exception as e:
            logging.error(f"批量處理失敗: {str(e)}")
            logging.error(traceback.format_exc())
    
    def _bulk_insert_transactions_hp(self, transactions):
        """高性能批量插入交易"""
        if not transactions:
            return
            
        # 去重
        unique_txs = {}
        for tx in transactions:
            key = (tx['signature'], tx['wallet_address'], tx['token_address'], tx['transaction_time'])
            unique_txs[key] = tx
        
        deduped = list(unique_txs.values())
        
        # 批量插入
        batch_size = 200  # 增大批次
        for i in range(0, len(deduped), batch_size):
            batch = deduped[i:i + batch_size]
            
            try:
                with self.engine_hp.begin() as conn:
                    # 構建批量SQL
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
                    
            except Exception as e:
                logging.error(f"批量插入失敗 (批次 {i//batch_size + 1}): {str(e)}")
    
    def _bulk_update_wallet_buy_data_hp(self, transactions):
        """高性能批量更新wallet_buy_data（簡化版）"""
        # 高頻場景下，簡化wallet_buy_data計算
        # 只計算當日統計，減少歷史查詢
        
        try:
            daily_stats = defaultdict(lambda: {
                'total_buy_amount': 0.0,
                'total_buy_cost': 0.0,
                'total_sell_amount': 0.0,
                'total_sell_value': 0.0,
                'buy_count': 0,
                'sell_count': 0,
                'last_time': 0
            })
            
            # 聚合當日數據
            for tx in transactions:
                ts = tx['transaction_time']
                date_key = datetime.fromtimestamp(ts, timezone(timedelta(hours=8))).date()
                
                key = (tx['wallet_address'], tx['token_address'], tx['chain'], date_key)
                stats = daily_stats[key]
                
                if tx['transaction_type'] in ['build', 'buy']:  # 建倉或加倉
                    stats['total_buy_amount'] += tx['amount']
                    stats['total_buy_cost'] += tx['amount'] * tx['price']
                    stats['buy_count'] += 1
                else:
                    stats['total_sell_amount'] += tx['amount']
                    stats['total_sell_value'] += tx['amount'] * tx['price']
                    stats['sell_count'] += 1
                
                stats['last_time'] = max(stats['last_time'], ts)
            
            # 批量更新
            if daily_stats:
                self._upsert_daily_stats(daily_stats)
                
        except Exception as e:
            logging.error(f"批量更新wallet_buy_data失敗: {str(e)}")
    
    def _upsert_daily_stats(self, daily_stats):
        """批量寫入日統計"""
        records = []
        
        # 去重處理 - 雖然daily_stats本身已經是去重的，但為了安全起見
        unique_records = {}
        
        for (wallet_address, token_address, chain, date_key), stats in daily_stats.items():
            # 計算簡化統計
            avg_buy_price = stats['total_buy_cost'] / stats['total_buy_amount'] if stats['total_buy_amount'] > 0 else 0
            current_position = stats['total_buy_amount'] - stats['total_sell_amount']
            
            record = {
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain_id': 501 if chain == 'SOLANA' else 9006,
                'chain': chain,
                'date': date_key,
                'total_amount': max(0, current_position),
                'total_cost': current_position * avg_buy_price if current_position > 0 else 0,
                'avg_buy_price': avg_buy_price,
                'historical_total_buy_amount': stats['total_buy_amount'],
                'historical_total_buy_cost': stats['total_buy_cost'],
                'historical_total_sell_amount': stats['total_sell_amount'],
                'historical_total_sell_value': stats['total_sell_value'],
                'total_buy_count': stats['buy_count'],
                'total_sell_count': stats['sell_count'],
                'last_transaction_time': stats['last_time'],
                'updated_at': datetime.now(timezone(timedelta(hours=8))),
                'position_opened_at': 0,
                'last_active_position_closed_at': 0,
                'historical_avg_buy_price': avg_buy_price,
                'historical_avg_sell_price': stats['total_sell_value'] / stats['total_sell_amount'] if stats['total_sell_amount'] > 0 else 0,
                'realized_profit': 0,
                'realized_profit_percentage': 0,
                'total_holding_seconds': 0
            }
            
            # 使用主鍵作為去重鍵
            unique_key = (wallet_address, token_address, chain, date_key)
            unique_records[unique_key] = record
        
        # 轉換為列表
        records = list(unique_records.values())
        
        # 批量寫入
        if records:
            try:
                with self.engine_hp.begin() as conn:
                    batch_size = 100
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        
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
                        
            except Exception as e:
                logging.error(f"寫入日統計失敗: {str(e)}")

    def _process_single_message(self, trade_data):
        """處理單條消息"""
        try:
            tx_data = self.process_trade_fast(trade_data)
            if tx_data:
                self.add_to_buffer_fast(tx_data)
        except Exception as e:
            self.stats['error_count'] += 1
            logging.error(f"處理單條消息失敗: {str(e)}")

    def shutdown(self):
        """優雅關閉"""
        logging.info("開始關閉高性能處理器...")
        self.running = False
        
        # 強制刷新所有緩衝區
        self._force_flush_buffers()
        
        # 等待所有任務完成
        self.executor.shutdown(wait=True)
        
        # 關閉數據庫連接
        self.engine_hp.dispose()
        
        logging.info("高性能處理器已關閉")

def high_performance_kafka_consumer():
    """高性能Kafka消費者"""
    processor = HighPerformanceKafkaProcessor()
    
    try:
        consumer = KafkaConsumer(
            'web3_trade_events',
            bootstrap_servers=KAFKA_BROKER,
            group_id='wallet_position_analyze_hp',
            auto_offset_reset='latest',  # 只處理新消息
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
            max_poll_records=500,  # 大批次處理
            fetch_min_bytes=1024,  # 最小獲取量
            fetch_max_wait_ms=500,  # 最大等待時間
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logging.info("高性能Kafka消費者已啟動，支持高頻交易處理")
        
        while True:
            try:
                # 批量獲取消息
                message_batch = consumer.poll(timeout_ms=100)
                
                if message_batch:
                    batch_size = sum(len(messages) for messages in message_batch.values())
                    if batch_size > 100:
                        logging.info(f"處理大批次消息: {batch_size} 筆")
                    
                    for topic_partition, messages in message_batch.items():
                        # 並行處理消息
                        futures = []
                        for message in messages:
                            future = processor.executor.submit(
                                processor._process_single_message, message.value
                            )
                            futures.append(future)
                        
                        # 等待所有消息處理完成
                        for future in futures:
                            try:
                                future.result(timeout=5)  # 5秒超時
                            except Exception as e:
                                logging.error(f"處理消息失敗: {str(e)}")
                
            except Exception as e:
                logging.error(f"Kafka消費者錯誤: {str(e)}")
                time.sleep(1)
                
    except Exception as e:
        logging.error(f"Kafka消費者致命錯誤: {str(e)}")
        raise
    finally:
        processor.running = False
        processor.executor.shutdown(wait=True)

def main():
    logging.info('=== 高性能Kafka消費者啟動 ===')
    high_performance_kafka_consumer()

if __name__ == '__main__':
    main() 