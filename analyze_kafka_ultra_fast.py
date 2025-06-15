import logging
import traceback
import json
import math
import time
import asyncio
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
import redis
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.pool import QueuePool
import os
import queue
from sqlalchemy import insert
from sqlalchemy.orm import Session
import sys

from config import engine_smartmoney, KAFKA_BROKER

logging.basicConfig(
    level=logging.INFO,
    format='[KAFKA_ULTRA] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

# 確保連接到正確的 schema
def get_engine():
    engine = engine_smartmoney
    # 設置搜索路徑
    with engine.connect() as conn:
        conn.execute(text("SET search_path TO dex_query_v1, public"))
        # 驗證表是否存在
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'dex_query_v1' 
                AND table_name = 'temp_wallet_buy_data'
            )
        """))
        if not result.scalar():
            raise Exception("Table dex_query_v1.temp_wallet_buy_data does not exist")
    return engine

class UltraFastKafkaProcessor:
    """超高速Kafka處理器 - 目標處理 100+ TPS"""
    
    def __init__(self):
        # Redis 緩存配置
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=int(os.getenv('REDIS_DB', 0)),
            decode_responses=True
        )
        
        # 單例鎖
        self.lock_key = 'ultra_fast_kafka_consumer_lock'
        self.lock_ttl = 60  # 秒
        if not self.redis_client.set(self.lock_key, os.getpid(), nx=True, ex=self.lock_ttl):
            print('Another instance is already running. Exiting.')
            sys.exit(0)
        threading.Thread(target=self._renew_lock, daemon=True).start()
        
        # 批量處理配置
        self.batch_size = 1000
        self.flush_interval = 600  # 10分鐘
        self.max_retries = 3
        self.retry_delay = 1  # 秒
        
        # 緩存鍵前綴
        self.POSITION_KEY = "position:{}:{}:{}"  # chain:wallet:token
        self.TRANSACTION_KEY = "transaction:{}:{}:{}"  # chain:wallet:token
        self.STATS_KEY = "stats:{}:{}:{}"  # chain:wallet:token
        
        # 批量寫入隊列
        self.write_queue = queue.Queue(maxsize=10000)
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.last_flush_time = time.time()
        
        # 預定義表結構
        self.metadata = MetaData()
        try:
            self.temp_wallet_buy_data_table = Table(
                'temp_wallet_buy_data', 
                self.metadata,
                schema='dex_query_v1',
                autoload_with=get_engine()
            )
        except Exception as e:
            logging.warning(f"無法加載表結構，將使用原始SQL: {str(e)}")
            self.temp_wallet_buy_data_table = None
        
        # 啟動後台任務
        self._start_background_tasks()
        
        # 加載歷史數據到緩存
        self._load_historical_data()
        
        self.wallet_buy_data_buffer = []
        self.buffer_lock = threading.Lock()

    def _load_historical_data(self):
        """從 temp_wallet_buy_data 聚合加載數據到 Redis 緩存"""
        try:
            engine = get_engine()
            with engine.connect() as conn:
                sql = '''
                SELECT
                    wallet_address,
                    token_address,
                    chain,
                    SUM(total_amount) AS total_amount,
                    SUM(total_cost) AS total_cost,
                    SUM(historical_total_buy_amount) AS historical_total_buy_amount,
                    SUM(historical_total_buy_cost) AS historical_total_buy_cost,
                    SUM(historical_total_sell_amount) AS historical_total_sell_amount,
                    SUM(historical_total_sell_value) AS historical_total_sell_value,
                    MAX(position_opened_at) AS position_opened_at,
                    MAX(last_transaction_time) AS last_transaction_time,
                    SUM(realized_profit) AS realized_profit,
                    SUM(realized_profit_percentage) AS realized_profit_percentage,
                    SUM(total_buy_count) AS total_buy_count,
                    SUM(total_sell_count) AS total_sell_count,
                    SUM(total_holding_seconds) AS total_holding_seconds
                FROM dex_query_v1.temp_wallet_buy_data
                GROUP BY wallet_address, token_address, chain
                '''
                result = conn.execute(text(sql))
                for row in result:
                    key = self.STATS_KEY.format(row.chain, row.wallet_address, row.token_address)
                    row_dict = dict(row._mapping)
                    # 確保所有值都是字符串（Redis要求）
                    redis_data = {k: str(v) if v is not None else '0' for k, v in row_dict.items()}
                    self.redis_client.hmset(key, redis_data)
            logging.info("成功加載 temp_wallet_buy_data 聚合數據到緩存")
        except Exception as e:
            logging.error(f"加載歷史數據失敗: {str(e)}")
            logging.error(f"錯誤詳情: {traceback.format_exc()}")
    
    def _start_background_tasks(self):
        """啟動後台任務"""
        # 批量處理線程
        threading.Thread(target=self._batch_processor, daemon=True).start()
        # 監控線程
        threading.Thread(target=self._stats_monitor, daemon=True).start()
        # 緩存清理線程
        threading.Thread(target=self._cache_cleaner, daemon=True).start()
    
    def _batch_processor(self):
        """批量處理線程"""
        while True:
            try:
                batch = []
                # 收集批量數據
                while len(batch) < self.batch_size:
                    try:
                        item = self.write_queue.get(timeout=0.1)
                        batch.append(item)
                    except queue.Empty:
                        break
                
                if batch:
                    self._process_batch_ultra_fast(batch)
                
                # 檢查是否需要強制刷新
                if time.time() - self.last_flush_time > self.flush_interval:
                    self._force_flush()
                
            except Exception as e:
                logging.error(f"批量處理異常: {str(e)}")
                time.sleep(1)
    
    def _stats_monitor(self):
        """監控線程"""
        while True:
            try:
                queue_size = self.write_queue.qsize()
                if queue_size > self.batch_size * 2:
                    logging.warning(f"寫入隊列積壓: {queue_size}")
                time.sleep(10)
            except Exception as e:
                logging.error(f"監控異常: {str(e)}")
    
    def _cache_cleaner(self):
        """緩存清理線程"""
        while True:
            try:
                # 清理過期緩存
                keys = self.redis_client.keys("position:*")
                for key in keys:
                    last_update = int(self.redis_client.hget(key, 'last_update') or 0)
                    if time.time() - last_update > 86400:  # 24小時
                        self.redis_client.delete(key)
                time.sleep(3600)  # 每小時清理一次
            except Exception as e:
                logging.error(f"緩存清理異常: {str(e)}")
    
    def process_trade_ultra_fast(self, trade_data):
        """快速處理交易數據"""
        try:
            event = trade_data.get('event', {})
            if not event:
                return
            
            # 提取基本信息
            wallet_address = event.get('address')
            token_address = event.get('tokenAddress')
            chain = event.get('network', 'SOLANA')
            side = event.get('side')
            amount = float(event.get('txnValue', 0))
            price = float(event.get('price', 0))
            timestamp = int(event.get('time', 0))
            if timestamp > 1e12:
                timestamp = int(timestamp / 1000)
            
            # 使用 Redis 緩存計算持倉
            position_key = self.POSITION_KEY.format(chain, wallet_address, token_address)
            current_position = float(self.redis_client.hget(position_key, 'amount') or 0)
            current_cost = float(self.redis_client.hget(position_key, 'cost') or 0)
            
            # 判斷交易類型
            transaction_type = self._determine_transaction_type_fast(
                wallet_address, token_address, chain, side, amount
            )
            
            # 獲取當前日期的統計數據
            stats_key = self.STATS_KEY.format(chain, wallet_address, token_address)
            redis_stats = self.redis_client.hgetall(stats_key)
            
            # 定義默認值
            default_stats = {
                'total_amount': '0',
                'total_cost': '0',
                'avg_buy_price': '0',
                'position_opened_at': '0',
                'historical_total_buy_amount': '0',
                'historical_total_buy_cost': '0',
                'historical_total_sell_amount': '0',
                'historical_total_sell_value': '0',
                'historical_avg_buy_price': '0',
                'historical_avg_sell_price': '0',
                'last_transaction_time': '0',
                'realized_profit': '0',
                'realized_profit_percentage': '0',
                'total_buy_count': '0',
                'total_sell_count': '0',
                'total_holding_seconds': '0'
            }
            
            # 合併 Redis 數據和默認值
            stats = default_stats.copy()
            stats.update(redis_stats)
            
            # 定義數值型欄位
            numeric_fields = {
                'total_amount': float,
                'total_cost': float,
                'avg_buy_price': float,
                'historical_total_buy_amount': float,
                'historical_total_buy_cost': float,
                'historical_total_sell_amount': float,
                'historical_total_sell_value': float,
                'historical_avg_buy_price': float,
                'historical_avg_sell_price': float,
                'realized_profit': float,
                'realized_profit_percentage': float,
                'total_buy_count': int,
                'total_sell_count': int,
                'total_holding_seconds': int,
                'position_opened_at': int,
                'last_transaction_time': int
            }
            
            # 安全地轉換數據類型
            converted_stats = {}
            for k, v in stats.items():
                if k in numeric_fields:
                    try:
                        converted_stats[k] = numeric_fields[k](v or 0)
                    except (ValueError, TypeError):
                        converted_stats[k] = numeric_fields[k](0)
                else:
                    converted_stats[k] = v
            stats = converted_stats
            
            # 初始化收益相關變量
            realized_profit = 0.0
            realized_profit_percentage = 0.0
            
            # 更新統計數據
            if transaction_type in ["build", "buy"]:
                # 買入邏輯
                if transaction_type == "build":
                    stats['position_opened_at'] = timestamp
                
                # 更新總持倉量和成本
                stats['total_amount'] = float(stats['total_amount']) + amount
                stats['total_cost'] = float(stats['total_cost']) + (amount * price)
                
                # 更新歷史買入數據
                stats['historical_total_buy_amount'] += amount
                stats['historical_total_buy_cost'] += (amount * price)
                stats['historical_avg_buy_price'] = stats['historical_total_buy_cost'] / stats['historical_total_buy_amount']
                
                # 更新平均買入價格
                stats['avg_buy_price'] = stats['total_cost'] / stats['total_amount'] if stats['total_amount'] > 0 else 0
                
                # 更新計數器
                stats['total_buy_count'] += 1
                
            else:  # sell or clean
                # 計算賣出收益
                avg_cost = stats['avg_buy_price']
                realized_profit = (price - avg_cost) * amount
                if avg_cost != 0:
                    realized_profit_percentage = ((price / avg_cost - 1) * 100)
                    realized_profit_percentage = max(realized_profit_percentage, -100)  # 限制最小值為-100%
                
                # 更新總持倉量和成本
                stats['total_amount'] = float(stats['total_amount']) - amount
                if stats['total_amount'] <= 0:
                    # 清倉時重置成本和均價
                    stats['total_cost'] = 0
                    stats['avg_buy_price'] = 0
                    stats['position_opened_at'] = 0
                else:
                    # 減倉時按比例減少成本
                    cost_ratio = amount / (stats['total_amount'] + amount)
                    stats['total_cost'] *= (1 - cost_ratio)
                
                # 更新歷史賣出數據
                stats['historical_total_sell_amount'] += amount
                stats['historical_total_sell_value'] += (amount * price)
                if stats['historical_total_sell_amount'] > 0:
                    stats['historical_avg_sell_price'] = stats['historical_total_sell_value'] / stats['historical_total_sell_amount']
                
                # 更新已實現收益
                stats['realized_profit'] += realized_profit
                stats['realized_profit_percentage'] += realized_profit_percentage
                
                # 更新計數器
                stats['total_sell_count'] += 1
            
            # 更新最後交易時間
            stats['last_transaction_time'] = timestamp
            
            # 更新持倉時長
            if stats['position_opened_at'] > 0:
                stats['total_holding_seconds'] = timestamp - stats['position_opened_at']
            
            # 保存到 Redis
            redis_stats = {k: str(v) for k, v in stats.items()}
            self.redis_client.hmset(stats_key, redis_stats)
            
            # 緩存到本地 buffer
            with self.buffer_lock:
                self.wallet_buy_data_buffer.append((wallet_address, token_address, chain, stats.copy()))
                if len(self.wallet_buy_data_buffer) >= self.batch_size or (time.time() - self.last_flush_time) > self.flush_interval:
                    self._async_flush_wallet_buy_data()
            
            # 構建交易記錄
            tx_data = {
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain': chain,
                'chain_id': 501 if chain == 'SOLANA' else 9006,
                'transaction_type': transaction_type,
                'amount': amount,
                'price': price,
                'transaction_time': timestamp,
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'signature': event.get('hash'),
                'time': datetime.now(timezone(timedelta(hours=8)))
            }
            
            # 添加到寫入隊列
            self.add_to_queue(tx_data)
            
        except Exception as e:
            logging.error(f"處理交易異常: {str(e)}")
            logging.error(f"錯誤詳情: {traceback.format_exc()}")
    
    def _determine_transaction_type_fast(self, wallet_address, token_address, chain, side, amount):
        """快速判斷交易類型"""
        position_key = self.POSITION_KEY.format(chain, wallet_address, token_address)
        current_position = float(self.redis_client.hget(position_key, 'amount') or 0)
        
        if side == 'buy':
            return "build" if current_position <= 1e-9 else "buy"
        else:
            if current_position <= 1e-9:
                return "sell"
            return "clean" if amount >= current_position - 1e-9 else "sell"
    
    def _calculate_profit_fast(self, wallet_address, token_address, chain, transaction_type, amount, price):
        """快速計算收益"""
        position_key = self.POSITION_KEY.format(chain, wallet_address, token_address)
        current_position = float(self.redis_client.hget(position_key, 'amount') or 0)
        current_cost = float(self.redis_client.hget(position_key, 'cost') or 0)
        
        if transaction_type in ["clean", "sell"] and current_position > 0:
            avg_cost = current_cost / current_position
            realized_profit = (price - avg_cost) * amount
            realized_profit_percentage = ((price / avg_cost - 1) * 100) if avg_cost > 0 else 0
            return realized_profit, realized_profit_percentage
        return 0, 0
    
    def add_to_queue(self, tx_data):
        """添加數據到寫入隊列"""
        try:
            self.write_queue.put(tx_data, timeout=1)
        except queue.Full:
            logging.warning("寫入隊列已滿，強制刷新")
            self._force_flush()
            self.write_queue.put(tx_data)
    
    def _process_batch_ultra_fast(self, batch):
        """批量處理數據"""
        if not batch:
            return
        
        retry_count = 0
        while retry_count < self.max_retries:
            try:
                engine = get_engine()
                with engine.connect() as conn:
                    # 更新統計數據
                    self._update_stats_ultra_fast(conn, batch)
                    
                self.last_flush_time = time.time()
                break
            except Exception as e:
                retry_count += 1
                if retry_count == self.max_retries:
                    logging.error(f"批量處理失敗: {str(e)}")
                else:
                    time.sleep(self.retry_delay * retry_count)
    
    def _update_stats_ultra_fast(self, conn, transactions):
        """更新統計數據"""
        try:
            # 按錢包和代幣分組
            grouped_txs = defaultdict(list)
            for tx in transactions:
                key = (tx['wallet_address'], tx['token_address'], tx['chain'])
                grouped_txs[key].append(tx)
            
            # 更新每組的統計數據
            for (wallet, token, chain), txs in grouped_txs.items():
                date = datetime.fromtimestamp(txs[0]['transaction_time'], 
                                           timezone(timedelta(hours=8))).date()
                
                # 從 Redis 獲取當前統計
                stats_key = self.STATS_KEY.format(chain, wallet, token)
                current_stats = self.redis_client.hgetall(stats_key)
                
                # 定義數值型欄位
                numeric_fields = {
                    'total_amount': float,
                    'total_cost': float,
                    'avg_buy_price': float,
                    'historical_total_buy_amount': float,
                    'historical_total_buy_cost': float,
                    'historical_total_sell_amount': float,
                    'historical_total_sell_value': float,
                    'historical_avg_buy_price': float,
                    'historical_avg_sell_price': float,
                    'realized_profit': float,
                    'realized_profit_percentage': float,
                    'total_buy_count': int,
                    'total_sell_count': int,
                    'total_holding_seconds': int,
                    'position_opened_at': int,
                    'last_transaction_time': int
                }
                
                # 轉換數據類型
                if current_stats:
                    converted_stats = {}
                    for k, v in current_stats.items():
                        if k in numeric_fields:
                            try:
                                converted_stats[k] = numeric_fields[k](v or 0)
                            except (ValueError, TypeError):
                                converted_stats[k] = 0
                        else:
                            converted_stats[k] = v
                    current_stats = converted_stats
                else:
                    current_stats = {
                        'total_amount': 0.0,
                        'total_cost': 0.0,
                        'avg_buy_price': 0.0,
                        'position_opened_at': 0,
                        'historical_total_buy_amount': 0.0,
                        'historical_total_buy_cost': 0.0,
                        'historical_total_sell_amount': 0.0,
                        'historical_total_sell_value': 0.0,
                        'historical_avg_buy_price': 0.0,
                        'historical_avg_sell_price': 0.0,
                        'last_transaction_time': 0,
                        'realized_profit': 0.0,
                        'realized_profit_percentage': 0.0,
                        'total_buy_count': 0,
                        'total_sell_count': 0,
                        'total_holding_seconds': 0
                    }
                
                # 計算歷史平均價格
                historical_avg_buy_price = (current_stats['historical_total_buy_cost'] / current_stats['historical_total_buy_amount'] 
                                         if current_stats['historical_total_buy_amount'] > 0 else 0)
                historical_avg_sell_price = (current_stats['historical_total_sell_value'] / current_stats['historical_total_sell_amount']
                                          if current_stats['historical_total_sell_amount'] > 0 else 0)
                
                # 使用原始SQL更新數據庫
                upsert_sql = '''
                INSERT INTO dex_query_v1.temp_wallet_buy_data 
                (wallet_address, token_address, chain_id, chain, date, 
                 total_amount, total_cost, avg_buy_price,
                 position_opened_at, historical_total_buy_amount, historical_total_buy_cost,
                 historical_total_sell_amount, historical_total_sell_value,
                 historical_avg_buy_price, historical_avg_sell_price,
                 total_buy_count, total_sell_count, total_holding_seconds,
                 last_transaction_time, realized_profit, realized_profit_percentage, updated_at)
                VALUES (
                    :wallet_address, :token_address, :chain_id, :chain, :date,
                    :total_amount, :total_cost, :avg_buy_price,
                    :position_opened_at, :historical_total_buy_amount, :historical_total_buy_cost,
                    :historical_total_sell_amount, :historical_total_sell_value,
                    :historical_avg_buy_price, :historical_avg_sell_price,
                    :total_buy_count, :total_sell_count, :total_holding_seconds,
                    :last_transaction_time, :realized_profit, :realized_profit_percentage, :updated_at
                )
                ON CONFLICT (wallet_address, token_address, chain, date)
                DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    total_cost = EXCLUDED.total_cost,
                    avg_buy_price = EXCLUDED.avg_buy_price,
                    position_opened_at = EXCLUDED.position_opened_at,
                    historical_total_buy_amount = EXCLUDED.historical_total_buy_amount,
                    historical_total_buy_cost = EXCLUDED.historical_total_buy_cost,
                    historical_total_sell_amount = EXCLUDED.historical_total_sell_amount,
                    historical_total_sell_value = EXCLUDED.historical_total_sell_value,
                    historical_avg_buy_price = EXCLUDED.historical_avg_buy_price,
                    historical_avg_sell_price = EXCLUDED.historical_avg_sell_price,
                    total_buy_count = EXCLUDED.total_buy_count,
                    total_sell_count = EXCLUDED.total_sell_count,
                    total_holding_seconds = EXCLUDED.total_holding_seconds,
                    last_transaction_time = EXCLUDED.last_transaction_time,
                    realized_profit = EXCLUDED.realized_profit,
                    realized_profit_percentage = EXCLUDED.realized_profit_percentage,
                    updated_at = EXCLUDED.updated_at
                '''
                
                conn.execute(text(upsert_sql), {
                    'wallet_address': wallet,
                    'token_address': token,
                    'chain': chain,
                    'chain_id': 501 if chain == 'SOLANA' else 9006,
                    'date': date,
                    'total_amount': current_stats['total_amount'],
                    'total_cost': current_stats['total_cost'],
                    'avg_buy_price': current_stats['avg_buy_price'],
                    'position_opened_at': current_stats['position_opened_at'],
                    'historical_total_buy_amount': current_stats['historical_total_buy_amount'],
                    'historical_total_buy_cost': current_stats['historical_total_buy_cost'],
                    'historical_total_sell_amount': current_stats['historical_total_sell_amount'],
                    'historical_total_sell_value': current_stats['historical_total_sell_value'],
                    'historical_avg_buy_price': historical_avg_buy_price,
                    'historical_avg_sell_price': historical_avg_sell_price,
                    'total_buy_count': current_stats['total_buy_count'],
                    'total_sell_count': current_stats['total_sell_count'],
                    'total_holding_seconds': current_stats['total_holding_seconds'],
                    'last_transaction_time': current_stats['last_transaction_time'],
                    'realized_profit': current_stats['realized_profit'],
                    'realized_profit_percentage': current_stats['realized_profit_percentage'],
                    'updated_at': datetime.now(timezone(timedelta(hours=8)))
                })
                conn.commit()
                
        except Exception as e:
            logging.error(f"更新統計失敗: {str(e)}")
            logging.error(f"錯誤詳情: {traceback.format_exc()}")
            raise
    
    def _force_flush(self):
        """強制刷新緩存到數據庫"""
        try:
            batch = []
            while not self.write_queue.empty():
                try:
                    batch.append(self.write_queue.get_nowait())
                except queue.Empty:
                    break
            
            if batch:
                self._process_batch_ultra_fast(batch)
        except Exception as e:
            logging.error(f"強制刷新失敗: {str(e)}")
    
    def shutdown(self):
        """關閉處理器"""
        try:
            self._force_flush()
            logging.info("處理器已關閉")
        except Exception as e:
            logging.error(f"關閉處理器異常: {str(e)}")
    
    def _renew_lock(self):
        while True:
            try:
                self.redis_client.expire(self.lock_key, self.lock_ttl)
            except Exception:
                pass
            time.sleep(self.lock_ttl // 2)

    def _async_flush_wallet_buy_data(self):
        with self.buffer_lock:
            buffer_copy = self.wallet_buy_data_buffer.copy()
            self.wallet_buy_data_buffer.clear()
            self.last_flush_time = time.time()
        self.executor.submit(self._flush_wallet_buy_data, buffer_copy)

    def _flush_wallet_buy_data(self, buffer_copy):
        try:
            # 分組，僅保留每個 (wallet, token, chain) 最後一筆
            latest_stats = {}
            for wallet, token, chain, stats in buffer_copy:
                key = (wallet, token, chain)
                latest_stats[key] = stats
            
            # 寫入資料庫
            engine = get_engine()
            with engine.connect() as conn:
                for (wallet, token, chain), stats in latest_stats.items():
                    # 計算持倉時長（如果還有持倉，補算到最後一筆）
                    position_key = self.POSITION_KEY.format(chain, wallet, token)
                    position = float(self.redis_client.hget(position_key, 'amount') or 0)
                    last_position_start = int(stats.get('position_opened_at', 0))
                    last_transaction_time = int(stats.get('last_transaction_time', 0))
                    total_holding_seconds = int(stats.get('total_holding_seconds', 0))
                    total_buy_amount = float(stats.get('total_buy_amount', 0))
                    total_sell_amount = float(stats.get('total_sell_amount', 0))
                    
                    # 沒有買入，則不計算持倉時長與利潤
                    if total_buy_amount == 0:
                        total_holding_seconds = 0
                        realized_profit = 0
                        realized_profit_percentage = 0
                    else:
                        # 超賣或清倉，持倉時長只到最後一次清倉
                        if position <= 0 and last_position_start > 0:
                            total_holding_seconds += last_transaction_time - last_position_start
                        # 若還有持倉，則不再累加
                        realized_profit = 0  # 這裡可根據你現有的計算方式補上
                        realized_profit_percentage = 0
                    
                    # 計算當前持倉量和成本
                    total_amount = total_buy_amount - total_sell_amount
                    
                    # 計算平均買入價格
                    avg_buy_price = (total_buy_amount / stats.get('total_buy_count', 1)) if stats.get('total_buy_count', 0) > 0 else 0
                    
                    # 使用原始SQL
                    upsert_sql = '''
                    INSERT INTO dex_query_v1.temp_wallet_buy_data 
                    (wallet_address, token_address, chain_id, chain, date, 
                     total_amount, total_cost, avg_buy_price,
                     historical_total_buy_cost, historical_total_sell_amount, historical_total_sell_value,
                     total_buy_count, total_sell_count, total_holding_seconds,
                     last_transaction_time, realized_profit, realized_profit_percentage, updated_at)
                    VALUES (
                        :wallet_address, :token_address, :chain_id, :chain, :date,
                        :total_amount, :total_cost, :avg_buy_price,
                        :historical_total_buy_cost, :historical_total_sell_amount, :historical_total_sell_value,
                        :total_buy_count, :total_sell_count, :total_holding_seconds,
                        :last_transaction_time, :realized_profit, :realized_profit_percentage, :updated_at
                    )
                    ON CONFLICT (wallet_address, token_address, chain, date)
                    DO UPDATE SET
                        total_amount = EXCLUDED.total_amount,
                        total_cost = EXCLUDED.total_cost,
                        avg_buy_price = EXCLUDED.avg_buy_price,
                        historical_total_buy_cost = EXCLUDED.historical_total_buy_cost,
                        historical_total_sell_amount = EXCLUDED.historical_total_sell_amount,
                        historical_total_sell_value = EXCLUDED.historical_total_sell_value,
                        total_buy_count = EXCLUDED.total_buy_count,
                        total_sell_count = EXCLUDED.total_sell_count,
                        total_holding_seconds = EXCLUDED.total_holding_seconds,
                        last_transaction_time = EXCLUDED.last_transaction_time,
                        realized_profit = EXCLUDED.realized_profit,
                        realized_profit_percentage = EXCLUDED.realized_profit_percentage,
                        updated_at = EXCLUDED.updated_at
                    '''
                    
                    conn.execute(text(upsert_sql), {
                        'wallet_address': wallet,
                        'token_address': token,
                        'chain': chain,
                        'chain_id': 501 if chain == 'SOLANA' else 9006,
                        'date': datetime.fromtimestamp(last_transaction_time, timezone(timedelta(hours=8))).date(),
                        'total_amount': total_amount,
                        'total_cost': total_buy_amount,  # 總買入成本
                        'avg_buy_price': avg_buy_price,
                        'historical_total_buy_cost': total_buy_amount,
                        'historical_total_sell_amount': total_sell_amount,
                        'historical_total_sell_value': total_sell_amount * avg_buy_price,  # 使用平均價格計算
                        'total_buy_count': stats.get('total_buy_count', 0),
                        'total_sell_count': stats.get('total_sell_count', 0),
                        'total_holding_seconds': total_holding_seconds,
                        'last_transaction_time': last_transaction_time,
                        'realized_profit': realized_profit,
                        'realized_profit_percentage': realized_profit_percentage,
                        'updated_at': datetime.now(timezone(timedelta(hours=8)))
                    })
                    
                conn.commit()
                
        except Exception as e:
            logging.error(f"wallet_buy_data 批量寫入失敗: {str(e)}")
            logging.error(f"錯誤詳情: {traceback.format_exc()}")

def ultra_fast_kafka_consumer():
    """Kafka 消費者"""
    processor = UltraFastKafkaProcessor()
    consumer = KafkaConsumer(
        'web3_trade_events',
        bootstrap_servers=KAFKA_BROKER,
        group_id=f'wallet_position_analyze_{int(time.time())}',
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
    
    try:
        while True:
            message_batch = consumer.poll(timeout_ms=1000)
            if message_batch:
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        processor.process_trade_ultra_fast(message.value)
    except Exception as e:
        logging.error(f"Kafka 消費異常: {str(e)}")
    finally:
        processor.shutdown()
        consumer.close()

def main():
    logging.info('=== 高性能 Kafka 消費者啟動 ===')
    ultra_fast_kafka_consumer()

if __name__ == '__main__':
    main()