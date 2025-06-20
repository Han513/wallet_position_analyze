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
import atexit
import signal
import uuid

from config import engine_smartmoney, KAFKA_BROKER

logging.basicConfig(
    level=logging.DEBUG,
    format='[KAFKA_ULTRA] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)
# 降低 kafka 相關日誌等級
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.ERROR)

# 確保連接到正確的 schema
def get_engine():
    engine = engine_smartmoney
    # 設置搜索路徑
    with engine.connect() as conn:
        conn.execute(text("SET search_path TO dex_query_v1, public"))
    return engine

class UltraFastKafkaProcessor:
    """超高速Kafka處理器 - 目標處理 100+ TPS"""
    
    def __init__(self):
        """超高速Kafka處理器 - 目標處理 100+ TPS"""
        try:
            logging.info("[KAFKA_ULTRA] 開始初始化處理器...")
            
            # Redis 緩存配置
            redis_host = os.getenv('REDIS_HOST', 'localhost')
            redis_port = int(os.getenv('REDIS_PORT', 6379))
            redis_db = int(os.getenv('REDIS_DB', 0))
            redis_password = os.getenv('REDIS_PASSWORD', None)

            redis_log_info = f"[KAFKA_ULTRA] 初始化 Redis 連接 - Host: {redis_host}, Port: {redis_port}, DB: {redis_db}"
            if redis_password:
                redis_log_info += ", Password: [REDACTED]"
            # logging.info(redis_log_info)

            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                password=redis_password,
                decode_responses=True,
                socket_timeout=5,
                socket_connect_timeout=5,
                health_check_interval=30
            )
            
            # 測試 Redis 連接
            logging.info("[KAFKA_ULTRA] 測試 Redis 連接...")
            self.redis_client.ping()
            logging.info("[KAFKA_ULTRA] Redis 連接成功")
            
            # 初始化基本配置
            logging.info("[KAFKA_ULTRA] 初始化基本配置...")
            self.lock_key = "wallet_position_analyze_lock"
            self.lock_timeout = 300
            self.lock_value = str(uuid.uuid4())
            self.POSITION_TTL = 30 * 24 * 3600
            self.WALLET_TTL = 30 * 24 * 3600
            self.STATS_TTL = 30 * 24 * 3600
            self.monitor_interval = 300
            self.last_monitor_time = time.time()
            self.error_count = 0
            self.max_errors = 1000
            self.flush_in_progress = False
            self.batch_size = 1000
            self.flush_interval = 60
            self.max_retries = 3
            self.retry_delay = 1
            
            # 初始化緩存鍵
            logging.info("[KAFKA_ULTRA] 初始化緩存鍵...")
            self.POSITION_KEY = "position:{}:{}:{}"
            self.TRANSACTION_KEY = "transaction:{}:{}:{}"
            self.STATS_KEY = "stats:{}:{}:{}"
            self.WALLET_KEY = "wallet:{}:{}"
            self.BATCH_KEY = "wallet_position:batch:{}"
            self.PENDING_UPDATES_KEY = "wallet_position:pending_updates"
            self.last_flush_time = time.time()
            
            # 清理過期鎖和批次標記
            logging.info("[KAFKA_ULTRA] 清理過期鎖和批次標記...")
            self._cleanup_stale_lock()
            self._cleanup_batch_marks()
            
            # 初始化緩存和隊列
            logging.info("[KAFKA_ULTRA] 初始化緩存和隊列...")
            self.wallet_buy_data_buffer = []
            self.buffer_lock = threading.Lock()
            self.process_queue = queue.Queue(maxsize=100000)
            self.workers = []
            self.stats_cache = {}
            self.stats_lock = threading.Lock()
            self.max_batch_wait = 1
            
            # 註冊清理函數
            logging.info("[KAFKA_ULTRA] 註冊清理函數...")
            atexit.register(self.cleanup)
            
            # 加載歷史數據
            logging.info("[KAFKA_ULTRA] 開始加載歷史數據...")
            self._load_historical_data()
            logging.info("[KAFKA_ULTRA] 歷史數據加載完成")
            
            # 啟動工作線程
            logging.info("[KAFKA_ULTRA] 啟動工作線程...")
            self.stop_flag = False
            self.worker_count = 4
            self._start_workers()
            
            # 啟動批處理線程
            logging.info("[KAFKA_ULTRA] 啟動批處理線程...")
            self.batch_thread = threading.Thread(target=self._batch_processor, daemon=True)
            self.batch_thread.start()
            
            logging.info("[KAFKA_ULTRA] 處理器初始化完成")
            
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 初始化處理器失敗: {str(e)}")
            logging.error(traceback.format_exc())
            raise

    def _cleanup_stale_lock(self):
        """清理過期的鎖"""
        try:
            # 檢查鎖是否存在
            if self.redis_client.exists(self.lock_key):
                # 獲取鎖的過期時間
                ttl = self.redis_client.ttl(self.lock_key)
                if ttl <= 0:  # 如果鎖已過期
                    logging.warning("[KAFKA_ULTRA] 發現過期鎖，正在清理...")
                    self.redis_client.delete(self.lock_key)
                    logging.info("[KAFKA_ULTRA] 過期鎖已清理")
                else:
                    # 如果鎖未過期，檢查是否為僵死鎖
                    lock_value = self.redis_client.get(self.lock_key)
                    if lock_value:
                        # 檢查進程是否存在
                        try:
                            pid = int(lock_value.split(':')[0])
                            os.kill(pid, 0)  # 檢查進程是否存在
                        except (OSError, ValueError):
                            # 進程不存在，清理鎖
                            logging.warning("[KAFKA_ULTRA] 發現僵死鎖，正在清理...")
                            self.redis_client.delete(self.lock_key)
                            logging.info("[KAFKA_ULTRA] 僵死鎖已清理")
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 清理過期鎖時發生錯誤: {str(e)}")

    def _acquire_lock(self):
        """獲取鎖，帶超時機制"""
        try:
            logging.info("[KAFKA_ULTRA] 開始嘗試獲取 Redis 鎖...")
            # 檢查 PID 文件
            pid_file = "/tmp/analyze_kafka_ultra_fast.pid"
            if os.path.exists(pid_file):
                with open(pid_file, 'r') as f:
                    old_pid = int(f.read().strip())
                    try:
                        # 檢查進程是否存在
                        os.kill(old_pid, 0)
                        # 進一步驗證進程名稱
                        with open(f"/proc/{old_pid}/cmdline", 'r') as cmd_f:
                            cmdline = cmd_f.read()
                            if "analyze_kafka_ultra_fast.py" not in cmdline:
                                logging.info(f"[KAFKA_ULTRA] PID {old_pid} 不是目標進程，清理 PID 文件")
                                os.remove(pid_file)
                            else:
                                logging.error(f"[KAFKA_ULTRA] 另一個實例 (PID: {old_pid}) 正在運行")
                                return False
                    except (OSError, IOError) as e:
                        logging.info(f"[KAFKA_ULTRA] PID {old_pid} 不存在或無法訪問，清理 PID 文件")
                        try:
                            os.remove(pid_file)
                        except OSError:
                            pass

            # 寫入新的 PID 文件
            try:
                with open(pid_file, 'w') as f:
                    f.write(str(os.getpid()))
            except OSError as e:
                logging.error(f"[KAFKA_ULTRA] 無法寫入 PID 文件: {str(e)}")
                return False

            # 使用 SET NX 命令嘗試獲取鎖
            pid = os.getpid()
            lock_value = f"{pid}:{self.lock_value}"
            logging.info(f"[KAFKA_ULTRA] 嘗試設置 Redis 鎖，PID: {pid}")
            
            # 檢查當前鎖的狀態
            current_lock = self.redis_client.get(self.lock_key)
            if current_lock:
                try:
                    lock_pid = int(current_lock.split(':')[0])
                    try:
                        os.kill(lock_pid, 0)
                        with open(f"/proc/{lock_pid}/cmdline", 'r') as cmd_f:
                            cmdline = cmd_f.read()
                            if "analyze_kafka_ultra_fast.py" not in cmdline:
                                logging.info(f"[KAFKA_ULTRA] Redis 鎖對應的 PID {lock_pid} 不是目標進程，清理鎖")
                                self.redis_client.delete(self.lock_key)
                            else:
                                logging.info(f"[KAFKA_ULTRA] Redis 鎖被進程 {lock_pid} 持有")
                                return False
                    except (OSError, IOError):
                        logging.info(f"[KAFKA_ULTRA] Redis 鎖對應的 PID {lock_pid} 不存在，清理鎖")
                        self.redis_client.delete(self.lock_key)
                except (ValueError, IndexError):
                    logging.warning(f"[KAFKA_ULTRA] Redis 鎖值格式錯誤: {current_lock}，清理鎖")
                    self.redis_client.delete(self.lock_key)
            
            # 使用 Lua 腳本確保原子性
            lua_script = """
            local key = KEYS[1]
            local value = ARGV[1]
            local ttl = ARGV[2]
            
            if redis.call('exists', key) == 0 then
                redis.call('set', key, value, 'EX', ttl)
                return 1
            end
            
            local current = redis.call('get', key)
            if current == value then
                redis.call('expire', key, ttl)
                return 1
            end
            
            return 0
            """
            
            # 註冊並執行 Lua 腳本
            set_lock = self.redis_client.register_script(lua_script)
            acquired = set_lock(keys=[self.lock_key], args=[lock_value, self.lock_timeout])
            
            if acquired:
                logging.info("[KAFKA_ULTRA] 成功獲取 Redis 鎖")
                return True
            else:
                logging.warning("[KAFKA_ULTRA] 無法獲取 Redis 鎖")
                return False
            
        except redis.RedisError as e:
            logging.error(f"[KAFKA_ULTRA] Redis 操作失敗: {str(e)}")
            logging.error(traceback.format_exc())
            return False
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 獲取鎖時發生錯誤: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    def _release_lock(self):
        """釋放鎖"""
        try:
            # 使用 Lua 腳本確保原子性操作
            script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            """
            result = self.redis_client.eval(
                script,
                1,
                self.lock_key,
                self.lock_value
            )
            if result:
                logging.info("[KAFKA_ULTRA] 成功釋放鎖")
            else:
                logging.warning("[KAFKA_ULTRA] 釋放鎖失敗，可能已被其他進程釋放")
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 釋放鎖時發生錯誤: {str(e)}")

    def _renew_lock(self):
        """續期鎖"""
        try:
            # 使用 Lua 腳本確保原子性操作
            script = """
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("expire", KEYS[1], ARGV[2])
            else
                return 0
            end
            """
            result = self.redis_client.eval(
                script,
                1,
                self.lock_key,
                self.lock_value,
                self.lock_timeout
            )
            if result:
                logging.debug("[KAFKA_ULTRA] 成功續期鎖")
            else:
                logging.debug("[KAFKA_ULTRA] 續期鎖失敗，可能已被其他進程釋放")
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 續期鎖時發生錯誤: {str(e)}")

    def cleanup(self):
        """清理資源"""
        try:
            logging.info("[KAFKA_ULTRA] 開始清理資源...")
            
            # 停止批處理線程
            self.stop_flag = True
            if hasattr(self, 'batch_thread'):
                self.batch_thread.join(timeout=10)
            
            # 清理批次標記
            self._cleanup_batch_marks()
            
            # 釋放 Redis 鎖
            self._release_lock()
            
            # 關閉 Redis 連接
            if hasattr(self, 'redis_client'):
                self.redis_client.close()
            
            # 刪除 PID 文件
            pid_file = "/tmp/analyze_kafka_ultra_fast.pid"
            try:
                if os.path.exists(pid_file):
                    with open(pid_file, 'r') as f:
                        pid = int(f.read().strip())
                        if pid == os.getpid():  # 只刪除自己的 PID 文件
                            os.remove(pid_file)
            except (OSError, ValueError) as e:
                logging.error(f"[KAFKA_ULTRA] 刪除 PID 文件時發生錯誤: {str(e)}")
            
            logging.info("[KAFKA_ULTRA] 資源清理完成")
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 清理資源時發生錯誤: {str(e)}")

    def _cleanup_batch_marks(self):
        """清理殘留的批次標記"""
        try:
            # 清理所有批次標記
            batch_keys = self.redis_client.keys(self.BATCH_KEY.format("*"))
            if batch_keys:
                self.redis_client.delete(*batch_keys)
            # 清理待更新集合
            self.redis_client.delete(self.PENDING_UPDATES_KEY)
            logging.info("[KAFKA_ULTRA] 清理批次標記完成")
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 清理批次標記失敗: {str(e)}")

    def _mark_for_update(self, wallet_address, token_address, chain, date):
        """標記需要更新的記錄"""
        try:
            batch_key = f"{chain}:{wallet_address}:{token_address}:{date.strftime('%Y-%m-%d')}"
            self.redis_client.sadd(self.PENDING_UPDATES_KEY, batch_key)
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 標記更新失敗: {str(e)}")

    def _batch_processor(self):
        """批量處理線程"""
        while not self.stop_flag:
            try:
                current_time = time.time()
                pending_updates = self.redis_client.smembers(self.PENDING_UPDATES_KEY)
                
                if pending_updates and (len(pending_updates) >= self.batch_size or 
                                      current_time - self.last_flush_time >= self.flush_interval):
                    logging.info(f"[KAFKA_ULTRA] 開始處理批次，大小: {len(pending_updates)}")
                    
                    # 分批處理
                    batch = []
                    processed_keys = set()
                    
                    for batch_key in pending_updates:
                        try:
                            chain, wallet, token, date_str = batch_key.split(":")
                            stats_key = self.STATS_KEY.format(chain, wallet, token)
                            stats = self.redis_client.hgetall(stats_key)
                            
                            if not stats:
                                continue
                                
                            # 轉換數據類型
                            stats = self._convert_stats_types(stats)
                            
                            # 添加到批次
                            batch.append({
                                'wallet_address': wallet,
                                'token_address': token,
                                'chain': chain,
                                'date': datetime.strptime(date_str, '%Y-%m-%d').date(),
                                'stats': stats
                            })
                            
                            processed_keys.add(batch_key)
                            
                            # 達到批次大小時處理
                            if len(batch) >= 1000:
                                self._process_batch(batch)
                                batch = []
                                # 從待更新集合中移除已處理的記錄
                                if processed_keys:
                                    self.redis_client.srem(self.PENDING_UPDATES_KEY, *processed_keys)
                                    processed_keys = set()
                                
                        except Exception as e:
                            logging.error(f"[KAFKA_ULTRA] 處理批次記錄失敗: {str(e)}")
                            continue
                    
                    # 處理剩餘的批次
                    if batch:
                        self._process_batch(batch)
                        # 從待更新集合中移除已處理的記錄
                        if processed_keys:
                            self.redis_client.srem(self.PENDING_UPDATES_KEY, *processed_keys)
                    
                    self.last_flush_time = current_time
                    # logging.info("[KAFKA_ULTRA] 批次處理完成")
                
                # 休眠一段時間
                time.sleep(1)
                
            except Exception as e:
                logging.error(f"[KAFKA_ULTRA] 批量處理異常: {str(e)}")
                logging.error(traceback.format_exc())
                time.sleep(5)  # 發生錯誤時多等待一會

    def _process_batch(self, batch):
        """處理一批數據"""
        if not batch:
            return
        
        engine = get_engine()
        try:
            with engine.connect() as conn:
                try:
                    with conn.begin():
                        for item in batch:
                            try:
                                # 數據驗證
                                if item['stats'].get('total_amount', 0) < -1e20 or item['stats'].get('total_amount', 0) > 1e20:
                                    logging.warning(f"[KAFKA_ULTRA] 數據異常，跳過處理: {item}")
                                    continue
                                params = {
                                    'wallet_address': item['wallet_address'],
                                    'token_address': item['token_address'],
                                    'chain': item['chain'],
                                    'chain_id': 501 if item['chain'] == 'SOLANA' else 9006,
                                    'date': item['date'],
                                    'total_amount': item['stats']['total_amount'],
                                    'total_cost': item['stats']['total_cost'],
                                    'avg_buy_price': item['stats']['avg_buy_price'],
                                    'position_opened_at': item['stats']['position_opened_at'],
                                    'historical_total_buy_amount': item['stats']['historical_total_buy_amount'],
                                    'historical_total_buy_cost': item['stats']['historical_total_buy_cost'],
                                    'historical_total_sell_amount': item['stats']['historical_total_sell_amount'],
                                    'historical_total_sell_value': item['stats']['historical_total_sell_value'],
                                    'historical_avg_buy_price': item['stats']['historical_avg_buy_price'],
                                    'historical_avg_sell_price': item['stats']['historical_avg_sell_price'],
                                    'total_buy_count': item['stats']['total_buy_count'],
                                    'total_sell_count': item['stats']['total_sell_count'],
                                    'total_holding_seconds': item['stats']['total_holding_seconds'],
                                    'last_transaction_time': item['stats']['last_transaction_time'],
                                    'realized_profit': item['stats']['realized_profit'],
                                    'realized_profit_percentage': max(min(float(item['stats']['realized_profit_percentage']), 10000), -100),
                                    'updated_at': datetime.now(timezone(timedelta(hours=8)))
                                }
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
                                conn.execute(text(upsert_sql), params)
                            except Exception as e:
                                logging.error(f"[KAFKA_ULTRA] SQL執行失敗: {str(e)}")
                                logging.error(f"[KAFKA_ULTRA] 參數: {params}")
                                raise
                    conn.commit()
                    logging.info(f"[KAFKA_ULTRA] 成功處理 {len(batch)} 條記錄")
                except Exception as batch_e:
                    logging.error(f"[KAFKA_ULTRA] 批量處理失敗，開始單筆重試: {str(batch_e)}")
                    logging.error(traceback.format_exc())
                    # 單筆重試
                    for item in batch:
                        try:
                            with conn.begin():
                                params = {
                                    'wallet_address': item['wallet_address'],
                                    'token_address': item['token_address'],
                                    'chain': item['chain'],
                                    'chain_id': 501 if item['chain'] == 'SOLANA' else 9006,
                                    'date': item['date'],
                                    'total_amount': item['stats']['total_amount'],
                                    'total_cost': item['stats']['total_cost'],
                                    'avg_buy_price': item['stats']['avg_buy_price'],
                                    'position_opened_at': item['stats']['position_opened_at'],
                                    'historical_total_buy_amount': item['stats']['historical_total_buy_amount'],
                                    'historical_total_buy_cost': item['stats']['historical_total_buy_cost'],
                                    'historical_total_sell_amount': item['stats']['historical_total_sell_amount'],
                                    'historical_total_sell_value': item['stats']['historical_total_sell_value'],
                                    'historical_avg_buy_price': item['stats']['historical_avg_buy_price'],
                                    'historical_avg_sell_price': item['stats']['historical_avg_sell_price'],
                                    'total_buy_count': item['stats']['total_buy_count'],
                                    'total_sell_count': item['stats']['total_sell_count'],
                                    'total_holding_seconds': item['stats']['total_holding_seconds'],
                                    'last_transaction_time': item['stats']['last_transaction_time'],
                                    'realized_profit': item['stats']['realized_profit'],
                                    'realized_profit_percentage': max(min(float(item['stats']['realized_profit_percentage']), 10000), -100),
                                    'updated_at': datetime.now(timezone(timedelta(hours=8)))
                                }
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
                                conn.execute(text(upsert_sql), params)
                        except Exception as single_e:
                            logging.error(f"[KAFKA_ULTRA] 單筆寫入失敗: {str(single_e)}")
                            logging.error(f"[KAFKA_ULTRA] 參數: {params}")
                            logging.error(traceback.format_exc())
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 批量處理失敗: {str(e)}")
            logging.error(traceback.format_exc())

    def _convert_stats_types(self, stats_from_redis):
        """
        轉換從 Redis 讀取的統計數據（字符串）為正確的數字類型。
        同時確保所有必要的鍵都存在，為缺失的鍵提供默認值。
        """
        # 從一個完整的默認統計對象開始
        converted = self._init_stats()
        
        # 遍歷默認對象的所有鍵，以確保最終字典是完整的
        for key, default_value in converted.items():
            # 從 Redis 獲取的值是字符串
            redis_value = stats_from_redis.get(key)
            
            if redis_value is not None:
                try:
                    # 根據默認值的類型來進行轉換
                    value_type = type(default_value)
                    converted[key] = value_type(redis_value)
                except (ValueError, TypeError):
                    # 如果轉換失敗（例如，空字符串''），則使用默認值
                    converted[key] = default_value
            # 如果 Redis 中沒有這個鍵，則保留默認值，無需操作
            
        return converted

    def _load_historical_data(self):
        """從 temp_wallet_buy_data 聚合加載數據到 Redis 緩存"""
        try:
            logging.info("[KAFKA_ULTRA] 開始加載歷史數據...")
            engine = get_engine()
            with engine.connect() as conn:
                logging.info("[KAFKA_ULTRA] 執行歷史數據查詢...")
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
                rows = result.fetchall()
                logging.info(f"[KAFKA_ULTRA] 查詢到 {len(rows)} 條歷史記錄")
                
                # 批量處理數據
                batch_size = 1000
                total_processed = 0
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i + batch_size]
                    for row in batch:
                        key = self.STATS_KEY.format(row.chain, row.wallet_address, row.token_address)
                        row_dict = dict(row._mapping)
                        redis_data = {k: str(v) if v is not None else '0' for k, v in row_dict.items()}
                        self.redis_client.hset(key, mapping=redis_data)
                    total_processed += len(batch)
                    logging.info(f"[KAFKA_ULTRA] 已處理 {total_processed}/{len(rows)} 條記錄")
                
                logging.info("[KAFKA_ULTRA] 歷史數據加載完成")
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 加載歷史數據失敗: {str(e)}")
            logging.error(traceback.format_exc())
            raise
    
    def _start_workers(self):
        """啟動工作線程"""
        for _ in range(self.worker_count):
            worker = threading.Thread(target=self._process_worker, daemon=True)
            worker.start()
            self.workers.append(worker)
    
    def _process_worker(self):
        """工作線程處理函數"""
        batch = []
        last_process_time = time.time()
        
        while True:
            try:
                # 收集批次數據
                try:
                    while len(batch) < self.batch_size:
                        # 如果等待時間超過閾值，處理當前批次
                        if time.time() - last_process_time > self.max_batch_wait and batch:
                            break
                        
                        item = self.process_queue.get(timeout=0.1)
                        batch.append(item)
                except queue.Empty:
                    if not batch:
                        continue
                
                # 按 chain:wallet:token 分組處理
                grouped_data = {}
                for item in batch:
                    key = f"{item['chain']}:{item['wallet_address']}:{item['token_address']}"
                    if key not in grouped_data:
                        grouped_data[key] = []
                    grouped_data[key].append(item)
                
                # 處理每個分組
                for key, items in grouped_data.items():
                    chain, wallet, token = key.split(":")
                    self._process_group(chain, wallet, token, items)
                
                # 清空批次
                batch = []
                last_process_time = time.time()
                
            except Exception as e:
                logging.error(f"[KAFKA_ULTRA] 工作線程處理異常: {str(e)}")
                logging.error(traceback.format_exc())
                time.sleep(1)
    
    def _process_group(self, chain, wallet, token, items):
        """處理單個分組的數據"""
        try:
            # 獲取或初始化統計數據
            stats_key = self.STATS_KEY.format(chain, wallet, token)
            stats = None
            with self.stats_lock:
                if stats_key in self.stats_cache:
                    stats = self.stats_cache[stats_key].copy()
                else:
                    stats_from_redis = self.redis_client.hgetall(stats_key)
                    if not stats_from_redis:
                        stats = self._init_stats()
                    else:
                        stats = self._convert_stats_types(stats_from_redis)
                    self.stats_cache[stats_key] = stats.copy()
            
            # 按時間排序處理
            items.sort(key=lambda x: x['timestamp'])
            
            # 更新統計數據
            for item in items:
                self._update_stats(stats, item)
            
            # 更新緩存
            with self.stats_lock:
                self.stats_cache[stats_key] = stats.copy()
            
            # 批量寫入數據庫
            self._batch_write_to_db(chain, wallet, token, stats, items[-1]['timestamp'])
            
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 處理分組數據異常: {str(e)}")
            logging.error(traceback.format_exc())
    
    def _init_stats(self):
        """初始化統計數據"""
        return {
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
            'total_buy_count': 0,
            'total_sell_count': 0,
            'total_holding_seconds': 0,
            'last_transaction_time': 0,
            'realized_profit': 0.0,
            'realized_profit_percentage': 0.0
        }
    
    def _update_stats(self, stats, item):
        """更新統計數據"""
        try:
            amount_before_tx = stats['total_amount']

            # 更新統計數據
            if item['side'] == 'buy':
                # 如果是新開倉（之前的持倉為0），則記錄開倉時間
                if amount_before_tx <= 1e-9:
                    stats['position_opened_at'] = item['timestamp']

                stats['total_amount'] += item['amount']
                stats['total_cost'] += (item['amount'] * item['price'])
                stats['historical_total_buy_amount'] += item['amount']
                stats['historical_total_buy_cost'] += (item['amount'] * item['price'])
                stats['total_buy_count'] += 1

            else:  # 'sell' side
                # 計算收益
                avg_cost = stats['avg_buy_price']
                if avg_cost > 0:
                    realized_profit = (item['price'] - avg_cost) * item['amount']
                    realized_profit_percentage = ((item['price'] / avg_cost - 1) * 100)
                    realized_profit_percentage = max(min(realized_profit_percentage, 10000), -100)
                    stats['realized_profit'] += realized_profit
                    stats['realized_profit_percentage'] += realized_profit_percentage
                
                # 更新持倉
                stats['total_amount'] -= item['amount']

                # 如果平倉（持倉歸零），則計算持倉時間
                if stats['total_amount'] <= 1e-9:
                    if stats['position_opened_at'] > 0:
                        time_held = item['timestamp'] - stats['position_opened_at']
                        if time_held > 0:
                            stats['total_holding_seconds'] += time_held
                    # 重置倉位相關數據
                    stats['total_cost'] = 0
                    stats['avg_buy_price'] = 0
                    stats['position_opened_at'] = 0
                else:
                    # 部分賣出，按比例減少成本
                    if amount_before_tx > 0:
                        cost_ratio = item['amount'] / amount_before_tx
                        stats['total_cost'] *= (1 - cost_ratio)
                
                stats['historical_total_sell_amount'] += item['amount']
                stats['historical_total_sell_value'] += (item['amount'] * item['price'])
                stats['total_sell_count'] += 1
            
            # 更新均價
            if stats['total_amount'] > 0:
                stats['avg_buy_price'] = stats['total_cost'] / stats['total_amount']
            else: # amount <= 0, avg_buy_price should be 0
                 stats['avg_buy_price'] = 0

            if stats['historical_total_buy_amount'] > 0:
                stats['historical_avg_buy_price'] = stats['historical_total_buy_cost'] / stats['historical_total_buy_amount']
            if stats['historical_total_sell_amount'] > 0:
                stats['historical_avg_sell_price'] = stats['historical_total_sell_value'] / stats['historical_total_sell_amount']
            
            # 更新時間
            stats['last_transaction_time'] = item['timestamp']
            
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 更新統計數據異常: {str(e)}")
            raise
    
    def process_trade_ultra_fast(self, trade_data):
        """快速處理交易數據"""
        try:
            # 解析交易數據
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
            
            # 時間戳處理
            if timestamp > 1e12:
                timestamp = int(timestamp / 1000)
            
            # 添加到處理隊列
            self.process_queue.put({
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain': chain,
                'side': side,
                'amount': amount,
                'price': price,
                'timestamp': timestamp
            })
            
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 處理交易異常: {str(e)}")
            logging.error(traceback.format_exc())

    def _batch_write_to_db(self, chain, wallet, token, stats, timestamp):
        """批量寫入數據庫"""
        try:
            # 計算日期
            date = datetime.fromtimestamp(timestamp).date()
            
            # 數據驗證
            if stats.get('total_amount', 0) < -1e20 or stats.get('total_amount', 0) > 1e20:
                logging.warning(f"[KAFKA_ULTRA] 數據異常，跳過處理: chain={chain}, wallet={wallet}, token={token}")
                return
                
            # 準備參數
            params = {
                'wallet_address': wallet,
                'token_address': token,
                'chain': chain,
                'chain_id': 501 if chain == 'SOLANA' else 9006,
                'date': date,
                'total_amount': float(stats['total_amount']),
                'total_cost': float(stats['total_cost']),
                'avg_buy_price': float(stats['avg_buy_price']),
                'position_opened_at': int(stats['position_opened_at']),
                'historical_total_buy_amount': float(stats['historical_total_buy_amount']),
                'historical_total_buy_cost': float(stats['historical_total_buy_cost']),
                'historical_total_sell_amount': float(stats['historical_total_sell_amount']),
                'historical_total_sell_value': float(stats['historical_total_sell_value']),
                'historical_avg_buy_price': float(stats['historical_avg_buy_price']),
                'historical_avg_sell_price': float(stats['historical_avg_sell_price']),
                'total_buy_count': int(stats['total_buy_count']),
                'total_sell_count': int(stats['total_sell_count']),
                'total_holding_seconds': int(stats['total_holding_seconds']),
                'last_transaction_time': int(stats['last_transaction_time']),
                'realized_profit': float(stats['realized_profit']),
                'realized_profit_percentage': max(min(float(stats['realized_profit_percentage']), 10000), -100),
                'updated_at': datetime.now(timezone(timedelta(hours=8)))
            }
            
            # 執行 upsert
            engine = get_engine()
            with engine.connect() as conn:
                with conn.begin():
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
                    conn.execute(text(upsert_sql), params)
                    
            # 更新 Redis 緩存
            stats_key = self.STATS_KEY.format(chain, wallet, token)
            with self.stats_lock:
                self.stats_cache[stats_key] = stats.copy()
                
            # 標記需要更新
            self._mark_for_update(wallet, token, chain, date)
            
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] 批量寫入數據庫失敗: {str(e)}")
            logging.error(traceback.format_exc())
            raise

def check_and_create_table(engine):
    """檢查並創建必要的數據表"""
    try:
        with engine.connect() as conn:
            with conn.begin():  # 使用事務確保原子性
                # 檢查 schema 是否存在
                check_schema_sql = "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dex_query_v1')"
                schema_exists = conn.execute(text(check_schema_sql)).scalar()
                if not schema_exists:
                    logging.info("[KAFKA_ULTRA] Schema 'dex_query_v1' 不存在，自動創建...")
                    conn.execute(text("CREATE SCHEMA dex_query_v1"))
                    logging.info("[KAFKA_ULTRA] Schema 'dex_query_v1' 已創建")

                # 設置搜索路徑以確保後續操作在正確的 schema 中
                conn.execute(text("SET search_path TO dex_query_v1, public"))

                # 檢查表是否存在
                check_sql = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'dex_query_v1' 
                        AND table_name = 'temp_wallet_buy_data'
                    )
                """
                exists = conn.execute(text(check_sql)).scalar()
                
                if not exists:
                    logging.info("[KAFKA_ULTRA] Table temp_wallet_buy_data 不存在，自動創建...")
                    create_table_sql = """
CREATE TABLE dex_query_v1.temp_wallet_buy_data (
    wallet_address character varying(255) NOT NULL,
    token_address character varying(255) NOT NULL,
    chain character varying(50) NOT NULL,
    date date NOT NULL,
    total_amount numeric(40, 18) NOT NULL DEFAULT 0,
    total_cost numeric(40, 18) NOT NULL DEFAULT 0,
    avg_buy_price numeric(40, 18) NOT NULL DEFAULT 0,
    position_opened_at bigint NOT NULL DEFAULT 0,
    historical_total_buy_amount numeric(40, 18) NOT NULL DEFAULT 0,
    historical_total_buy_cost numeric(40, 18) NOT NULL DEFAULT 0,
    historical_total_sell_amount numeric(40, 18) NOT NULL DEFAULT 0,
    historical_total_sell_value numeric(40, 18) NOT NULL DEFAULT 0,
    historical_avg_buy_price numeric(40, 18) NOT NULL DEFAULT 0,
    historical_avg_sell_price numeric(40, 18) NOT NULL DEFAULT 0,
    total_buy_count integer NOT NULL DEFAULT 0,
    total_sell_count integer NOT NULL DEFAULT 0,
    total_holding_seconds bigint NOT NULL DEFAULT 0,
    last_transaction_time bigint NOT NULL DEFAULT 0,
    realized_profit numeric(40, 18) NOT NULL DEFAULT 0,
    realized_profit_percentage numeric(40, 18) NOT NULL DEFAULT 0,
    chain_id integer NOT NULL DEFAULT 0,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wallet_address, token_address, chain, date)
);
                    """
                    conn.execute(text(create_table_sql))
                    logging.info("[KAFKA_ULTRA] Table temp_wallet_buy_data 已創建")
                else:
                    logging.info("[KAFKA_ULTRA] Table temp_wallet_buy_data 已存在")
    except Exception as e:
        logging.error(f"[KAFKA_ULTRA] 檢查/創建表失敗: {str(e)}")
        raise

def ultra_fast_kafka_consumer():
    """超快速 Kafka 消費者"""
    processor = None
    consumer = None
    
    def signal_handler(signum, frame):
        """信號處理函數"""
        logging.info("[KAFKA_ULTRA] 收到終止信號，開始清理資源...")
        if processor:
            processor.cleanup()
        if consumer:
            consumer.close()
        logging.info("[KAFKA_ULTRA] 資源清理完成，程序退出")
        sys.exit(0)
    
    # 註冊信號處理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        logging.info("[KAFKA_ULTRA] 開始初始化處理器...")
        # 初始化處理器
        processor = UltraFastKafkaProcessor()
        
        # 嘗試獲取鎖
        logging.info("[KAFKA_ULTRA] 嘗試獲取處理器鎖...")
        lock_acquired = processor._acquire_lock()
        if not lock_acquired:
            logging.error("[KAFKA_ULTRA] 無法獲取鎖，程序退出")
            return
        
        logging.info("[KAFKA_ULTRA] 成功獲取處理器鎖，開始初始化 Kafka 消費者...")
        
        # 初始化 Kafka 消費者
        kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
        kafka_topic = os.getenv('KAFKA_TOPIC', 'web3_trade_events')
        group_id = f'wallet_position_analyze_group_{int(time.time())}'
        logging.info(f"[KAFKA_ULTRA] Kafka 配置 - Broker: {kafka_broker}, Topic: {kafka_topic}, Group: {group_id}")
        
        try:
            logging.info("[KAFKA_ULTRA] 創建 Kafka 消費者實例...")
            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=kafka_broker,
                group_id=group_id,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                max_poll_records=1000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                request_timeout_ms=60000,
                connections_max_idle_ms=300000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                api_version=(2, 5, 0),
                security_protocol="PLAINTEXT"
            )
            
            # 測試連接
            logging.info("[KAFKA_ULTRA] 測試 Kafka 連接...")
            topics = consumer.topics()
            logging.info(f"[KAFKA_ULTRA] Kafka 連接成功，可用 topics: {topics}")
            
            # 確認訂閱
            logging.info(f"[KAFKA_ULTRA] 訂閱 topic: {kafka_topic}")
            consumer.subscribe([kafka_topic])
            
        except Exception as e:
            logging.error(f"[KAFKA_ULTRA] Kafka 初始化失敗: {str(e)}")
            logging.error(traceback.format_exc())
            raise
        
        logging.info("[KAFKA_ULTRA] 開始消費消息...")
        
        # 消費消息主循環
        while True:
            try:
                logging.debug("[KAFKA_ULTRA] 等待新消息...")
                message_batch = consumer.poll(timeout_ms=1000)
                if message_batch:
                    logging.info(f"[KAFKA_ULTRA] 收到消息批次: {len(message_batch)} 個分區有數據")
                    for topic_partition, messages in message_batch.items():
                        logging.debug(f"[KAFKA_ULTRA] 處理分區 {topic_partition} 的 {len(messages)} 條消息")
                        for message in messages:
                            processor.process_trade_ultra_fast(message.value)
                else:
                    logging.debug("[KAFKA_ULTRA] 當前無新消息")
            except Exception as e:
                logging.error(f"[KAFKA_ULTRA] 處理消息時發生錯誤: {str(e)}")
                logging.error(traceback.format_exc())
                time.sleep(1)
                continue
                    
    except Exception as e:
        logging.error(f"[KAFKA_ULTRA] 程序異常: {str(e)}")
        logging.error(traceback.format_exc())
    finally:
        logging.info("[KAFKA_ULTRA] 程序結束，開始清理資源...")
        if processor:
            processor.cleanup()
        if consumer:
            consumer.close()
        logging.info("[KAFKA_ULTRA] 資源清理完成")

def main():
    """主函數"""
    try:
        # 檢查並創建必要的數據表
        check_and_create_table(engine_smartmoney)
        
        # 啟動 Kafka 消費者
        ultra_fast_kafka_consumer()
    except KeyboardInterrupt:
        logging.info("[KAFKA_ULTRA] 收到鍵盤中斷信號，程序退出")
    except Exception as e:
        logging.error(f"[KAFKA_ULTRA] 程序異常: {str(e)}")
        logging.error(f"[KAFKA_ULTRA] 錯誤詳情: {traceback.format_exc()}")
    finally:
        logging.info("[KAFKA_ULTRA] 程序結束")

if __name__ == "__main__":
    main()