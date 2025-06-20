import os
import logging
import json
import time
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
import redis
from kafka import KafkaConsumer
import threading
import queue

# 配置
NETWORK = os.getenv('NETWORK', 'SOLANA')  # 啟動時指定
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'web3_trade_events')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
DB_URL = os.getenv('DATABASE_SMARTMONEY')  # 需設置正確的資料庫連線字串
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 100))
FLUSH_INTERVAL = int(os.getenv('FLUSH_INTERVAL', 5))  # 秒
KAFKA_WORKER_NUM = int(os.getenv('KAFKA_WORKER_NUM', 4))
KAFKA_QUEUE_MAXSIZE = int(os.getenv('KAFKA_QUEUE_MAXSIZE', 10000))

logging.basicConfig(
    level=logging.INFO,
    format='[NETWORK_KAFKA] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

# 所有統計欄位
numeric_fields = {
    'total_amount': float,
    'total_cost': float,
    'avg_buy_price': float,
    'position_opened_at': int,
    'historical_total_buy_amount': float,
    'historical_total_buy_cost': float,
    'historical_total_sell_amount': float,
    'historical_total_sell_value': float,
    'historical_avg_buy_price': float,
    'historical_avg_sell_price': float,
    'total_buy_count': int,
    'total_sell_count': int,
    'total_holding_seconds': int,
    'last_transaction_time': int,
    'realized_profit': float,
    'realized_profit_percentage': float
}

class NetworkKafkaProcessor:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True
        )
        self.engine = create_engine(DB_URL, pool_pre_ping=True)
        self.STATS_KEY = "stats:{}:{}:{}:{}"  # network, chain, wallet, token
        self.BATCH = []
        self.last_flush = time.time()
        self.queue = queue.Queue(maxsize=KAFKA_QUEUE_MAXSIZE)
        self.lock = threading.Lock()  # 保護BATCH

    def _init_stats(self):
        return {k: 0 for k in numeric_fields}

    def _convert_stats_types(self, stats):
        converted = {}
        for k, v in stats.items():
            if k in numeric_fields:
                try:
                    converted[k] = numeric_fields[k](v or 0)
                except (ValueError, TypeError):
                    converted[k] = numeric_fields[k](0)
            else:
                converted[k] = v
        return converted

    def process_trade(self, trade_data):
        event = trade_data.get('event', {})
        network = event.get('network')
        if network != NETWORK:
            return  # 只處理本 network 的數據
        wallet_address = event.get('address')
        token_address = event.get('tokenAddress')
        chain = event.get('network')
        side = event.get('side')
        amount = float(event.get('txnValue', 0))
        price = float(event.get('price', 0))
        timestamp = int(event.get('time', 0))
        if timestamp > 1e12:
            timestamp = int(timestamp / 1000)
        stats_key = self.STATS_KEY.format(network, chain, wallet_address, token_address)
        stats = self.redis_client.hgetall(stats_key)
        if not stats:
            stats = self._init_stats()
        stats = self._convert_stats_types(stats)
        # 更新統計邏輯（完全對齊 analyze_kafka_ultra_fast.py）
        if side == 'buy':
            stats['total_amount'] += amount
            stats['total_cost'] += amount * price
            stats['historical_total_buy_amount'] += amount
            stats['historical_total_buy_cost'] += amount * price
            stats['total_buy_count'] += 1
            if stats['total_amount'] <= amount:
                stats['position_opened_at'] = timestamp
        else:
            avg_cost = stats['avg_buy_price']
            if avg_cost > 0:
                realized_profit = (price - avg_cost) * amount
                realized_profit_percentage = ((price / avg_cost - 1) * 100)
                realized_profit_percentage = max(min(realized_profit_percentage, 10000), -100)
                stats['realized_profit'] += realized_profit
                stats['realized_profit_percentage'] += realized_profit_percentage
            stats['total_amount'] -= amount
            if stats['total_amount'] <= 1e-9:
                stats['total_cost'] = 0
                stats['avg_buy_price'] = 0
            else:
                cost_ratio = amount / (stats['total_amount'] + amount)
                stats['total_cost'] *= (1 - cost_ratio)
            stats['historical_total_sell_amount'] += amount
            stats['historical_total_sell_value'] += amount * price
            stats['total_sell_count'] += 1
        # 均價計算
        if stats['total_amount'] > 0:
            stats['avg_buy_price'] = stats['total_cost'] / stats['total_amount']
        if stats['historical_total_buy_amount'] > 0:
            stats['historical_avg_buy_price'] = stats['historical_total_buy_cost'] / stats['historical_total_buy_amount']
        if stats['historical_total_sell_amount'] > 0:
            stats['historical_avg_sell_price'] = stats['historical_total_sell_value'] / stats['historical_total_sell_amount']
        stats['last_transaction_time'] = timestamp
        # 寫回 Redis
        self.redis_client.hset(stats_key, mapping={k: str(v) for k, v in stats.items()})
        # 加入批次
        with self.lock:
            self.BATCH.append({
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain': chain,
                'chain_id': 501 if chain == 'SOLANA' else 9006,
                'date': datetime.fromtimestamp(timestamp, tz=timezone(timedelta(hours=8))).date(),
                'total_amount': stats['total_amount'],
                'total_cost': stats['total_cost'],
                'avg_buy_price': stats['avg_buy_price'],
                'position_opened_at': stats['position_opened_at'],
                'historical_total_buy_amount': stats['historical_total_buy_amount'],
                'historical_total_buy_cost': stats['historical_total_buy_cost'],
                'historical_total_sell_amount': stats['historical_total_sell_amount'],
                'historical_total_sell_value': stats['historical_total_sell_value'],
                'historical_avg_buy_price': stats['historical_avg_buy_price'],
                'historical_avg_sell_price': stats['historical_avg_sell_price'],
                'total_buy_count': int(stats['total_buy_count']),
                'total_sell_count': int(stats['total_sell_count']),
                'total_holding_seconds': int(stats['total_holding_seconds']),
                'last_transaction_time': int(stats['last_transaction_time']),
                'realized_profit': stats['realized_profit'],
                'realized_profit_percentage': max(min(float(stats['realized_profit_percentage']), 10000), -100),
                'updated_at': datetime.now(timezone(timedelta(hours=8)))
            })
            if len(self.BATCH) >= BATCH_SIZE or (time.time() - self.last_flush) > FLUSH_INTERVAL:
                self.flush_batch()

    def flush_batch(self):
        with self.lock:
            if not self.BATCH:
                return
            upsert_sql = '''
            INSERT INTO dex_query_v1.temp_wallet_buy_data 
            (wallet_address, token_address, chain_id, chain, date, 
             total_amount, total_cost, avg_buy_price, position_opened_at, 
             historical_total_buy_amount, historical_total_buy_cost, 
             historical_total_sell_amount, historical_total_sell_value, 
             historical_avg_buy_price, historical_avg_sell_price, 
             total_buy_count, total_sell_count, total_holding_seconds, 
             last_transaction_time, realized_profit, realized_profit_percentage, updated_at)
            VALUES (
                :wallet_address, :token_address, :chain_id, :chain, :date,
                :total_amount, :total_cost, :avg_buy_price, :position_opened_at,
                :historical_total_buy_amount, :historical_total_buy_cost,
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
            try:
                with self.engine.connect() as conn:
                    with conn.begin():
                        for item in self.BATCH:
                            conn.execute(text(upsert_sql), item)
                logging.info(f"[NETWORK_KAFKA] 批次寫入 {len(self.BATCH)} 筆數據到DB")
            except Exception as e:
                logging.error(f"[NETWORK_KAFKA] 批次寫入失敗: {str(e)}")
            self.BATCH.clear()
            self.last_flush = time.time()

    def worker_loop(self):
        while True:
            try:
                trade_data = self.queue.get(timeout=1)
                self.process_trade(trade_data)
            except queue.Empty:
                self.flush_batch()
                continue
            except Exception as e:
                logging.error(f"[NETWORK_KAFKA] Worker異常: {str(e)}")

    def start_workers(self, num_workers):
        self.workers = []
        for _ in range(num_workers):
            t = threading.Thread(target=self.worker_loop, daemon=True)
            t.start()
            self.workers.append(t)

def main():
    processor = NetworkKafkaProcessor()
    processor.start_workers(KAFKA_WORKER_NUM)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f'network_analyze_{NETWORK}',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    logging.info(f"[NETWORK_KAFKA] 啟動消費者，負責 network={NETWORK}")
    try:
        for message in consumer:
            while True:
                try:
                    processor.queue.put(message.value, timeout=1)
                    break
                except queue.Full:
                    processor.flush_batch()
    except KeyboardInterrupt:
        logging.info("[NETWORK_KAFKA] 收到中斷信號，退出...")
    finally:
        processor.flush_batch()
        consumer.close()

if __name__ == "__main__":
    main() 