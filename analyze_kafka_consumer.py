#!/usr/bin/env python3
"""
Kafka 處理後數據消費者
用於處理處理後的交易數據並寫入數據庫
"""

import os
import logging
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from datetime import datetime, timezone, timedelta
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='[KAFKA_CONSUMER] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

load_dotenv()
kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
db_url = os.getenv('DATABASE_SMARTMONEY', 'postgresql+asyncpg://postgres:henrywork8812601@localhost:5432/dex_db')

class AsyncKafkaProcessedConsumer:
    def __init__(self):
        self.stats = {
            'processed_count': 0,
            'error_count': 0,
            'start_time': datetime.now().timestamp()
        }
        self.engine = create_async_engine(
            db_url,
            pool_size=20,
            max_overflow=40,
            pool_pre_ping=True,
            pool_recycle=1800
        )
        self.grouped_data = {}  # key: (chain, wallet, token), value: list of items
        self.lock = asyncio.Lock()
        self.flush_interval = 5  # 每5秒聚合一次

    async def add_and_aggregate(self, item):
        key = (item['chain'], item['wallet_address'], item['token_address'])
        async with self.lock:
            if key not in self.grouped_data:
                self.grouped_data[key] = []
            self.grouped_data[key].append(item)

    async def periodic_aggregate_and_write(self):
        while True:
            await asyncio.sleep(self.flush_interval)
            await self.aggregate_and_write()

    async def aggregate_and_write(self):
        async with self.lock:
            if not self.grouped_data:
                return
            # 聚合每個分組
            async with AsyncSession(self.engine) as session:
                for key, items in self.grouped_data.items():
                    stats = self._calculate_stats(items)
                    chain, wallet, token = key
                    date = datetime.fromtimestamp(items[-1]['timestamp'], timezone(timedelta(hours=8))).date()
                    upsert_sql = text('''
                        INSERT INTO dex_query_v1.wallet_buy_data 
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
                    ''')
                    params = {
                        'wallet_address': wallet,
                        'token_address': token,
                        'chain_id': 501 if chain == 'SOLANA' else 9006,
                        'chain': chain,
                        'date': date,
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
                        'total_buy_count': stats['total_buy_count'],
                        'total_sell_count': stats['total_sell_count'],
                        'total_holding_seconds': stats['total_holding_seconds'],
                        'last_transaction_time': stats['last_transaction_time'],
                        'realized_profit': stats['realized_profit'],
                        'realized_profit_percentage': stats['realized_profit_percentage'],
                        'updated_at': datetime.now(timezone(timedelta(hours=8)))
                    }
                    await session.execute(upsert_sql, params)
                await session.commit()
                self.stats['processed_count'] += len(self.grouped_data)
            self.grouped_data.clear()

    def _calculate_stats(self, items):
        stats = {
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
        for item in items:
            if item['side'] in ['build', 'buy']:
                stats['total_amount'] += item['amount']
                stats['total_cost'] += item['amount'] * item['price']
                stats['historical_total_buy_amount'] += item['amount']
                stats['historical_total_buy_cost'] += item['amount'] * item['price']
                stats['total_buy_count'] += 1
                if stats['total_amount'] <= item['amount']:
                    stats['position_opened_at'] = item['timestamp']
            else:
                stats['total_amount'] -= item['amount']
                stats['historical_total_sell_amount'] += item['amount']
                stats['historical_total_sell_value'] += item['amount'] * item['price']
                stats['total_sell_count'] += 1
            stats['last_transaction_time'] = item['timestamp']
            stats['realized_profit'] += item.get('realized_profit', 0)
            stats['realized_profit_percentage'] += item.get('realized_profit_percentage', 0)
        if stats['total_amount'] > 0:
            stats['avg_buy_price'] = stats['total_cost'] / stats['total_amount']
        if stats['historical_total_buy_amount'] > 0:
            stats['historical_avg_buy_price'] = stats['historical_total_buy_cost'] / stats['historical_total_buy_amount']
        if stats['historical_total_sell_amount'] > 0:
            stats['historical_avg_sell_price'] = stats['historical_total_sell_value'] / stats['historical_total_sell_amount']
        return stats

async def async_kafka_processed_consumer():
    consumer = AsyncKafkaProcessedConsumer()
    kafka_consumer = AIOKafkaConsumer(
        'processed_trade_events',
        bootstrap_servers=kafka_broker,
        group_id='wallet_position_consumer',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    await kafka_consumer.start()
    # 啟動聚合寫入的背景任務
    asyncio.create_task(consumer.periodic_aggregate_and_write())
    try:
        logging.info("異步 Kafka 處理後數據消費者已啟動")
        async for msg in kafka_consumer:
            await consumer.add_and_aggregate(msg.value)
    except Exception as e:
        logging.error(f"Kafka 消費者致命錯誤: {str(e)}")
        raise
    finally:
        await kafka_consumer.stop()
        await consumer.engine.dispose()

def main():
    asyncio.run(async_kafka_processed_consumer())

if __name__ == '__main__':
    main() 