#!/usr/bin/env python3
"""
Kafka 交易數據處理器
用於處理原始交易數據並推送到新的 topic
"""
import os
import logging
import json
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from datetime import datetime, timezone, timedelta
import psutil
from dotenv import load_dotenv
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='[KAFKA_PROCESSOR] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

load_dotenv()
kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:9092')
kafka_topic = os.getenv('KAFKA_TOPIC', 'web3_trade_events')

# 每次啟動都產生唯一 group_id
kafka_group_id = f"wallet_position_processor_{uuid.uuid4()}"

class AsyncKafkaTradeProcessor:
    """Kafka 交易數據處理器"""
    
    def __init__(self):
        # 性能調優參數
        self.max_workers = min(8, psutil.cpu_count())  # 動態工作線程數
        self.batch_size = 1000  # 批次大小
        self.max_batch_wait = 1  # 最大等待時間（秒）
        
        # 內存緩存
        self.position_cache = {}
        self.cache_lock = asyncio.Lock()
        
        # 統計信息
        self.stats = {
            'processed_count': 0,
            'error_count': 0,
            'start_time': datetime.now().timestamp()
        }
    
    async def process_trade(self, trade_data):
        """處理單筆交易"""
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
            
            # 計算已實現收益
            realized_profit, realized_profit_percentage = await self._calculate_profit(
                wallet_address, token_address, chain, side, amount, price, timestamp
            )
            
            # 構建處理後的數據
            processed_data = {
                'wallet_address': wallet_address,
                'token_address': token_address,
                'chain': chain,
                'chain_id': chain_id,
                'side': side,
                'amount': amount,
                'price': price,
                'timestamp': timestamp,
                'signature': signature,
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'processed_at': int(datetime.now().timestamp())
            }
            
            return processed_data
            
        except Exception as e:
            self.stats['error_count'] += 1
            logging.error(f"處理交易失敗: {str(e)}")
            return None
    
    async def _calculate_profit(self, wallet_address, token_address, chain, side, amount, price, timestamp):
        """計算已實現收益"""
        key = (wallet_address, token_address, chain)
        
        async with self.cache_lock:
            if key not in self.position_cache:
                self.position_cache[key] = {
                    'holding_amount': 0.0,
                    'holding_cost': 0.0,
                    'last_update': timestamp
                }
            
            cache_data = self.position_cache[key]
            
            realized_profit = 0.0
            realized_profit_percentage = 0.0
            
            if side in ['build', 'buy']:  # 建倉或加倉
                cache_data['holding_amount'] += amount
                cache_data['holding_cost'] += amount * price
            elif side in ['clean', 'sell']:  # 清倉或減倉
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
            
            cache_data['last_update'] = timestamp
            
            return realized_profit, realized_profit_percentage
    
    def add_to_queue(self, processed_data):
        """添加處理後的數據到隊列"""
        if not processed_data:
            return
            
        try:
            self.process_queue.put(processed_data, timeout=1)
            self.stats['processed_count'] += 1
        except Exception as e:
            logging.error(f"添加到隊列失敗: {str(e)}")
    
    def shutdown(self):
        """優雅關閉"""
        logging.info("開始關閉處理器...")
        self.running = False
        self.executor.shutdown(wait=True)
        logging.info("處理器已關閉")

async def async_kafka_trade_processor():
    """Kafka 交易數據處理器主函數"""
    processor = AsyncKafkaTradeProcessor()
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_broker,
        group_id=kafka_group_id,  # 使用唯一 group_id
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    await producer.start()
    await consumer.start()
    try:
        logging.info(f"異步 Kafka 交易數據處理器已啟動，group_id={kafka_group_id}")
        async for msg in consumer:
            processed_data = await processor.process_trade(msg.value)
            if processed_data:
                await producer.send_and_wait('processed_trade_events', processed_data)
                processor.stats['processed_count'] += 1
    except Exception as e:
        logging.error(f"Kafka 處理器致命錯誤: {str(e)}")
        raise
    finally:
        await producer.stop()
        await consumer.stop()

def main():
    asyncio.run(async_kafka_trade_processor())

if __name__ == '__main__':
    main() 