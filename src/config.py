import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_Trade = os.getenv('DATABASE_Trade')
DATABASE_SMARTMONEY = os.getenv('DATABASE_SMARTMONEY')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'web3_trade_events')
CHAIN_IDS = os.getenv('CHAIN_IDS', '501,9006').split(',')
BATCH_SIZE = 200
SHARD_COUNT = int(os.getenv('SHARD_COUNT', 5))
SHARD_INDEX = int(os.getenv('SHARD_INDEX', 0))

engine_trade = create_engine(DATABASE_Trade.replace('postgresql+asyncpg', 'postgresql+psycopg2'))
engine_smartmoney = create_engine(DATABASE_SMARTMONEY.replace('postgresql+asyncpg', 'postgresql+psycopg2'))