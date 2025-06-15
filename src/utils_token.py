# import json
# import asyncio
# import os
# import redis
# import logging
# from decimal import Decimal
# from typing import List, Dict, Any
# from sqlalchemy import text
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
# from sqlalchemy.pool import AsyncAdaptedQueuePool
# from dotenv import load_dotenv

# load_dotenv()

# # 設置日誌
# logging.basicConfig(level=logging.WARNING)
# logger = logging.getLogger(__name__)

# DATABASE_Trade = os.getenv('DATABASE_Trade')
# REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
# REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
# REDIS_DB = int(os.getenv('REDIS_DB', 0))

# # 初始化 Redis 客戶端
# redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

# class TokenInfoFetcher:
#     CACHE_EXPIRE = 3600  # 緩存過期時間：1小時
#     MAX_RETRIES = 3  # 最大重試次數
#     RETRY_DELAY = 1  # 重試延遲（秒）
#     BATCH_SIZE = 20  # 每批查詢的數量
#     MAX_CONCURRENT_BATCHES = 3  # 最大並發批次數
#     # SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)  # 控制並發數
#     CONNECTION_TIMEOUT = 60  # 連接超時時間（秒）

#     @staticmethod
#     def get_semaphore():
#         return asyncio.Semaphore(TokenInfoFetcher.MAX_CONCURRENT_BATCHES)
    
#     @staticmethod
#     def get_cache_key(token_address: str, chain_id: int) -> str:
#         return f"token_info:{token_address}:{chain_id}"  # 保持原始大小寫
    
#     @staticmethod
#     def decimal_to_float(val):
#         """將 Decimal 轉換為 float"""
#         if isinstance(val, Decimal):
#             return float(val)
#         return val
    
#     @staticmethod
#     async def create_engine():
#         if not DATABASE_Trade:
#             raise ValueError("DATABASE_Trade environment variable is not set")
#         db_url = DATABASE_Trade.replace('postgresql+psycopg2', 'postgresql+asyncpg')
#         return create_async_engine(
#             db_url,
#             echo=False,
#             poolclass=AsyncAdaptedQueuePool,
#             pool_size=10,
#             max_overflow=20,
#             pool_timeout=TokenInfoFetcher.CONNECTION_TIMEOUT,
#             pool_recycle=1800,
#             connect_args={
#                 "timeout": TokenInfoFetcher.CONNECTION_TIMEOUT,
#                 "command_timeout": TokenInfoFetcher.CONNECTION_TIMEOUT
#             }
#         )
    
#     @staticmethod
#     async def batch_fetch_token_info(token_addresses: List[str], chain_id: int, session: AsyncSession) -> Dict[str, Dict[str, Any]]:
#         if not token_addresses:
#             return {}
#         try:
#             stmt = text("""
#                 SELECT address, name, logo, chain_id, supply, decimals, price_usd, fdv_usd, symbol
#                 FROM dex_query_v1.tokens
#                 WHERE address = ANY(:token_addresses) AND chain_id = :chain_id
#             """)
#             logger.debug(f"Executing batch query for addresses: {token_addresses}, chain_id: {chain_id}")
#             try:
#                 result = await session.execute(
#                     stmt,
#                     {
#                         "token_addresses": token_addresses,  # 不要 lower
#                         "chain_id": chain_id
#                     }
#                 )
#             except Exception as e:
#                 logger.error(f"Database query failed for addresses {token_addresses}: {str(e)}")
#                 logger.error(f"Query parameters: token_addresses={token_addresses}, chain_id={chain_id}")
#                 raise
#             rows = result.fetchall()
#             token_infos = {}
#             for row in rows:
#                 try:
#                     token_info = {
#                         "token_address": row.address,
#                         "token_name": row.name,
#                         "token_icon": row.logo,
#                         "chain_id": row.chain_id,
#                         "supply": TokenInfoFetcher.decimal_to_float(row.supply),
#                         "decimals": row.decimals,
#                         "price_usd": TokenInfoFetcher.decimal_to_float(row.price_usd),
#                         "marketcap": TokenInfoFetcher.decimal_to_float(row.fdv_usd),
#                         "symbol": row.symbol
#                     }
#                     token_infos[row.address] = token_info  # 保持原始大小寫
#                     # 存入緩存
#                     cache_key = TokenInfoFetcher.get_cache_key(row.address, chain_id)
#                     try:
#                         redis_client.setex(
#                             cache_key,
#                             TokenInfoFetcher.CACHE_EXPIRE,
#                             json.dumps(token_info, default=str)
#                         )
#                     except Exception as e:
#                         logger.error(f"Failed to cache token info for {row.address}: {str(e)}")
#                 except Exception as e:
#                     logger.error(f"Error processing row for address {row.address if hasattr(row, 'address') else 'unknown'}: {str(e)}")
#                     continue
#             found_addresses = set(token_infos.keys())
#             missing_addresses = set(token_addresses) - found_addresses
#             if missing_addresses:
#                 logger.debug(f"Some addresses were not found in database: {missing_addresses}")
#             return token_infos
#         except Exception as e:
#             logger.error(f"Error in batch fetch for addresses {token_addresses}: {str(e)}")
#             logger.error(f"Error type: {type(e).__name__}")
#             import traceback
#             logger.error(f"Traceback: {traceback.format_exc()}")
#             return {}
    
#     @staticmethod
#     async def get_token_info(token_address: str, chain_id: int = 9006) -> dict:
#         cache_key = TokenInfoFetcher.get_cache_key(token_address, chain_id)
#         cached_data = redis_client.get(cache_key)
#         if cached_data:
#             try:
#                 return json.loads(cached_data)
#             except json.JSONDecodeError as e:
#                 logger.warning(f"Cache data corrupted for {token_address}: {str(e)}")
#         engine = await TokenInfoFetcher.create_engine()
#         for retry in range(TokenInfoFetcher.MAX_RETRIES):
#             try:
#                 async with AsyncSession(engine) as session:
#                     result = await TokenInfoFetcher.batch_fetch_token_info([token_address], chain_id, session)
#                     return result.get(token_address, {})
#             except Exception as e:
#                 logger.error(f"Error fetching token info for {token_address} (attempt {retry + 1}/{TokenInfoFetcher.MAX_RETRIES}): {str(e)}")
#                 if retry < TokenInfoFetcher.MAX_RETRIES - 1:
#                     await asyncio.sleep(TokenInfoFetcher.RETRY_DELAY * (retry + 1))
#                 else:
#                     logger.error(f"Failed to fetch token info for {token_address} after {TokenInfoFetcher.MAX_RETRIES} attempts")
#             finally:
#                 await engine.dispose()
#         return {}

# async def batch_get_token_infos(token_addresses: List[str], chain_id: int) -> Dict[str, Dict[str, Any]]:
#     if not token_addresses:
#         return {}
#     cache_keys = [TokenInfoFetcher.get_cache_key(addr, chain_id) for addr in token_addresses]
#     cached_results = redis_client.mget(cache_keys)
#     to_fetch = []
#     results = {}
#     for addr, cached_data in zip(token_addresses, cached_results):
#         if cached_data:
#             try:
#                 results[addr] = json.loads(cached_data)
#             except json.JSONDecodeError as e:
#                 logger.warning(f"Cache data corrupted for {addr}: {str(e)}")
#                 to_fetch.append(addr)
#         else:
#             to_fetch.append(addr)
#     if to_fetch:
#         engine = await TokenInfoFetcher.create_engine()
#         try:
#             batches = [to_fetch[i:i + TokenInfoFetcher.BATCH_SIZE] for i in range(0, len(to_fetch), TokenInfoFetcher.BATCH_SIZE)]
#             async def process_batch(batch):
#                 semaphore = TokenInfoFetcher.get_semaphore()
#                 async with semaphore:
#                     async with AsyncSession(engine) as session:
#                         return await TokenInfoFetcher.batch_fetch_token_info(batch, chain_id, session)
#             batch_tasks = [process_batch(batch) for batch in batches]
#             batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
#             for batch_result in batch_results:
#                 if isinstance(batch_result, Exception):
#                     logger.error(f"Error in batch processing: {str(batch_result)}")
#                     continue
#                 results.update(batch_result)
#         except Exception as e:
#             logger.error(f"Error in batch processing: {str(e)}")
#         finally:
#             await engine.dispose()
#     return results 

# utils_token.py（簡化與效能優化後）
import json
import asyncio
import os
import redis
import logging
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_Trade = os.getenv('DATABASE_Trade')
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

_engine = None
async def get_engine():
    global _engine
    if _engine is None:
        db_url = DATABASE_Trade.replace('postgresql+psycopg2', 'postgresql+asyncpg')
        _engine = create_async_engine(
            db_url,
            echo=False,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=10,
            max_overflow=20,
            pool_timeout=60,
            pool_recycle=1800,
            connect_args={"timeout": 60, "command_timeout": 60}
        )
    return _engine

class TokenInfoFetcher:
    CACHE_EXPIRE = 3600
    BATCH_SIZE = 20
    MAX_CONCURRENT_BATCHES = 3

    @staticmethod
    def get_cache_key(token_address: str, chain_id: int) -> str:
        return f"token_info:{token_address}:{chain_id}"

    @staticmethod
    def decimal_to_float(val):
        return float(val) if isinstance(val, Decimal) else val

    @staticmethod
    async def batch_fetch_token_info(token_addresses: List[str], chain_id: int, session: AsyncSession) -> Dict[str, Dict[str, Any]]:
        stmt = text("""
            SELECT address, name, logo, chain_id, supply, decimals, price_usd, fdv_usd, symbol
            FROM dex_query_v1.tokens
            WHERE address = ANY(:token_addresses) AND chain_id = :chain_id
        """)
        result = await session.execute(stmt, {"token_addresses": token_addresses, "chain_id": chain_id})
        rows = result.fetchall()
        token_infos = {}
        for row in rows:
            info = {
                "token_address": row.address,
                "token_name": row.name,
                "token_icon": row.logo,
                "chain_id": row.chain_id,
                "supply": TokenInfoFetcher.decimal_to_float(row.supply),
                "decimals": row.decimals,
                "price_usd": TokenInfoFetcher.decimal_to_float(row.price_usd),
                "marketcap": TokenInfoFetcher.decimal_to_float(row.fdv_usd),
                "symbol": row.symbol
            }
            token_infos[row.address] = info
            redis_client.setex(TokenInfoFetcher.get_cache_key(row.address, chain_id), TokenInfoFetcher.CACHE_EXPIRE, json.dumps(info, default=str))
        return token_infos

common_tokens = {
    'So11111111111111111111111111111111111111112': {
        "symbol": "SOL",
        "name": "Solana",
        "price_usd": 164.83,
        "decimals": 9,
        "supply": 15656430,
        "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
    },
    'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': {
        "symbol": "USDC",
        "name": "USD Coin",
        "price_usd": 1,
        "decimals": 6,
        "supply": 8462371310,
        "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
    },
    'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': {
        "symbol": "USDT",
        "name": "Tether USD",
        "price_usd": 1,
        "decimals": 6,
        "supply": 2389928455,
        "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB/logo.png"
    }
}

async def get_token_info_batch_map(token_addresses: List[str], chain_id: int) -> Dict[str, Dict[str, Any]]:
    if not token_addresses:
        return {}
    results = {}
    to_fetch = []
    for addr in token_addresses:
        if addr in common_tokens:
            info = common_tokens[addr]
            results[addr] = {
                "token_address": addr,
                "token_name": info["name"],
                "token_icon": info["icon"],
                "chain_id": chain_id,
                "supply": info["supply"],
                "decimals": info["decimals"],
                "price_usd": info["price_usd"],
                "marketcap": None,
                "symbol": info["symbol"]
            }
        else:
            to_fetch.append(addr)
    if to_fetch:
        cache_keys = [TokenInfoFetcher.get_cache_key(addr, chain_id) for addr in to_fetch]
        cached_results = redis_client.mget(cache_keys)
        for addr, cache in zip(to_fetch, cached_results):
            if cache:
                try:
                    results[addr] = json.loads(cache)
                except:
                    pass
        to_fetch2 = [addr for addr in to_fetch if addr not in results]
        if to_fetch2:
            engine = await get_engine()
            batches = [to_fetch2[i:i+TokenInfoFetcher.BATCH_SIZE] for i in range(0, len(to_fetch2), TokenInfoFetcher.BATCH_SIZE)]
            semaphore = asyncio.Semaphore(TokenInfoFetcher.MAX_CONCURRENT_BATCHES)
            async def process_batch(batch):
                async with semaphore:
                    async with AsyncSession(engine) as session:
                        return await TokenInfoFetcher.batch_fetch_token_info(batch, chain_id, session)
            batch_tasks = [process_batch(batch) for batch in batches]
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            for batch_result in batch_results:
                if isinstance(batch_result, dict):
                    results.update(batch_result)
    return results
