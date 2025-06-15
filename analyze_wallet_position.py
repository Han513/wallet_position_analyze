import os
import pandas as pd
import numpy as np
import logging
import time
import signal
import traceback
import redis
import math
import json
import threading
import asyncio
from models import Transaction
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
from datetime import datetime, timezone, timedelta, date as dt_date
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map, common_tokens
from more_itertools import chunked

# 關閉 noisy 日誌
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("solana").setLevel(logging.WARNING)

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
CHECKPOINT_FILE_TEMPLATE = 'batch_checkpoint_signer_{}_{}.txt'
SHARD_COUNT = int(os.getenv('SHARD_COUNT', 5))
SHARD_INDEX = int(os.getenv('SHARD_INDEX', 0))

WSOL = 'So11111111111111111111111111111111111111112'
USDT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
STABLES = {USDT, USDC}

stop_signal_received = False

def handle_signal(signum, frame):
    global stop_signal_received
    stop_signal_received = True
    logging.warning("Stop signal received. Finishing current batch then exiting...")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# 修改数据库连接字符串，使用 psycopg2 驱动
engine_trade = create_engine(DATABASE_Trade.replace('postgresql+asyncpg', 'postgresql+psycopg2'))
engine_smartmoney = create_engine(DATABASE_SMARTMONEY.replace('postgresql+asyncpg', 'postgresql+psycopg2'))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
Session = sessionmaker(bind=engine_smartmoney)

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

def upsert_transaction(tx_data):
    session = Session()
    try:
        stmt = insert(Transaction).values(**tx_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['signature', 'wallet_address', 'token_address', 'transaction_time'],
            set_={k: v for k, v in tx_data.items() if k not in ['signature', 'wallet_address', 'token_address', 'transaction_time']}
        )
        session.execute(stmt)
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

@retry(wait=wait_random_exponential(min=3, max=10), stop=stop_after_attempt(3), retry=retry_if_exception_type(Exception))
def fetch_signers_batch(chain_id, last_signer):
    sql = f'''
    SELECT DISTINCT signer, {chain_id} AS chain_id
    FROM dex_query_v1.trades
    WHERE chain_id = {chain_id}
      AND signer > :last_signer
      AND mod(abs(hashtext(signer)), {SHARD_COUNT}) = {SHARD_INDEX}
    ORDER BY signer
    LIMIT {BATCH_SIZE}
    '''
    return pd.read_sql(text(sql), con=engine_trade, params={"last_signer": last_signer})

@retry(wait=wait_random_exponential(min=2, max=6), stop=stop_after_attempt(3), retry=retry_if_exception_type(Exception))
def fetch_trades_for_batch(batch, chain_id, table_name, engine_trade):
    logging.debug("建立 SQL 查詢字串與參數中...")
    where_clauses = []
    params = {"chain_id": int(chain_id)}
    for i, (signer, min_time) in enumerate(batch):
        where_clauses.append(f"(signer = :signer_{i} AND timestamp >= :time_{i})")
        params[f"signer_{i}"] = signer
        params[f"time_{i}"] = int(min_time)
    where_sql = " OR ".join(where_clauses)
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
        WHERE chain_id = :chain_id AND ({where_sql})
    """)
    try:
        df = pd.read_sql(trades_sql, con=engine_trade, params=params)
        logging.info(f"成功查詢 {len(df)} 筆交易資料。")
        return df
    except Exception as e:
        logging.error("查詢 SQL 發生錯誤：%s", e)
        logging.error("堆疊追蹤:\n%s", traceback.format_exc())
        raise

def to_seconds(ts):
    if ts is None or str(ts) in ['NaT', 'nan'] or (hasattr(ts, 'isnull') and ts.isnull()):
        return None
    if pd.isnull(ts) or (isinstance(ts, float) and math.isnan(ts)):
        return None
    if isinstance(ts, float) or isinstance(ts, int):
        if ts > 1e12:
            ts = int(ts / 1000)
        else:
            ts = int(ts)
        return ts
    if hasattr(ts, 'timestamp'):
        return int(ts.timestamp())
    return None

def safe_int(val):
    try:
        if val is None:
            return None
        if isinstance(val, float):
            if math.isnan(val):
                return None
        val = int(val)
        if val > 9223372036854775807 or val < -9223372036854775808:
            return None
        return val
    except:
        return None

def safe_bigint(val):
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

def calc_wallet_token_stats(group):
    # 確保 group 有必要的列
    if 'transaction_type' not in group.columns:
        group['transaction_type'] = group['side'] if 'side' in group.columns else None
    
    buys = group[group['transaction_type'].isin(['build', 'buy'])]  # 建倉或加倉
    sells = group[group['transaction_type'].isin(['clean', 'sell'])]  # 清倉或減倉
    all_trades = pd.concat([buys, sells]).sort_values('transaction_time')
    total_buy_amount = buys['dest_token_amount'].sum()
    total_sell_amount = sells['from_token_amount'].sum()
    current_position = total_buy_amount - total_sell_amount
    total_holding_seconds = 0
    position_start_time = None
    last_active_position_closed_at = None
    position = 0

    # 新增：每日 realized_profit 累積器
    realized_profit_by_date = {}
    # 移動平均成本池
    holding_amount = 0
    holding_cost = 0

    for _, trade in all_trades.iterrows():
        # 檢查 timestamp 合理性（假設合理範圍是 2000-01-01 ~ 2100-01-01）
        ts = int(trade['transaction_time'])
        if ts > 1e12:  # 如果是毫秒，轉成秒
            ts = int(ts / 1000)
        if ts < 946684800 or ts > 4102444800:  # 2000-01-01 ~ 2100-01-01
            logging.warning(f"Skip abnormal timestamp: {trade['transaction_time']} in trade: {trade}")
            continue  # 跳過異常資料
        trade_date = datetime.fromtimestamp(ts, timezone(timedelta(hours=8))).date()
        if trade['transaction_type'] in ['build', 'buy']:  # 建倉或加倉
            # 買入時增加持倉與成本
            holding_amount += trade['dest_token_amount']
            holding_cost += trade['dest_token_amount'] * trade['price']
        else:
            # 賣出時計算移動平均成本
            sell_amount = trade['from_token_amount']
            if holding_amount > 0:
                avg_cost = holding_cost / holding_amount
                profit = (trade['price'] - avg_cost) * sell_amount
                # 累加到當天 realized_profit
                realized_profit_by_date.setdefault(trade_date, 0)
                realized_profit_by_date[trade_date] += profit
                # 扣除持倉
                holding_amount -= sell_amount
                holding_cost -= avg_cost * sell_amount
                if holding_amount < 0:
                    holding_amount = 0
                    holding_cost = 0
            else:
                # 沒有持倉時，不計算 realized_profit，直接為 0
                realized_profit_by_date.setdefault(trade_date, 0)
                # 不累加利潤

    # 其餘統計維持原本
    position = 0
    for _, trade in all_trades.iterrows():
        if trade['transaction_type'] in ['build', 'buy']:  # 建倉或加倉
            if position == 0:
                position_start_time = trade['transaction_time']
            position += trade['dest_token_amount']
        else:
            position -= trade['from_token_amount']
            if position <= 0 and position_start_time is not None:
                total_holding_seconds += trade['transaction_time'] - position_start_time
                last_active_position_closed_at = trade['transaction_time']
                position_start_time = None
            if position < 0:
                position = 0

    if position > 0 and position_start_time is not None:
        total_holding_seconds += all_trades['transaction_time'].max() - position_start_time
    historical_buys = []
    historical_sells = []
    current_buy_amount = 0
    current_buy_cost = 0
    for _, trade in all_trades.iterrows():
        if trade['transaction_type'] in ['build', 'buy']:  # 建倉或加倉
            if current_buy_amount == 0:
                pass
            current_buy_amount += trade['dest_token_amount']
            current_buy_cost += trade['dest_token_amount'] * trade['price']
            historical_buys.append({
                'amount': trade['dest_token_amount'],
                'price': trade['price'],
                'timestamp': trade['transaction_time']
            })
        else:
            historical_sells.append({
                'amount': trade['from_token_amount'],
                'price': trade['price'],
                'timestamp': trade['transaction_time']
            })
    historical_total_buy_amount = sum(buy['amount'] for buy in historical_buys)
    historical_total_buy_cost = sum(buy['amount'] * buy['price'] for buy in historical_buys)
    historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0
    historical_total_sell_amount = sum(sell['amount'] for sell in historical_sells)
    historical_total_sell_value = sum(sell['amount'] * sell['price'] for sell in historical_sells)
    historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0

    # 取本日 realized_profit
    this_date = group['date'].iloc[0]
    realized_profit = realized_profit_by_date.get(this_date, 0)
    if historical_total_sell_amount > 0 and historical_avg_buy_price > 0:
        realized_profit_percentage = (realized_profit / (historical_total_sell_amount * historical_avg_buy_price)) * 100
        realized_profit_percentage = min(1000000, max(-100, realized_profit_percentage))
    else:
        realized_profit_percentage = 0

    if historical_total_buy_amount == 0:
        historical_avg_buy_price = 0
        current_avg_buy_price = 0
    else:
        current_avg_buy_price = current_buy_cost / current_buy_amount if current_buy_amount > 0 else 0

    if current_position == 0:
        current_buy_cost = 0
        current_avg_buy_price = 0

    chain_id = group['chain_id'].iloc[0]
    # 確保 chain 值始終是有效的分區值
    if chain_id == 501:
        chain = 'SOLANA'
    elif chain_id == 9006:
        chain = 'BSC'
    else:
        # 如果 chain_id 無效，跳過這筆數據
        logging.warning(f"Invalid chain_id: {chain_id}, skipping this record")
        return None

    current_time = datetime.now(timezone(timedelta(hours=8)))

    position_opened_at_sec = to_seconds(position_start_time)
    last_active_position_closed_at_sec = to_seconds(last_active_position_closed_at)
    last_transaction_time_int = int(all_trades['transaction_time'].max())
    if last_transaction_time_int > 1e12:
        last_transaction_time_int = int(last_transaction_time_int / 1000)
    return pd.Series({
        'wallet_address': group['wallet_address'].iloc[0],
        'token_address': group['token_address'].iloc[0],
        'chain_id': chain_id,
        'date': group['date'].iloc[0],
        'total_amount': current_position,
        'total_cost': current_buy_cost,
        'avg_buy_price': current_avg_buy_price,
        'position_opened_at': safe_bigint(position_opened_at_sec),
        'historical_total_buy_amount': historical_total_buy_amount,
        'historical_total_buy_cost': historical_total_buy_cost,
        'historical_total_sell_amount': historical_total_sell_amount,
        'historical_total_sell_value': historical_total_sell_value,
        'last_active_position_closed_at': safe_bigint(last_active_position_closed_at_sec),
        'historical_avg_buy_price': historical_avg_buy_price,
        'historical_avg_sell_price': historical_avg_sell_price,
        'last_transaction_time': safe_bigint(last_transaction_time_int),
        'realized_profit': realized_profit,
        'realized_profit_percentage': realized_profit_percentage,
        'total_buy_count': len(historical_buys),
        'total_sell_count': len(historical_sells),
        'total_holding_seconds': safe_bigint(total_holding_seconds),
        'chain': chain,
        'updated_at': current_time
    })

def process_kafka_trade(trade_data, engine_smartmoney):
    try:
        event = trade_data['event']
        wallet_address = event['address']
        token_address = event['tokenAddress']
        network = event['network']
        # 根據 network 設置 chain_id
        if network == 'SOLANA':
            chain_id = 501
            chain = 'SOLANA'
        elif network == 'BSC':
            chain_id = 9006
            chain = 'BSC'
        else:
            logging.warning(f"Invalid network: {network}, skipping this record")
            return

        timestamp = event['time']
        side = event['side']
        amount = float(event['txnValue'])
        price = float(event['price'])
        price_nav = float(event['priceNav'])
        baseMint = event.get('baseMint', None)
        quoteMint = event.get('quoteMint', None)
        fromTokenAmount = event.get('fromTokenAmount', None)
        toTokenAmount = event.get('toTokenAmount', None)
        signature = event.get('signature', f'{wallet_address}_{token_address}_{timestamp}')

        ts = int(timestamp)
        if ts > 1e12:
            ts = int(ts / 1000)
        dt = datetime.fromtimestamp(ts, timezone(timedelta(hours=8)))
        date_val = dt.date()

        # 獲取 token 信息
        token_info = asyncio.run(get_token_info_batch_map([token_address], chain_id)).get(token_address, {})
        
        # 獲取 from_token 和 dest_token 信息
        from_token_info = {}
        dest_token_info = {}
        if baseMint:
            from_token_info = asyncio.run(get_token_info_batch_map([baseMint], chain_id)).get(baseMint, {})
        if quoteMint:
            dest_token_info = asyncio.run(get_token_info_batch_map([quoteMint], chain_id)).get(quoteMint, {})

        # 1. 查詢該錢包+幣種的所有歷史交易，計算賣出時的 realized_profit
        session = Session()
        try:
            txs = session.query(Transaction).filter(
                Transaction.wallet_address == wallet_address,
                Transaction.token_address == token_address,
                Transaction.chain == chain,
                Transaction.transaction_time < ts
            ).order_by(Transaction.transaction_time).all()
            holding_amount = 0
            holding_cost = 0
            for tx in txs:
                if tx.transaction_type == 'buy':
                    holding_amount += tx.amount
                    holding_cost += tx.amount * tx.price
                elif tx.transaction_type == 'sell':
                    avg_cost = (holding_cost / holding_amount) if holding_amount > 0 else 0
                    holding_amount -= tx.amount
                    holding_cost -= avg_cost * tx.amount
                    if holding_amount < 0:
                        holding_amount = 0
                        holding_cost = 0
            realized_profit = None
            realized_profit_percentage = None
            if side == 'sell':
                avg_cost = (holding_cost / holding_amount) if holding_amount > 0 else 0
                realized_profit = (price - avg_cost) * amount if avg_cost > 0 else 0
                realized_profit_percentage = ((price / avg_cost - 1) * 100) if avg_cost > 0 else 0
                holding_amount -= amount
                holding_cost -= avg_cost * amount
                if holding_amount < 0:
                    holding_amount = 0
                    holding_cost = 0
            elif side == 'buy':
                holding_amount += amount
                holding_cost += amount * price

            wallet_balance = asyncio.run(get_sol_balance(wallet_address))
            
            # 計算 marketcap
            try:
                marketcap = price * float(token_info.get('supply', 0))
            except Exception:
                marketcap = 0

            tx_data = {
                'wallet_address': wallet_address,
                'wallet_balance': wallet_balance['balance']['int'],
                'token_address': token_address,
                'token_icon': token_info.get('token_icon'),
                'token_name': truncate_string(token_info.get('token_name'), 255),
                'price': price,
                'amount': amount,
                'marketcap': marketcap,
                'value': price * amount,
                'holding_percentage': None,
                'chain': chain,
                'chain_id': chain_id,
                'realized_profit': realized_profit,
                'realized_profit_percentage': realized_profit_percentage,
                'transaction_type': side,
                'transaction_time': ts,
                'time': datetime.now(timezone(timedelta(hours=8))),
                'signature': signature,
                'from_token_address': baseMint,
                'from_token_symbol': truncate_string(from_token_info.get('symbol'), 100),
                'from_token_amount': fromTokenAmount,
                'dest_token_address': quoteMint,
                'dest_token_symbol': truncate_string(dest_token_info.get('symbol'), 100),
                'dest_token_amount': toTokenAmount,
            }
            upsert_transaction(tx_data)
            # 2. 重新從 Transaction 表撈出該錢包、該幣、該天的所有交易 groupby 統計 wallet_buy_data
            start_of_day = datetime(dt.year, dt.month, dt.day, tzinfo=timezone(timedelta(hours=8)))
            end_of_day = start_of_day + timedelta(days=1)
            day_txs = session.query(Transaction).filter(
                Transaction.wallet_address == wallet_address,
                Transaction.token_address == token_address,
                Transaction.chain == chain,
                Transaction.transaction_time >= int(start_of_day.timestamp()),
                Transaction.transaction_time < int(end_of_day.timestamp())
            ).all()
            if day_txs:
                tx_df = pd.DataFrame([tx.__dict__ for tx in day_txs])
                tx_df['date'] = tx_df['transaction_time'].apply(lambda ts: datetime.fromtimestamp(int(ts), timezone(timedelta(hours=8))).date())
                tx_df['chain_id'] = chain_id  # 確保 chain_id 被正確設置
                grouped = tx_df.groupby(['wallet_address', 'token_address', 'chain', 'date'])
                for _, group in grouped:
                    stats = calc_wallet_token_stats(group)
                    if stats is None:
                        logging.warning(f"Skipping invalid stats for wallet {wallet_address}, token {token_address}")
                        continue
                    row_dict = stats.to_dict()
                    # 修正 total_amount 為負或極小時直接設為 0
                    if row_dict.get('total_amount', 0) < 1e-10:
                        row_dict['total_amount'] = 0
                    redis_key = f"wallet_cache:{row_dict['wallet_address']}:{row_dict['token_address']}:{row_dict['chain_id']}"
                    redis_client.set(redis_key, str(row_dict['total_amount']), ex=86400)
                    bulk_upsert_wallet_buy_data([row_dict], engine_smartmoney)
        finally:
            session.close()
    except Exception as e:
        logging.error(f"Error processing trade: {str(e)}")
        logging.error(traceback.format_exc())
        raise

def bulk_upsert_wallet_buy_data(rows, engine_smartmoney):
    if not rows:
        return
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['wallet_address'], r['token_address'], r['chain'], r['date'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    batch_size = 100
    total_rows = len(deduped_rows)
    
    for i in range(0, total_rows, batch_size):
        batch = deduped_rows[i:i + batch_size]
        try:
            with engine_smartmoney.begin() as conn:
                # 使用 VALUES 子句進行批量插入
                values_clause = []
                params = {}
                for idx, row in enumerate(batch):
                    placeholders = []
                    for key, value in row.items():
                        param_name = f"{key}_{idx}"
                        params[param_name] = value
                        placeholders.append(f":{param_name}")
                    values_clause.append(f"({', '.join(placeholders)})")
                
                columns = list(batch[0].keys())
                columns_str = ', '.join(columns)
                values_str = ', '.join(values_clause)
                
                # 構建更新語句
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
        except Exception as e:
            logging.error(f"Error in bulk_upsert_wallet_buy_data: {str(e)}")
            raise

def upsert_wallet_buy_data(row_dict, engine_smartmoney):
    """
    單條記錄的更新函數，用於兼容舊代碼
    建議使用 bulk_upsert_wallet_buy_data 進行批量處理
    """
    try:
        with engine_smartmoney.begin() as conn:
            upsert_sql = text('''
                INSERT INTO dex_query_v1.wallet_buy_data (
                    wallet_address, token_address, chain_id, date, total_amount, total_cost, 
                    avg_buy_price, position_opened_at, historical_total_buy_amount, 
                    historical_total_buy_cost, historical_total_sell_amount, 
                    historical_total_sell_value, last_active_position_closed_at, 
                    historical_avg_buy_price, historical_avg_sell_price, last_transaction_time,
                    realized_profit, realized_profit_percentage, total_buy_count, 
                    total_sell_count, total_holding_seconds, chain, updated_at
                ) VALUES (
                    :wallet_address, :token_address, :chain_id, :date, :total_amount, :total_cost, 
                    :avg_buy_price, :position_opened_at, :historical_total_buy_amount, 
                    :historical_total_buy_cost, :historical_total_sell_amount, 
                    :historical_total_sell_value, :last_active_position_closed_at, 
                    :historical_avg_buy_price, :historical_avg_sell_price, :last_transaction_time,
                    :realized_profit, :realized_profit_percentage, :total_buy_count, 
                    :total_sell_count, :total_holding_seconds, :chain, :updated_at
                )
                ON CONFLICT (wallet_address, token_address, chain, date) DO UPDATE SET
                    total_amount = EXCLUDED.total_amount,
                    total_cost = EXCLUDED.total_cost,
                    avg_buy_price = EXCLUDED.avg_buy_price,
                    position_opened_at = EXCLUDED.position_opened_at,
                    historical_total_buy_amount = EXCLUDED.historical_total_buy_amount,
                    historical_total_buy_cost = EXCLUDED.historical_total_buy_cost,
                    historical_total_sell_amount = EXCLUDED.historical_total_sell_amount,
                    historical_total_sell_value = EXCLUDED.historical_total_sell_value,
                    last_active_position_closed_at = EXCLUDED.last_active_position_closed_at,
                    historical_avg_buy_price = EXCLUDED.historical_avg_buy_price,
                    historical_avg_sell_price = EXCLUDED.historical_avg_sell_price,
                    last_transaction_time = EXCLUDED.last_transaction_time,
                    realized_profit = EXCLUDED.realized_profit,
                    realized_profit_percentage = EXCLUDED.realized_profit_percentage,
                    total_buy_count = EXCLUDED.total_buy_count,
                    total_sell_count = EXCLUDED.total_sell_count,
                    total_holding_seconds = EXCLUDED.total_holding_seconds,
                    chain = EXCLUDED.chain,
                    updated_at = EXCLUDED.updated_at
            ''')
            conn.execute(upsert_sql, row_dict)
    except Exception as e:
        logging.error(f"Error in upsert_wallet_buy_data: {str(e)}")
        raise

def bulk_insert_transactions(rows, engine):
    if not rows:
        return
    
    # 添加日誌記錄
    logging.info(f"開始批量插入，原始數據量: {len(rows)}")
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['signature'], r['wallet_address'], r['token_address'], r['transaction_time'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    logging.info(f"去重後數據量: {len(deduped_rows)}")
    
    batch_size = 100
    total_rows = len(deduped_rows)
    max_retries = 3
    retry_delay = 1
    
    successful_inserts = 0
    
    for i in range(0, total_rows, batch_size):
        batch = deduped_rows[i:i + batch_size]
        retry_count = 0
        while retry_count < max_retries:
            try:
                # 預處理數據
                for r in batch:
                    if 'token_name' in r:
                        r['token_name'] = truncate_string(r['token_name'], 255)
                    if 'from_token_symbol' in r:
                        r['from_token_symbol'] = truncate_string(r['from_token_symbol'], 100)
                    if 'dest_token_symbol' in r:
                        r['dest_token_symbol'] = truncate_string(r['dest_token_symbol'], 100)
                
                with engine.begin() as conn:
                    # 使用 VALUES 子句進行批量插入
                    values_clause = []
                    params = {}
                    for idx, row in enumerate(batch):
                        placeholders = []
                        for key, value in row.items():
                            param_name = f"{key}_{idx}"
                            params[param_name] = value
                            placeholders.append(f":{param_name}")
                        values_clause.append(f"({', '.join(placeholders)})")
                    
                    columns = list(batch[0].keys())
                    columns_str = ', '.join(columns)
                    values_str = ', '.join(values_clause)
                    
                    # 構建更新語句
                    update_clause = ', '.join(
                        f"{col} = EXCLUDED.{col}"
                        for col in columns
                        if col not in ['signature', 'wallet_address', 'token_address', 'transaction_time']
                    )
                    
                    upsert_sql = text(f'''
                        INSERT INTO dex_query_v1.wallet_transaction ({columns_str})
                        VALUES {values_str}
                        ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                        DO UPDATE SET {update_clause}
                    ''')
                    
                    conn.execute(upsert_sql, params)
                    successful_inserts += len(batch)
                break
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    logging.error(f"Error inserting batch {i//batch_size + 1} after {max_retries} retries: {str(e)}")
                    raise
                logging.warning(f"Retry {retry_count}/{max_retries} for batch {i//batch_size + 1}: {str(e)}")
                time.sleep(retry_delay * retry_count)
    
    logging.info(f"成功插入/更新記錄數: {successful_inserts}")

def kafka_consumer_loop():
    try:
        consumer = KafkaConsumer(
            'web3_trade_events',
            bootstrap_servers=KAFKA_BROKER,
            group_id='wallet_position_analyze',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        while True:
            try:
                for message in consumer:
                    try:
                        trade_data = message.value
                        process_kafka_trade(trade_data, engine_smartmoney)
                    except Exception as e:
                        logging.error(f"Kafka consumer error: {str(e)}")
                        logging.error(traceback.format_exc())
                        continue
            except Exception as e:
                logging.error(f"Kafka consumer loop error: {str(e)}")
                logging.error(traceback.format_exc())
                time.sleep(5)  # 等待5秒後重試
                continue
    except Exception as e:
        logging.error(f"Fatal Kafka consumer error: {str(e)}")
        logging.error(traceback.format_exc())
        raise

def get_chain_name(chain_id):
    if str(chain_id) == "501":
        return "SOLANA"
    elif str(chain_id) == "9006":
        return "BSC"
    else:
        return str(chain_id)

def determine_transaction_type(side, amount, current_holding):
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

def truncate_string(value, limit):
    if isinstance(value, str):
        return value[:limit]
    return value

kafka_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
kafka_thread.start()

for chain_id in CHAIN_IDS:
    checkpoint_file = CHECKPOINT_FILE_TEMPLATE.format(chain_id, SHARD_INDEX)
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            last_signer = f.read().strip()
    else:
        last_signer = ''
    while not stop_signal_received:
        signers_df = fetch_signers_batch(chain_id, last_signer)
        if signers_df.empty:
            logging.info(f"chain_id={chain_id} signer 掃描完畢。")
            break
        # 防呆：如果 checkpoint 沒推進，直接 break
        if signers_df['signer'].max() <= last_signer:
            logging.warning(f'Checkpoint未推進，last_signer={last_signer}，max_signer={signers_df["signer"].max()}，跳出避免死循環')
            break
        logging.info(f"處理 signer: {signers_df['signer'].min()} ~ {signers_df['signer'].max()}，共 {len(signers_df)} 筆")
        try:
            trades = fetch_trades_for_batch(batch=signers_df.itertuples(index=False, name=None), chain_id=chain_id, table_name='dex_query_v1.trades', engine_trade=engine_trade)
        except RetryError as re:
            logging.error("fetch_trades_for_batch 最終失敗：%s", re)
            continue
        if trades.empty:
            last_signer = signers_df['signer'].max()
            with open(checkpoint_file, 'w') as f:
                f.write(last_signer)
            continue
        def normalize_trade(row):
            tx_hash = row['tx_hash']
            token_in = row['token_in']
            token_out = row['token_out']
            decimals_in = row.get('decimals_in', 0) or 0
            decimals_out = row.get('decimals_out', 0) or 0
            side = row['side']
            price = row['price_usd'] if 'price_usd' in row else row['price']
            signer = row['signer']
            chain_id = row['chain_id']
            timestamp = row['timestamp']
            timestamp = row['timestamp']

            in_is_stable_or_wsol = token_in in STABLES or token_in == WSOL
            out_is_stable_or_wsol = token_out in STABLES or token_out == WSOL
            in_is_non_stable = not in_is_stable_or_wsol
            out_is_non_stable = not out_is_stable_or_wsol

            if (in_is_stable_or_wsol and out_is_non_stable):
                token_address = token_out
                direction = 'buy'
                amount_in = row['amount_in'] / (10 ** decimals_in)
                amount_out = row['amount_out'] / (10 ** decimals_out)
                amount = amount_out
            elif (out_is_stable_or_wsol and in_is_non_stable):
                token_address = token_in
                direction = 'sell'
                amount_in = row['amount_in'] / (10 ** decimals_in)
                amount_out = row['amount_out'] / (10 ** decimals_out)
                amount = amount_in
            elif in_is_non_stable and out_is_non_stable:
                if side == 'buy':
                    token_address = token_out
                    direction = 'buy'
                    amount_in = row['amount_in'] / (10 ** decimals_in)
                    amount_out = row['amount_out'] / (10 ** decimals_out)
                    amount = amount_out
                else:
                    token_address = token_in
                    direction = 'sell'
                    amount_in = row['amount_in'] / (10 ** decimals_in)
                    amount_out = row['amount_out'] / (10 ** decimals_out)
                    amount = amount_in
            else:
                token_address = token_out
                direction = 'buy' if side == 'buy' else 'sell'
                amount_in = row['amount_in'] / (10 ** decimals_in)
                amount_out = row['amount_out'] / (10 ** decimals_out)
                amount = amount_out if direction == 'buy' else amount_in

            return {
                'tx_hash': tx_hash,
                'signer': signer,
                'token_address': token_address,
                'side': direction,
                'token_in': token_in,
                'token_out': token_out,
                'amount_in': amount_in,
                'amount_out': amount_out,
                'amount': amount,
                'price': price,
                'timestamp': timestamp,
                'chain_id': chain_id,
                'timestamp': timestamp
            }
        trades = trades.sort_values('timestamp')
        trades = trades.apply(normalize_trade, axis=1, result_type='expand')
        trades = trades[trades['token_address'] != WSOL]
        # 新增 date 欄位（UTC+8 當天）
        def extract_date(row):
            ts = int(row['timestamp'])
            if ts > 1e12:
                ts = int(ts / 1000)
            dt = datetime.fromtimestamp(ts, timezone(timedelta(hours=8)))
            return dt.date()
        trades['date'] = trades.apply(extract_date, axis=1)
        # 1. 收集本批所有 signer、token_address
        signers = trades['signer'].unique().tolist()
        token_addresses = trades['token_address'].unique().tolist()
        # 2. 批次查詢 SOL 餘額與 token 資訊
        sol_balance_dict = {}
        if int(chain_id) == 501:
            async def batch_get_sol_balances(signers):
                tasks = [get_sol_balance(signer) for signer in signers]
                results = await asyncio.gather(*tasks)
                return dict(zip(signers, [r['balance']['int'] for r in results]))
            sol_balance_dict = asyncio.run(batch_get_sol_balances(signers))
        token_info_dict = {}
        transaction_rows = []
        for chunk in chunked(token_addresses, 50):
            chunk_result = loop.run_until_complete(get_token_info_batch_map(chunk, chain_id))
            token_info_dict.update(chunk_result)
        # 1. 以錢包+幣種為單位，緩存池計算每筆交易的realized_profit
        for (signer, token_address, chain_id), group in trades.groupby(['signer', 'token_address', 'chain_id']):
            group = group.sort_values('timestamp')
            holding_amount = 0
            holding_cost = 0
            original_holding_amount = 0  # for sell holding_percentage
            for trade_row in group.itertuples(index=False):
                # 判斷交易型態
                transaction_type = determine_transaction_type(
                    trade_row.side, 
                    trade_row.amount, 
                    holding_amount
                )
                
                realized_profit = 0
                realized_profit_percentage = 0
                if trade_row.side == 'sell':
                    sell_amount = trade_row.amount
                    avg_cost = (holding_cost / holding_amount) if holding_amount > 0 else 0
                    realized_profit = (trade_row.price - avg_cost) * sell_amount if avg_cost > 0 else 0
                    realized_profit_percentage = ((trade_row.price / avg_cost - 1) * 100) if avg_cost > 0 else 0
                    original_holding_amount = holding_amount
                    holding_amount -= sell_amount
                    holding_cost -= avg_cost * sell_amount
                    if holding_amount < 0:
                        holding_amount = 0
                        holding_cost = 0
                elif trade_row.side == 'buy':
                    holding_amount += trade_row.amount
                    holding_cost += trade_row.amount * trade_row.price
                ts = int(trade_row.timestamp)
                if ts > 1e12:
                    ts = int(ts / 1000)
                wallet_balance = sol_balance_dict.get(trade_row.signer, 0) if int(chain_id) == 501 else 0
                token_info = token_info_dict.get(trade_row.token_address, {})
                price = trade_row.price
                supply = token_info.get('supply') or 0
                try:
                    marketcap = price * float(supply)
                except Exception:
                    marketcap = 0
                value = price * trade_row.amount
                # holding_percentage
                holding_percentage = None
                if trade_row.side == 'buy':
                    if wallet_balance > 0:
                        holding_percentage = min(100, (value / wallet_balance) * 100)
                    else:
                        holding_percentage = 0
                elif trade_row.side == 'sell':
                    if original_holding_amount > 0:
                        holding_percentage = min(100, (trade_row.amount / original_holding_amount) * 100)
                    else:
                        holding_percentage = 0
                # from_token_symbol, dest_token_symbol
                def get_symbol(addr):
                    info = token_info_dict.get(addr)
                    if info and info.get("symbol"):
                        return truncate_string(info["symbol"], 100)
                    return None
                def get_icon(addr):
                    info = token_info_dict.get(addr)
                    if info and info.get("token_icon"):
                        return info["token_icon"]
                    return None
                from_token_symbol = get_symbol(trade_row.token_in)
                dest_token_symbol = get_symbol(trade_row.token_out)
                token_icon = get_icon(trade_row.token_address)
                tx_data = {
                    'wallet_address': trade_row.signer,
                    'wallet_balance': wallet_balance,
                    'token_address': trade_row.token_address,
                    'token_icon': token_icon,
                    'token_name': truncate_string(token_info.get('symbol', None), 255),
                    'price': price,
                    'amount': trade_row.amount,
                    'marketcap': marketcap,
                    'value': value,
                    'holding_percentage': holding_percentage,
                    'chain': get_chain_name(trade_row.chain_id),
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

        bulk_insert_transactions(transaction_rows, engine_smartmoney)
        
        # 2. 從Transaction表groupby統計wallet_buy_data（僅統計30天內的交易）
        session = Session()
        try:
            wallet_addresses = trades['signer'].unique().tolist()
            token_addresses = trades['token_address'].unique().tolist()
            chain_ids = [int(cid) for cid in trades['chain_id'].unique().tolist()]
            now_ts = int(datetime.now(timezone(timedelta(hours=8))).timestamp())
            thirty_days_ago_ts = now_ts - 30 * 24 * 60 * 60
            txs = session.query(Transaction).filter(
                Transaction.wallet_address.in_(wallet_addresses),
                Transaction.chain_id.in_(chain_ids),
                Transaction.transaction_time >= thirty_days_ago_ts,
                Transaction.transaction_time <= now_ts
            ).all()
            txs = [tx for tx in txs if tx is not None]
            if txs:
                tx_df = pd.DataFrame([tx.__dict__ for tx in txs if tx is not None])
                logging.info(f"從Transaction表讀取到 {len(tx_df)} 筆交易記錄")
                if 'transaction_type' in tx_df.columns:
                    tx_df['side'] = tx_df['transaction_type']
                tx_df['date'] = tx_df['transaction_time'].apply(lambda ts: datetime.fromtimestamp(int(ts), timezone(timedelta(hours=8))).date())
                grouped = tx_df.groupby(['wallet_address', 'token_address', 'chain', 'date'])
                logging.info(f"Group by 後的群組數量: {len(grouped)}")
                group_count = 0
                success_count = 0
                for _, group in grouped:
                    group_count += 1
                    try:
                        stats = calc_wallet_token_stats(group)
                        if stats is not None:
                            row_dict = stats.to_dict()
                            # 修正 total_amount 為負或極小時直接設為 0
                            if row_dict.get('total_amount', 0) < 1e-10:
                                row_dict['total_amount'] = 0
                            redis_key = f"wallet_cache:{row_dict['wallet_address']}:{row_dict['token_address']}:{row_dict['chain_id']}"
                            redis_client.set(redis_key, str(row_dict['total_amount']), ex=86400)
                            bulk_upsert_wallet_buy_data([row_dict], engine_smartmoney)
                            success_count += 1
                            if group_count % 100 == 0:
                                logging.info(f"已處理 {group_count} 個群組，成功寫入 {success_count} 筆")
                    except Exception as e:
                        logging.error(f"處理群組時發生錯誤: {str(e)}")
                        logging.error(f"群組資料: wallet_address={group['wallet_address'].iloc[0]}, token_address={group['token_address'].iloc[0]}, chain={group['chain'].iloc[0]}, date={group['date'].iloc[0]}")
                        continue
                logging.info(f"Group by 處理完成，總共處理 {group_count} 個群組，成功寫入 {success_count} 筆")
            else:
                logging.info("沒有找到需要處理的交易記錄")
        finally:
            session.close()
        last_signer = signers_df['signer'].max()
        with open(checkpoint_file, 'w') as f:
            f.write(last_signer)
        logging.info(f"處理完成 signer <= {last_signer}\n")
logging.info("所有 signer 分析任務結束。")
