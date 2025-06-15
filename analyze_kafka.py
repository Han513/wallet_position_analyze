import logging
import traceback
import json
import math
import pandas as pd
import asyncio
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone, timedelta
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
import time

from config import engine_smartmoney, KAFKA_BROKER, KAFKA_TOPIC
from models import Transaction
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map

logging.basicConfig(
    level=logging.INFO,
    format='[KAFKA] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("solana").setLevel(logging.WARNING)

# 在 config.py 沒有 Session，需自行建立 Session
Session = sessionmaker(bind=engine_smartmoney)

def truncate_string(value, limit):
    if isinstance(value, str):
        return value[:limit]
    return value

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

def bulk_upsert_wallet_buy_data(rows, engine_smartmoney):
    if not rows:
        logging.info("bulk_upsert_wallet_buy_data: 無資料可寫入")
        return
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['wallet_address'], r['token_address'], r['chain'], r['date'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    batch_size = 100
    total_rows = len(deduped_rows)
    logging.info(f"bulk_upsert_wallet_buy_data: 準備寫入 {total_rows} 筆資料 (批次大小: {batch_size})")
    # 顯示主鍵列表
    for row in deduped_rows:
        logging.debug(f"wallet_buy_data主鍵: {row['wallet_address']}, {row['token_address']}, {row['chain']}, {row['date']}")
    
    for i in range(0, total_rows, batch_size):
        batch = deduped_rows[i:i + batch_size]
        try:
            with engine_smartmoney.begin() as conn:
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
            logging.info(f"bulk_upsert_wallet_buy_data: 成功寫入 {len(batch)} 筆 (第 {i//batch_size+1} 批)")
        except Exception as e:
            logging.error(f"bulk_upsert_wallet_buy_data: 批次 {i//batch_size+1} 寫入失敗: {str(e)}")
            raise

def calc_wallet_token_stats(group):
    if group.empty:
        logging.warning("calc_wallet_token_stats received an empty group.")
        return None

    # Ensure correct data types and handle potential NaN values
    group['price'] = pd.to_numeric(group['price'], errors='coerce')
    group['dest_token_amount'] = pd.to_numeric(group['dest_token_amount'], errors='coerce')
    group['from_token_amount'] = pd.to_numeric(group['from_token_amount'], errors='coerce')
    group.dropna(subset=['price'], inplace=True)
    group['dest_token_amount'] = group['dest_token_amount'].fillna(0)
    group['from_token_amount'] = group['from_token_amount'].fillna(0)
    
    buys = group[group['transaction_type'] == 'buy'].copy()
    sells = group[group['transaction_type'] == 'sell'].copy()
    all_trades = pd.concat([buys, sells]).sort_values('transaction_time')

    # --- Historical Stats ---
    historical_total_buy_amount = buys['dest_token_amount'].sum()
    historical_total_buy_cost = (buys['dest_token_amount'] * buys['price']).sum()
    historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0

    historical_total_sell_amount = sells['from_token_amount'].sum()
    historical_total_sell_value = (sells['from_token_amount'] * sells['price']).sum()
    historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0
    
    # --- P&L Calculation (using average cost basis) ---
    cost_of_goods_sold = historical_total_sell_amount * historical_avg_buy_price
    realized_profit = historical_total_sell_value - cost_of_goods_sold
    
    realized_profit_percentage = (realized_profit / cost_of_goods_sold) * 100 if cost_of_goods_sold > 0 else 0
    if pd.notna(realized_profit_percentage):
        realized_profit_percentage = min(1000000, max(-100, realized_profit_percentage))
    else:
        realized_profit_percentage = 0

    # --- Current Position Stats ---
    current_position = historical_total_buy_amount - historical_total_sell_amount
    
    current_cost = 0
    current_avg_buy_price = 0 # This field is avg_buy_price in the table
    if current_position > 1e-9:
         current_cost = current_position * historical_avg_buy_price
         current_avg_buy_price = historical_avg_buy_price
    else:
        current_position = 0 # Clean up dust amounts

    # Calculate total_holding_seconds
    position = 0
    position_start_time = None
    total_holding_seconds = 0
    last_active_position_closed_at = None

    for _, trade in all_trades.iterrows():
        if trade['transaction_type'] == 'buy':
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
        'position_opened_at': safe_bigint(position_start_time),
        'historical_total_buy_amount': historical_total_buy_amount,
        'historical_total_buy_cost': historical_total_buy_cost,
        'historical_total_sell_amount': historical_total_sell_amount,
        'historical_total_sell_value': historical_total_sell_value,
        'last_active_position_closed_at': safe_bigint(last_active_position_closed_at),
        'historical_avg_buy_price': historical_avg_buy_price,
        'historical_avg_sell_price': historical_avg_sell_price,
        'last_transaction_time': group['transaction_time'].max(),
        'realized_profit': realized_profit,
        'realized_profit_percentage': realized_profit_percentage,
        'total_buy_count': len(buys),
        'total_sell_count': len(sells),
        'total_holding_seconds': safe_bigint(total_holding_seconds),
        'chain': chain,
        'updated_at': datetime.now(timezone(timedelta(hours=8)))
    })

def get_symbol(token_info_dict, addr):
    info = token_info_dict.get(addr)
    if info and info.get("symbol"):
        return truncate_string(info["symbol"], 100)
    return None

def get_name(token_info_dict, addr):
    info = token_info_dict.get(addr)
    if info and info.get("token_name"):
        return truncate_string(info["token_name"], 255)
    return None

def get_icon(token_info_dict, addr):
    info = token_info_dict.get(addr)
    if info and info.get("token_icon"):
        return info["token_icon"]
    return None

def process_kafka_trade(trade_data, engine_smartmoney):
    try:
        event = trade_data['event']
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
        signature = event.get('hash', None)

        # 統一時間戳處理邏輯
        ts = int(timestamp)
        if ts > 1e12:  # 如果是毫秒，轉成秒
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

        token_info_dict = {
            token_address: token_info,
            baseMint: from_token_info,
            quoteMint: dest_token_info
        }

        # Only get SOL balance for Solana addresses
        wallet_balance = 0
        if chain == 'SOLANA':
            try:
                wallet_balance = asyncio.run(get_sol_balance(wallet_address))['balance']['int']
            except Exception as e:
                logging.error(f"Failed to get SOL balance for {wallet_address}: {e}")
                wallet_balance = 0
        
        # 計算 marketcap
        try:
            marketcap = price * float(token_info.get('supply', 0))
        except Exception:
            marketcap = 0

        tx_data = {
            'wallet_address': wallet_address,
            'wallet_balance': wallet_balance,
            'token_address': token_address,
            'token_icon': get_icon(token_info_dict, token_address),
            'token_name': get_name(token_info_dict, token_address),
            'price': price,
            'amount': amount,
            'marketcap': marketcap,
            'value': price * amount,
            'holding_percentage': None,
            'chain': chain,
            'chain_id': chain_id,
            'realized_profit': 0,
            'realized_profit_percentage': 0,
            'transaction_type': side,
            'transaction_time': ts,
            'time': datetime.now(timezone(timedelta(hours=8))),
            'signature': signature,
            'from_token_address': baseMint,
            'from_token_symbol': get_symbol(token_info_dict, baseMint),
            'from_token_amount': fromTokenAmount,
            'dest_token_address': quoteMint,
            'dest_token_symbol': get_symbol(token_info_dict, quoteMint),
            'dest_token_amount': toTokenAmount,
        }
        logging.info(f'[KAFKA] 即將 upsert wallet_transaction: {signature}, {wallet_address}, {token_address}, {ts}')
        upsert_transaction(tx_data)
        logging.info(f'[KAFKA] 完成 upsert wallet_transaction: {signature}, {wallet_address}, {token_address}, {ts}')
        
        # 2. 重新從 Transaction 表撈出該錢包、該幣的所有歷史交易，並重新計算所有受影響日期的 wallet_buy_data
        logging.info(f"[KAFKA] 開始為 {wallet_address}/{token_address} 更新 wallet_buy_data")
        
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
            realized_profit = 0
            realized_profit_percentage = 0
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
                'token_icon': get_icon(token_info_dict, token_address),
                'token_name': get_name(token_info_dict, token_address),
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
                'from_token_symbol': get_symbol(token_info_dict, baseMint),
                'from_token_amount': fromTokenAmount,
                'dest_token_address': quoteMint,
                'dest_token_symbol': get_symbol(token_info_dict, quoteMint),
                'dest_token_amount': toTokenAmount,
            }
            logging.info(f'[KAFKA] 即將 upsert wallet_transaction: {signature}, {wallet_address}, {token_address}, {ts}')
            upsert_transaction(tx_data)
            logging.info(f'[KAFKA] 完成 upsert wallet_transaction: {signature}, {wallet_address}, {token_address}, {ts}')
            
            # 2. 重新從 Transaction 表撈出該錢包、該幣的所有歷史交易，並重新計算所有受影響日期的 wallet_buy_data
            logging.info(f"[KAFKA] 開始為 {wallet_address}/{token_address} 更新 wallet_buy_data")
            
            full_history_query = session.query(Transaction).filter(
                Transaction.wallet_address == wallet_address,
                Transaction.token_address == token_address,
                Transaction.chain == chain
            ).order_by(Transaction.transaction_time)
            
            full_history_df = pd.read_sql(full_history_query.statement, full_history_query.session.bind)

            if not full_history_df.empty:
                full_history_df['date'] = pd.to_datetime(full_history_df['transaction_time'], unit='s', utc=True).dt.tz_convert('Asia/Shanghai').dt.date
                
                all_stats_to_upsert = []
                unique_dates = sorted(full_history_df['date'].unique())

                for d in unique_dates:
                    history_for_day = full_history_df[full_history_df['date'] <= d].copy()
                    
                    if 'chain_id' not in history_for_day.columns:
                        history_for_day['chain_id'] = chain_id

                    stats = calc_wallet_token_stats(history_for_day)
                    if stats is not None:
                        row_dict = stats.to_dict()
                        if row_dict.get('total_amount', 0) < 1e-10:
                            row_dict['total_amount'] = 0
                        all_stats_to_upsert.append(row_dict)

                if all_stats_to_upsert:
                    logging.info(f"[KAFKA] 準備為 {wallet_address}/{token_address} 寫入/更新 {len(all_stats_to_upsert)} 筆 wallet_buy_data")
                    bulk_upsert_wallet_buy_data(all_stats_to_upsert, engine_smartmoney)
        finally:
            session.close()
    except Exception as e:
        logging.error(f"Error processing trade: {str(e)}")
        logging.error(traceback.format_exc())
        raise

def kafka_consumer_loop():
    try:
        consumer = KafkaConsumer(
            'web3_trade_events',
            bootstrap_servers=KAFKA_BROKER,
            group_id='wallet_position_analyze',
            auto_offset_reset='latest',
            enable_auto_commit=False,
            session_timeout_ms=60000,  # 增加 session 超时时间到 60 秒
            heartbeat_interval_ms=20000,  # 设置心跳间隔为 20 秒
            max_poll_interval_ms=300000,  # 设置最大轮询间隔为 5 分钟
            max_poll_records=100,  # 限制每次轮询的最大记录数
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        while True:
            try:
                message_batch = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        try:
                            trade_data = message.value
                            process_kafka_trade(trade_data, engine_smartmoney)
                            # 使用异步提交，避免阻塞
                            consumer.commit_async()
                        except Exception as e:
                            logging.error(f"處理消息時發生錯誤: {str(e)}")
                            logging.error(traceback.format_exc())
                            continue
            except Exception as e:
                logging.error(f"Kafka consumer loop error: {str(e)}")
                logging.error(traceback.format_exc())
                time.sleep(5)
    except Exception as e:
        logging.error(f"Fatal Kafka consumer error: {str(e)}")
        logging.error(traceback.format_exc())
        raise

def main():
    logging.info('=== THIS IS KAFKA ===')
    kafka_consumer_loop()

if __name__ == '__main__':
    main()