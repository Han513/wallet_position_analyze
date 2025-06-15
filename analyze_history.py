import os
import math
import time
import logging
import traceback
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
from config import engine_trade, engine_smartmoney, CHAIN_IDS, BATCH_SIZE, SHARD_COUNT, SHARD_INDEX
from models import Transaction
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map
from more_itertools import chunked

logging.basicConfig(
    level=logging.INFO,
    format='[HISTORY] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("solana").setLevel(logging.WARNING)

CHECKPOINT_FILE_TEMPLATE = 'batch_checkpoint_signer_{}_{}.txt'

WSOL = 'So11111111111111111111111111111111111111112'
USDT = 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
STABLES = {USDT, USDC}

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

def get_chain_name(chain_id):
    if str(chain_id) == "501":
        return "SOLANA"
    elif str(chain_id) == "9006":
        return "BSC"
    else:
        return str(chain_id)

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

def fetch_signers_batch(chain_id, last_signer):
    sql = f'''
    SELECT DISTINCT signer, {chain_id} AS chain_id
    FROM dex_query_v1.trades
    WHERE chain_id = {chain_id}
      AND signer > %(last_signer)s
      AND mod(abs(hashtext(signer)), {SHARD_COUNT}) = {SHARD_INDEX}
    ORDER BY signer
    LIMIT {BATCH_SIZE}
    '''
    return pd.read_sql(sql, con=engine_trade, params={"last_signer": last_signer})

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

def bulk_insert_transactions(rows, engine):
    if not rows:
        return
    
    logging.info(f"開始批量插入，原始數據量: {len(rows)}")
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['signature'], r['wallet_address'], r['token_address'], r['transaction_time'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    logging.info(f"去重後數據量: {len(deduped_rows)}")
    
    batch_size = 50
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
                    
                    # 改進的更新語句：只更新非 NULL 值
                    update_clauses = []
                    for col in columns:
                        if col not in ['signature', 'wallet_address', 'token_address', 'transaction_time']:
                            update_clauses.append(f"{col} = COALESCE(EXCLUDED.{col}, wallet_transaction.{col})")
                    
                    update_clause = ', '.join(update_clauses)
                    
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

def main():
    logging.info('=== THIS IS HISTORY ===')
    for chain_id in CHAIN_IDS:
        checkpoint_file = CHECKPOINT_FILE_TEMPLATE.format(chain_id, SHARD_INDEX)
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                last_signer = f.read().strip()
        else:
            last_signer = ''
        while True:
            signers_df = fetch_signers_batch(chain_id, last_signer)
            if signers_df.empty:
                logging.info(f"chain_id={chain_id} signer 掃描完畢。")
                break
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
                price = row['price_usd'] if 'price_usd' in row and not pd.isnull(row['price_usd']) else row['price']
                signer = row['signer']
                chain_id = row['chain_id']
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
                }
            trades = trades.sort_values('timestamp')
            trades = trades.apply(normalize_trade, axis=1, result_type='expand')
            trades = trades[trades['token_address'] != WSOL]
            
            def extract_date(row):
                ts = int(row['timestamp'])
                if ts > 1e12:
                    ts = int(ts / 1000)
                dt = datetime.fromtimestamp(ts, timezone(timedelta(hours=8)))
                return dt.date()
            trades['date'] = trades.apply(extract_date, axis=1)

            signers = trades['signer'].unique().tolist()
            all_involved_tokens = pd.concat([trades['token_in'], trades['token_out'], trades['token_address']]).unique().tolist()
            
            sol_balance_dict = {}
            if int(chain_id) == 501 and signers:
                async def batch_get_sol_balances(signers):
                    tasks = [get_sol_balance(signer) for signer in signers]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    balances = {}
                    for i, res in enumerate(results):
                        if isinstance(res, Exception):
                            logging.warning(f"Failed to get balance for {signers[i]}: {res}")
                            balances[signers[i]] = 0
                        else:
                            balances[signers[i]] = res.get('balance', {}).get('int', 0)
                    return balances
                sol_balance_dict = loop.run_until_complete(batch_get_sol_balances(signers))

            token_info_dict = {}
            if all_involved_tokens:
                for chunk in chunked(all_involved_tokens, 50):
                    chunk_result = loop.run_until_complete(get_token_info_batch_map(list(chunk), chain_id))
                    token_info_dict.update(chunk_result)

            transaction_rows = []
            for (signer, token_address), group in trades.groupby(['signer', 'token_address']):
                group = group.sort_values('timestamp')
                holding_amount = 0
                holding_cost = 0
                original_holding_amount = 0
                for trade_row in group.itertuples(index=False):
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
                    
                    wallet_balance = sol_balance_dict.get(trade_row.signer, 0)
                    token_info = token_info_dict.get(trade_row.token_address, {})
                    price = trade_row.price
                    supply = token_info.get('supply') or 0
                    try:
                        marketcap = price * float(supply)
                    except (ValueError, TypeError):
                        marketcap = 0
                    
                    value = price * trade_row.amount
                    
                    holding_percentage = 0
                    if trade_row.side == 'buy':
                        if wallet_balance > 0:
                            holding_percentage = min(100, (value / wallet_balance) * 100) if wallet_balance > 0 else 0
                    elif trade_row.side == 'sell':
                        if original_holding_amount > 0:
                            holding_percentage = min(100, (trade_row.amount / original_holding_amount) * 100)
                    
                    token_icon = get_icon(token_info_dict, trade_row.token_address)
                    token_name = get_name(token_info_dict, trade_row.token_address)
                    from_token_symbol = get_symbol(token_info_dict, trade_row.token_in)
                    dest_token_symbol = get_symbol(token_info_dict, trade_row.token_out)
                    
                    tx_data = {
                        'wallet_address': trade_row.signer,
                        'wallet_balance': wallet_balance,
                        'token_address': trade_row.token_address,
                        'token_icon': token_icon,
                        'token_name': token_name,
                        'price': price,
                        'amount': trade_row.amount,
                        'marketcap': marketcap,
                        'value': value,
                        'holding_percentage': holding_percentage,
                        'chain': get_chain_name(trade_row.chain_id),
                        'chain_id': str(trade_row.chain_id),
                        'realized_profit': realized_profit,
                        'realized_profit_percentage': realized_profit_percentage,
                        'transaction_type': trade_row.side,
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

            if not transaction_rows:
                logging.info('[批次UPsert] 無資料可寫入 wallet_transaction')
            else:
                logging.info(f'[批次UPsert] 準備寫入 wallet_transaction，批次筆數: {len(transaction_rows)}')
                all_keys = [(r['signature'], r['wallet_address'], r['token_address'], r['transaction_time']) for r in transaction_rows]
                logging.info(f'[批次UPsert] 主鍵範圍: {all_keys[0]} ~ {all_keys[-1]}')
                bulk_insert_transactions(transaction_rows, engine_smartmoney)
                logging.info(f'[批次UPsert] 寫入 wallet_transaction 完成，批次筆數: {len(transaction_rows)}')

                logging.info("開始統計並寫入 wallet_buy_data...")
                session = sessionmaker(bind=engine_smartmoney)()
                try:
                    new_tx_df = pd.DataFrame(transaction_rows)
                    pairs_to_update_df = new_tx_df[['wallet_address', 'token_address', 'chain']].drop_duplicates()
                    
                    if not pairs_to_update_df.empty:
                        from sqlalchemy import or_, and_
                        
                        for chunk_df in np.array_split(pairs_to_update_df, len(pairs_to_update_df) // 100 + 1):
                            chunk_pairs = [tuple(x) for x in chunk_df.to_numpy()]
                            if not chunk_pairs:
                                continue
                            
                            filters = [
                                and_(
                                    Transaction.wallet_address == w,
                                    Transaction.token_address == t,
                                    Transaction.chain == c
                                ) for w, t, c in chunk_pairs
                            ]
                            
                            query = session.query(Transaction).filter(or_(*filters))
                            all_tx_df = pd.read_sql(query.statement, query.session.bind)
                            logging.info(f"查詢到 {len(all_tx_df)} 筆歷史交易用於更新 wallet_buy_data (批次)")

                            if not all_tx_df.empty:
                                all_tx_df['date'] = pd.to_datetime(all_tx_df['transaction_time'], unit='s', utc=True).dt.tz_convert('Asia/Shanghai').dt.date
                                grouped_by_pair = all_tx_df.groupby(['wallet_address', 'token_address', 'chain'])
                                all_stats_to_upsert = []

                                for pair_key, pair_history_df in grouped_by_pair:
                                    unique_dates = sorted(pair_history_df['date'].unique())
                                    for d in unique_dates:
                                        history_for_day = pair_history_df[pair_history_df['date'] <= d].copy()
                                        if 'chain_id' not in history_for_day.columns:
                                            chain_id_map = {'SOLANA': 501, 'BSC': 9006}
                                            history_for_day['chain_id'] = chain_id_map.get(pair_key[2], 0)
                                        
                                        stats = calc_wallet_token_stats(history_for_day)
                                        if stats is not None:
                                            all_stats_to_upsert.append(stats.to_dict())
                                
                                if all_stats_to_upsert:
                                    logging.info(f"準備寫入/更新 {len(all_stats_to_upsert)} 筆 wallet_buy_data 記錄")
                                    bulk_upsert_wallet_buy_data(all_stats_to_upsert, engine_smartmoney)
                except Exception as e:
                    logging.error(f"更新 wallet_buy_data 過程中出錯: {e}")
                    logging.error(traceback.format_exc())
                finally:
                    session.close()
            
            last_signer = signers_df['signer'].max()
            with open(checkpoint_file, 'w') as f:
                f.write(last_signer)
            logging.info(f"處理完成 signer <= {last_signer}\n")
    logging.info("所有 signer 分析任務結束。")

if __name__ == '__main__':
    main()