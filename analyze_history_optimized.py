import os
import math
import time
import logging
import traceback
import pandas as pd
import numpy as np
import asyncio
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
from config import engine_trade, engine_smartmoney, CHAIN_IDS, BATCH_SIZE, SHARD_COUNT, SHARD_INDEX
from models import Transaction
from utils_wallet import get_sol_balance
from utils_token import get_token_info_batch_map
from more_itertools import chunked

# 導入性能優化補丁
try:
    from analyze_history_performance_patch import apply_performance_patches, cleanup_performance_resources, optimize_data_processing
    PERFORMANCE_PATCH_AVAILABLE = True
    logging.info("性能優化補丁已載入")
except ImportError:
    PERFORMANCE_PATCH_AVAILABLE = False
    logging.warning("性能優化補丁未找到，使用標準性能模式")

logging.basicConfig(
    level=logging.INFO,
    format='[HISTORY_OPT] %(asctime)s - %(levelname)s - %(message)s',
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

class WalletPositionCache:
    """錢包持倉緩存，用於內存計算避免重複查詢數據庫"""
    
    def __init__(self):
        # 存儲每個錢包-代幣對的交易歷史 {(wallet, token, chain): [transactions]}
        self.wallet_token_transactions = defaultdict(list)
        # 存儲每個錢包-代幣對的持倉狀態 {(wallet, token, chain): position_state}
        self.wallet_positions = defaultdict(lambda: {
            'holding_amount': 0,
            'holding_cost': 0,
            'transactions': []
        })
    
    def add_transaction(self, wallet_address, token_address, chain, tx_data):
        """添加交易記錄到緩存"""
        key = (wallet_address, token_address, chain)
        self.wallet_token_transactions[key].append(tx_data.copy())
        
        # 更新持倉狀態
        position = self.wallet_positions[key]
        
        if tx_data['transaction_type'] in ["build", "buy"]:  # 建倉或加倉 (買入)
            position['holding_amount'] += tx_data['amount']
            position['holding_cost'] += tx_data['amount'] * tx_data['price']
        elif tx_data['transaction_type'] in ["clean", "sell"]:  # 清倉或減倉 (賣出)
            # 計算平均成本
            avg_cost = (position['holding_cost'] / position['holding_amount']) if position['holding_amount'] > 0 else 0
            
            # 計算已實現收益
            tx_data['realized_profit'] = (tx_data['price'] - avg_cost) * tx_data['amount'] if avg_cost > 0 else 0
            tx_data['realized_profit_percentage'] = ((tx_data['price'] / avg_cost - 1) * 100) if avg_cost > 0 else 0
            
            # 更新持倉
            position['holding_amount'] -= tx_data['amount']
            position['holding_cost'] -= avg_cost * tx_data['amount']
            
            if position['holding_amount'] < 0:
                position['holding_amount'] = 0
                position['holding_cost'] = 0
        
        position['transactions'].append(tx_data)
    
    def get_wallet_buy_data_records(self):
        """從緩存計算所有的wallet_buy_data記錄"""
        wallet_buy_data_records = []
        
        for (wallet_address, token_address, chain), transactions in self.wallet_token_transactions.items():
            if not transactions:
                continue
            
            # 按時間排序
            transactions.sort(key=lambda x: x['transaction_time'])
            
            # 按日期分組
            date_groups = defaultdict(list)
            for tx in transactions:
                date_key = datetime.fromtimestamp(tx['transaction_time'], timezone(timedelta(hours=8))).date()
                date_groups[date_key].append(tx)
            
            # 計算每個日期的累計統計
            all_historical_txs = []
            for date_key in sorted(date_groups.keys()):
                all_historical_txs.extend(date_groups[date_key])
                
                # 創建DataFrame用於計算
                df = pd.DataFrame(all_historical_txs)
                if 'chain_id' not in df.columns:
                    chain_id_map = {'SOLANA': 501, 'BSC': 9006}
                    df['chain_id'] = chain_id_map.get(chain, 501)
                else:
                    # 確保 chain_id 是整數類型
                    df['chain_id'] = pd.to_numeric(df['chain_id'], errors='coerce').fillna(501).astype(int)
                df['date'] = date_key
                
                stats = self.calc_wallet_token_stats(df)
                if stats is not None:
                    wallet_buy_data_records.append(stats.to_dict())
        
        return wallet_buy_data_records
    
    def calc_wallet_token_stats(self, group):
        """計算錢包代幣統計數據"""
        if group.empty:
            return None

        # 確保數據類型正確
        group['price'] = pd.to_numeric(group['price'], errors='coerce')
        group['amount'] = pd.to_numeric(group['amount'], errors='coerce')
        group.dropna(subset=['price'], inplace=True)
        group['amount'] = group['amount'].fillna(0)
        
        buys = group[group['transaction_type'].isin(["build", "buy"])].copy()  # 建倉或加倉
        sells = group[group['transaction_type'].isin(["clean", "sell"])].copy()  # 清倉或減倉
        all_trades = pd.concat([buys, sells]).sort_values('transaction_time')

        # 歷史統計
        historical_total_buy_amount = buys['amount'].sum()
        historical_total_buy_cost = (buys['amount'] * buys['price']).sum()
        historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0

        historical_total_sell_amount = sells['amount'].sum()
        historical_total_sell_value = (sells['amount'] * sells['price']).sum()
        historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0
        
        # P&L計算
        cost_of_goods_sold = historical_total_sell_amount * historical_avg_buy_price
        realized_profit = historical_total_sell_value - cost_of_goods_sold
        realized_profit_percentage = (realized_profit / cost_of_goods_sold) * 100 if cost_of_goods_sold > 0 else 0
        
        if pd.notna(realized_profit_percentage):
            realized_profit_percentage = min(1000000, max(-100, realized_profit_percentage))
        else:
            realized_profit_percentage = 0

        # 當前持倉
        current_position = historical_total_buy_amount - historical_total_sell_amount
        current_cost = 0
        current_avg_buy_price = 0
        
        if current_position > 1e-9:
            current_cost = current_position * historical_avg_buy_price
            current_avg_buy_price = historical_avg_buy_price
        else:
            current_position = 0

        # 計算持倉時間
        position = 0
        position_start_time = None
        total_holding_seconds = 0
        last_active_position_closed_at = None

        for _, trade in all_trades.iterrows():
            if trade['transaction_type'] in ["build", "buy"]:  # 建倉或加倉 (買入)
                if position == 0:
                    position_start_time = trade['transaction_time']
                position += trade['amount']
            else:  # 清倉或減倉 (賣出)
                position -= trade['amount']
                if position <= 0 and position_start_time is not None:
                    total_holding_seconds += trade['transaction_time'] - position_start_time
                    last_active_position_closed_at = trade['transaction_time']
                    position_start_time = None
                if position < 0:
                    position = 0

        if position > 0 and position_start_time is not None:
            total_holding_seconds += all_trades['transaction_time'].max() - position_start_time

        chain_id = group['chain_id'].iloc[0]
        # 確保 chain_id 是整數或字符串都能正確處理
        if str(chain_id) == '501' or chain_id == 501:
            chain = 'SOLANA'
        elif str(chain_id) == '9006' or chain_id == 9006:
            chain = 'BSC'
        else:
            chain = str(chain_id)

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
    
    def clear(self):
        """清空緩存"""
        self.wallet_token_transactions.clear()
        self.wallet_positions.clear()

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

def safe_bigint(val):
    """安全的bigint轉換，處理溢出和無效值"""
    try:
        if val is None:
            return 0
        
        # 處理NaN和無窮大
        if isinstance(val, float):
            if math.isnan(val) or math.isinf(val):
                return 0
        
        # 轉換為整數
        int_val = int(float(val))
        
        # PostgreSQL BIGINT 範圍: -9223372036854775808 到 9223372036854775807
        max_bigint = 9223372036854775807
        min_bigint = -9223372036854775808
        
        if int_val > max_bigint:
            logging.warning(f"值 {int_val} 超出BIGINT最大值，截斷為 {max_bigint}")
            return max_bigint
        elif int_val < min_bigint:
            logging.warning(f"值 {int_val} 超出BIGINT最小值，截斷為 {min_bigint}")
            return min_bigint
        
        return int_val
        
    except (ValueError, TypeError, OverflowError) as e:
        logging.warning(f"safe_bigint 轉換失敗 {val}: {str(e)}，返回0")
        return 0
    except Exception as e:
        logging.error(f"safe_bigint 未預期錯誤 {val}: {str(e)}，返回0")
        return 0

@retry(wait=wait_random_exponential(min=2, max=6), stop=stop_after_attempt(3), retry=retry_if_exception_type(Exception))
def fetch_signers_batch(chain_id, last_signer):
    """獲取signer批次，使用優化的查詢方案"""
    
    # 優化方案：使用CTE和分步查詢，減少DISTINCT的負擔
    optimized_sql = f'''
    WITH signer_candidates AS (
        SELECT signer
        FROM dex_query_v1.trades
        WHERE chain_id = {chain_id}
          AND signer > %(last_signer)s
        ORDER BY signer
        LIMIT {BATCH_SIZE * 5}  -- 取更多候選數據
    )
    SELECT DISTINCT signer, {chain_id} AS chain_id
    FROM signer_candidates
    WHERE mod(abs(hashtext(signer)), {SHARD_COUNT}) = {SHARD_INDEX}
    ORDER BY signer
    LIMIT {BATCH_SIZE}
    '''
    
    # 備用方案：如果優化查詢失敗，使用簡化版本
    fallback_sql = f'''
    SELECT signer, {chain_id} AS chain_id
    FROM (
        SELECT DISTINCT signer
        FROM dex_query_v1.trades
        WHERE chain_id = {chain_id}
          AND signer > %(last_signer)s
        ORDER BY signer
        LIMIT {BATCH_SIZE * 3}
    ) t
    WHERE mod(abs(hashtext(signer)), {SHARD_COUNT}) = {SHARD_INDEX}
    ORDER BY signer
    LIMIT {BATCH_SIZE}
    '''
    
    try:
        # 首先嘗試優化查詢
        with engine_trade.begin() as conn:
            # 設置查詢超時
            conn.execute(text("SET statement_timeout = '30s'"))
            
            start_time = time.time()
            result = conn.execute(text(optimized_sql), {"last_signer": last_signer})
            df = pd.DataFrame(result.fetchall(), columns=['signer', 'chain_id'])
            query_time = time.time() - start_time
            
            logging.info(f"優化查詢成功，查詢 {len(df)} 個 signer，耗時 {query_time:.3f}s")
            return df
            
    except Exception as e:
        logging.warning(f"優化查詢失敗: {str(e)}，嘗試備用方案")
        
        try:
            # 清理連接池並重試
            engine_trade.dispose()
            time.sleep(1)
            
            with engine_trade.begin() as conn:
                # 使用更短的超時時間
                conn.execute(text("SET statement_timeout = '15s'"))
                
                start_time = time.time()
                result = conn.execute(text(fallback_sql), {"last_signer": last_signer})
                df = pd.DataFrame(result.fetchall(), columns=['signer', 'chain_id'])
                query_time = time.time() - start_time
                
                logging.info(f"備用查詢成功，查詢 {len(df)} 個 signer，耗時 {query_time:.3f}s")
                return df
                
        except Exception as retry_e:
            logging.error(f"備用查詢也失敗: {str(retry_e)}")
            
            # 最後嘗試：使用最簡單的查詢
            try:
                simple_sql = f'''
                SELECT DISTINCT signer, {chain_id} AS chain_id
                FROM dex_query_v1.trades
                WHERE chain_id = {chain_id}
                  AND signer > %(last_signer)s
                  AND mod(abs(hashtext(signer)), {SHARD_COUNT}) = {SHARD_INDEX}
                ORDER BY signer
                LIMIT {BATCH_SIZE}
                '''
                
                with engine_trade.begin() as conn:
                    conn.execute(text("SET statement_timeout = '10s'"))
                    result = conn.execute(text(simple_sql), {"last_signer": last_signer})
                    df = pd.DataFrame(result.fetchall(), columns=['signer', 'chain_id'])
                    logging.info(f"簡單查詢成功，查詢 {len(df)} 個 signer")
                    return df
                    
            except Exception as final_e:
                logging.error(f"所有查詢方案都失敗: {str(final_e)}")
                raise

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

def optimized_bulk_insert_transactions(rows, engine):
    """優化的批量插入函數，增加數據驗證和錯誤處理"""
    if not rows:
        logging.info("無資料可寫入 wallet_transaction")
        return 0
    
    start_time = time.time()
    logging.info(f"開始批量插入 wallet_transaction，原始數據量: {len(rows)}")
    
    # 數據清理和驗證
    cleaned_rows = []
    for r in rows:
        try:
            # 驗證必要字段
            required_fields = ['signature', 'wallet_address', 'token_address', 'transaction_time']
            if not all(r.get(field) for field in required_fields):
                logging.warning(f"跳過缺少必要字段的記錄: {r}")
                continue
            
            # 清理和驗證數據
            cleaned_row = r.copy()
            
            # 處理字符串長度
            if 'token_name' in cleaned_row and cleaned_row['token_name']:
                cleaned_row['token_name'] = truncate_string(cleaned_row['token_name'], 255)
            if 'from_token_symbol' in cleaned_row and cleaned_row['from_token_symbol']:
                cleaned_row['from_token_symbol'] = truncate_string(cleaned_row['from_token_symbol'], 100)
            if 'dest_token_symbol' in cleaned_row and cleaned_row['dest_token_symbol']:
                cleaned_row['dest_token_symbol'] = truncate_string(cleaned_row['dest_token_symbol'], 100)
            
            # 處理數值字段
            numeric_fields = ['wallet_balance', 'price', 'amount', 'marketcap', 'value', 'holding_percentage', 
                            'realized_profit', 'realized_profit_percentage', 'from_token_amount', 'dest_token_amount']
            for field in numeric_fields:
                if field in cleaned_row:
                    val = cleaned_row[field]
                    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
                        cleaned_row[field] = 0.0
                    elif isinstance(val, (int, float)):
                        # 檢查是否超出PostgreSQL的數值範圍
                        if abs(val) > 1e308:  # 接近PostgreSQL的最大數值
                            cleaned_row[field] = 0.0
                        else:
                            cleaned_row[field] = float(val)
            
            # 處理整數字段
            if 'transaction_time' in cleaned_row:
                cleaned_row['transaction_time'] = safe_bigint(cleaned_row['transaction_time'])
            if 'chain_id' in cleaned_row:
                cleaned_row['chain_id'] = int(cleaned_row['chain_id']) if cleaned_row['chain_id'] else 501
            
            # 處理transaction_type
            if 'transaction_type' in cleaned_row:
                if isinstance(cleaned_row['transaction_type'], (int, float)):
                    # 將數字轉換為字符串
                    type_map = {0: 'clean', 1: 'build', 2: 'buy', 3: 'sell'}
                    cleaned_row['transaction_type'] = type_map.get(int(cleaned_row['transaction_type']), 'build')
                elif not isinstance(cleaned_row['transaction_type'], str):
                    cleaned_row['transaction_type'] = 'build'
            
            cleaned_rows.append(cleaned_row)
            
        except Exception as e:
            logging.warning(f"清理數據時出錯，跳過記錄: {str(e)}")
            continue
    
    if not cleaned_rows:
        logging.warning("清理後無有效數據可插入")
        return 0
    
    logging.info(f"數據清理完成: {len(rows)} -> {len(cleaned_rows)} 條記錄")
    
    # 去重處理
    unique_map = {}
    for r in cleaned_rows:
        key = (r['signature'], r['wallet_address'], r['token_address'], r['transaction_time'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    logging.info(f"去重後數據量: {len(deduped_rows)}")
    
    if not deduped_rows:
        logging.info("去重後無資料需要插入")
        return 0
    
    batch_size = 50  # 減小批次大小以提高穩定性
    total_rows = len(deduped_rows)
    successful_inserts = 0
    
    for i in range(0, total_rows, batch_size):
        batch = deduped_rows[i:i + batch_size]
        batch_start_time = time.time()
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                with engine.begin() as conn:
                    # 設置search_path確保操作在正確的schema中
                    conn.execute(text("SET search_path TO dex_query_v1"))
                    
                    # 構建批量插入SQL
                    columns = list(batch[0].keys())
                    columns_str = ', '.join(columns)
                    
                    values_clauses = []
                    params = {}
                    
                    for idx, row in enumerate(batch):
                        placeholders = []
                        for key, value in row.items():
                            param_name = f"{key}_{idx}"
                            params[param_name] = value
                            placeholders.append(f":{param_name}")
                        values_clauses.append(f"({', '.join(placeholders)})")
                    
                    values_str = ', '.join(values_clauses)
                    
                    # 構建UPDATE子句
                    update_clauses = []
                    for col in columns:
                        if col not in ['signature', 'wallet_address', 'token_address', 'transaction_time']:
                            update_clauses.append(f"{col} = EXCLUDED.{col}")
                    
                    update_clause = ', '.join(update_clauses)
                    
                    upsert_sql = text(f'''
                        INSERT INTO wallet_transaction ({columns_str})
                        VALUES {values_str}
                        ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                        DO UPDATE SET {update_clause}
                    ''')
                    
                    conn.execute(upsert_sql, params)
                    successful_inserts += len(batch)
                    
                    batch_time = time.time() - batch_start_time
                    logging.info(f"批次 {i//batch_size + 1}/{(total_rows-1)//batch_size + 1} 完成，"
                               f"處理 {len(batch)} 條記錄，耗時 {batch_time:.2f}s")
                    break  # 成功則跳出重試循環
                    
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    logging.error(f"批次 {i//batch_size + 1} 插入失敗，已重試 {max_retries} 次: {str(e)}")
                    # 記錄失敗的數據樣例
                    if batch:
                        sample = {k: v for k, v in batch[0].items() if k in ['signature', 'wallet_address', 'token_address', 'transaction_time']}
                        logging.error(f"失敗批次樣例數據: {sample}")
                    raise
                else:
                    logging.warning(f"批次 {i//batch_size + 1} 插入失敗，第 {retry_count} 次重試: {str(e)}")
                    time.sleep(retry_count * 2)  # 遞增等待時間
                    # 清理連接池
                    try:
                        engine.dispose()
                    except:
                        pass
    
    total_time = time.time() - start_time
    logging.info(f"批量插入完成！成功插入/更新記錄數: {successful_inserts}，總耗時: {total_time:.2f}s")
    return successful_inserts

def optimized_bulk_upsert_wallet_buy_data(rows, engine_smartmoney):
    """優化的wallet_buy_data批量更新函數"""
    if not rows:
        logging.info("bulk_upsert_wallet_buy_data: 無資料可寫入")
        return 0
    
    start_time = time.time()
    logging.info(f"開始批量更新 wallet_buy_data，原始數據量: {len(rows)}")
    
    # 檢查並修正chain值
    logging.info(f"嘗試插入的列: {list(rows[0].keys()) if rows else 'None'}")
    
    # 驗證並修正chain值
    invalid_chains = []
    fixed_count = 0
    for row in rows:
        if row['chain'] not in ['SOLANA', 'BSC']:
            invalid_chains.append(row['chain'])
            old_chain = row['chain']
            if str(row.get('chain_id')) == '501' or row.get('chain_id') == 501:
                row['chain'] = 'SOLANA'
            elif str(row.get('chain_id')) == '9006' or row.get('chain_id') == 9006:
                row['chain'] = 'BSC'
            else:
                row['chain'] = 'SOLANA'  # 默認值
            fixed_count += 1
            logging.debug(f"修正chain值: {old_chain} -> {row['chain']} (chain_id: {row.get('chain_id')})")
    
    if invalid_chains:
        unique_invalid_chains = list(set(invalid_chains))
        logging.error(f"發現並修正了 {fixed_count} 個無效的chain值: {unique_invalid_chains}")
    
    # 檢查前幾筆數據的chain值
    for i, row in enumerate(rows[:3]):
        logging.info(f"樣例數據 {i+1}: chain='{row['chain']}', chain_id='{row['chain_id']}'")
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['wallet_address'], r['token_address'], r['chain'], r['date'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    if not deduped_rows:
        logging.info("去重後無資料需要寫入")
        return 0
    
    batch_size = 100
    total_rows = len(deduped_rows)
    logging.info(f"去重後數據量: {total_rows}，批次大小: {batch_size}")
    
    successful_upserts = 0
    
    for i in range(0, total_rows, batch_size):
        batch = deduped_rows[i:i + batch_size]
        batch_start_time = time.time()
        
        try:
            with engine_smartmoney.begin() as conn:
                # 設置search_path確保操作在正確的schema中
                conn.execute(text("SET search_path TO dex_query_v1, public"))
                
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
                successful_upserts += len(batch)
                
                # 添加驗證查詢
                if i == 0:  # 只在第一個批次驗證
                    try:
                        sample_row = batch[0]
                        verify_sql = text("""
                            SELECT COUNT(*) as count FROM dex_query_v1.wallet_buy_data 
                            WHERE wallet_address = :wallet_address 
                            AND token_address = :token_address 
                            AND chain = :chain
                            AND date = :date
                        """)
                        verify_result = conn.execute(verify_sql, {
                            'wallet_address': sample_row['wallet_address'],
                            'token_address': sample_row['token_address'],
                            'chain': sample_row['chain'],
                            'date': sample_row['date']
                        })
                        count = verify_result.fetchone()[0]
                        logging.info(f"wallet_buy_data 驗證查詢: 找到 {count} 條匹配記錄")
                        logging.info(f"樣例記錄: wallet={sample_row['wallet_address'][:20]}..., token={sample_row['token_address'][:20]}..., chain={sample_row['chain']}, date={sample_row['date']}")
                        
                        # 檢查分區情況
                        partition_check_sql = text("""
                            SELECT schemaname, tablename 
                            FROM pg_tables 
                            WHERE schemaname = 'dex_query_v1' 
                            AND tablename LIKE 'wallet_buy_data%'
                            ORDER BY tablename
                        """)
                        partition_result = conn.execute(partition_check_sql)
                        partitions = partition_result.fetchall()
                        logging.info(f"可用的 wallet_buy_data 分區: {[p[1] for p in partitions]}")
                        
                    except Exception as verify_e:
                        logging.error(f"wallet_buy_data 驗證查詢失敗: {verify_e}")
                
                batch_time = time.time() - batch_start_time
                logging.info(f"wallet_buy_data 批次 {i//batch_size + 1}/{(total_rows-1)//batch_size + 1} 完成，"
                           f"處理 {len(batch)} 條記錄，耗時 {batch_time:.2f}s")
                
        except Exception as e:
            logging.error(f"wallet_buy_data 批次 {i//batch_size + 1} 寫入失敗: {str(e)}")
            # 檢查表結構
            try:
                with engine_smartmoney.begin() as conn:
                    check_sql = text("SELECT column_name FROM information_schema.columns WHERE table_name='wallet_buy_data' AND table_schema='dex_query_v1'")
                    columns_result = conn.execute(check_sql)
                    existing_columns = [row[0] for row in columns_result]
                    logging.error(f"wallet_buy_data 表中實際存在的列: {existing_columns}")
                    logging.error(f"嘗試插入的列: {list(batch[0].keys()) if batch else 'None'}")
            except Exception as check_e:
                logging.error(f"檢查表結構失敗: {check_e}")
            raise
    
    total_time = time.time() - start_time
    logging.info(f"wallet_buy_data 批量更新完成！成功更新記錄數: {successful_upserts}，總耗時: {total_time:.2f}s")
    return successful_upserts

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
    logging.info('=== 優化版歷史數據分析開始 ===')
    
    # 應用性能補丁
    if PERFORMANCE_PATCH_AVAILABLE:
        apply_performance_patches()
        optimizations = optimize_data_processing()
        logging.info(f"性能優化配置: {optimizations}")
    
    try:
        for chain_id in CHAIN_IDS:
            logging.info(f"開始處理鏈 {chain_id}")
            
            checkpoint_file = CHECKPOINT_FILE_TEMPLATE.format(chain_id, SHARD_INDEX)
            if os.path.exists(checkpoint_file):
                with open(checkpoint_file, 'r') as f:
                    last_signer = f.read().strip()
            else:
                last_signer = ''
            
            # 創建緩存實例
            position_cache = WalletPositionCache()
            
            while True:
                cycle_start_time = time.time()
                
                signers_df = fetch_signers_batch(chain_id, last_signer)
                if signers_df.empty:
                    logging.info(f"chain_id={chain_id} signer 掃描完畢。")
                    break
                    
                if signers_df['signer'].max() <= last_signer:
                    logging.warning(f'Checkpoint未推進，last_signer={last_signer}，max_signer={signers_df["signer"].max()}，跳出避免死循環')
                    break
                
                logging.info(f"處理 signer: {signers_df['signer'].min()} ~ {signers_df['signer'].max()}，共 {len(signers_df)} 筆")
                
                try:
                    trades = fetch_trades_for_batch(
                        batch=signers_df.itertuples(index=False, name=None), 
                        chain_id=chain_id, 
                        table_name='dex_query_v1.trades', 
                        engine_trade=engine_trade
                    )
                except RetryError as re:
                    logging.error("fetch_trades_for_batch 最終失敗：%s", re)
                    continue
                
                if trades.empty:
                    last_signer = signers_df['signer'].max()
                    with open(checkpoint_file, 'w') as f:
                        f.write(last_signer)
                    continue

                # 數據處理和標準化
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
                
                # 獲取SOL餘額 - 根據性能配置決定是否跳過
                sol_balance_dict = {}
                if PERFORMANCE_PATCH_AVAILABLE:
                    optimizations = optimize_data_processing()
                    skip_balance = optimizations.get('skip_sol_balance', False)
                else:
                    skip_balance = False
                
                if not skip_balance and int(chain_id) == 501 and signers:
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

                # 獲取代幣信息
                token_info_dict = {}
                if all_involved_tokens:
                    for chunk in chunked(all_involved_tokens, 50):
                        chunk_result = loop.run_until_complete(get_token_info_batch_map(list(chunk), chain_id))
                        token_info_dict.update(chunk_result)

                # 使用緩存處理交易
                transaction_rows = []
                for (signer, token_address), group in trades.groupby(['signer', 'token_address']):
                    group = group.sort_values('timestamp')
                    holding_amount = 0
                    holding_cost = 0
                    
                    for trade_row in group.itertuples(index=False):
                        ts = int(trade_row.timestamp)
                        if ts > 1e12:
                            ts = int(ts / 1000)
                        
                        wallet_balance = sol_balance_dict.get(trade_row.signer, 0)
                        token_info = token_info_dict.get(trade_row.token_address, {})
                        price = trade_row.price
                        supply = token_info.get('supply') or 0
                        
                        # 根據性能配置決定是否計算市值
                        if PERFORMANCE_PATCH_AVAILABLE:
                            optimizations = optimize_data_processing()
                            skip_marketcap = optimizations.get('skip_marketcap', False)
                        else:
                            skip_marketcap = False
                        
                        if not skip_marketcap:
                            try:
                                marketcap = price * float(supply)
                            except (ValueError, TypeError):
                                marketcap = 0
                        else:
                            marketcap = 0
                        
                        value = price * trade_row.amount
                        
                        # 計算 holding_percentage
                        holding_percentage = 0.0  # 預設值為0
                        
                        # 判斷交易型態
                        transaction_type = determine_transaction_type(
                            trade_row.side, 
                            trade_row.amount, 
                            holding_amount
                        )
                        
                        if trade_row.side == 'buy':
                            # 買入：計算買入value佔wallet_balance的百分比
                            if wallet_balance > 0:
                                holding_percentage = min(100.0, max(0.0, (value / wallet_balance) * 100))
                            else:
                                holding_percentage = 0.0
                        elif trade_row.side == 'sell':
                            # 賣出：計算賣出數量佔原持倉數量的百分比
                            if holding_amount > 0:
                                sell_percentage = (trade_row.amount / holding_amount) * 100
                                holding_percentage = min(100.0, max(0.0, sell_percentage))
                            else:
                                holding_percentage = 0.0
                        
                        # 構建交易數據
                        tx_data = {
                            'wallet_address': trade_row.signer,
                            'wallet_balance': wallet_balance,
                            'token_address': trade_row.token_address,
                            'token_icon': get_icon(token_info_dict, trade_row.token_address),
                            'token_name': get_name(token_info_dict, trade_row.token_address),
                            'price': price,
                            'amount': trade_row.amount,
                            'marketcap': marketcap,
                            'value': value,
                            'holding_percentage': holding_percentage,  # 使用計算好的百分比
                            'chain': get_chain_name(trade_row.chain_id),
                            'chain_id': str(trade_row.chain_id),
                            'realized_profit': 0,  # 初始值，會在緩存中計算
                            'realized_profit_percentage': 0,
                            'transaction_type': transaction_type,
                            'transaction_time': ts,
                            'time': datetime.now(timezone(timedelta(hours=8))),
                            'signature': trade_row.tx_hash,
                            'from_token_address': trade_row.token_in,
                            'from_token_symbol': get_symbol(token_info_dict, trade_row.token_in),
                            'from_token_amount': trade_row.amount_in,
                            'dest_token_address': trade_row.token_out,
                            'dest_token_symbol': get_symbol(token_info_dict, trade_row.token_out),
                            'dest_token_amount': trade_row.amount_out,
                        }
                        
                        # 添加到緩存，這裡會自動計算realized_profit等字段
                        position_cache.add_transaction(
                            trade_row.signer, 
                            trade_row.token_address, 
                            get_chain_name(trade_row.chain_id), 
                            tx_data
                        )
                        
                        transaction_rows.append(tx_data)
                        
                        # 更新持倉狀態（用於下一筆交易的計算）
                        if trade_row.side == 'buy':
                            holding_amount += trade_row.amount
                            holding_cost += trade_row.amount * price
                        elif trade_row.side == 'sell':
                            holding_amount -= trade_row.amount
                            holding_cost -= (holding_cost / max(holding_amount + trade_row.amount, 1e-9)) * trade_row.amount
                            if holding_amount < 0:
                                holding_amount = 0
                                holding_cost = 0

                # 批量寫入Transaction表
                if transaction_rows:
                    logging.info(f'[批次寫入] 準備寫入 wallet_transaction，批次筆數: {len(transaction_rows)}')
                    insert_count = optimized_bulk_insert_transactions(transaction_rows, engine_smartmoney)
                    logging.info(f'[批次寫入] wallet_transaction 完成，實際寫入: {insert_count} 筆')

                    # 從緩存獲取wallet_buy_data記錄並批量寫入
                    logging.info("從緩存計算 wallet_buy_data...")
                    wallet_buy_data_records = position_cache.get_wallet_buy_data_records()
                    
                    if wallet_buy_data_records:
                        logging.info(f"準備寫入/更新 {len(wallet_buy_data_records)} 筆 wallet_buy_data 記錄")
                        upsert_count = optimized_bulk_upsert_wallet_buy_data(wallet_buy_data_records, engine_smartmoney)
                        logging.info(f"wallet_buy_data 寫入完成，實際處理: {upsert_count} 筆")
                    else:
                        logging.info("無 wallet_buy_data 記錄需要寫入")
                else:
                    logging.info('[批次寫入] 無資料可寫入 wallet_transaction')

                # 清空緩存，準備下一批
                position_cache.clear()
                
                # 更新checkpoint
                last_signer = signers_df['signer'].max()
                with open(checkpoint_file, 'w') as f:
                    f.write(last_signer)
                
                cycle_time = time.time() - cycle_start_time
                logging.info(f"處理完成 signer <= {last_signer}，本輪耗時: {cycle_time:.2f}s\n")
        
        logging.info("所有鏈的 signer 分析任務結束。")
        
    except Exception as e:
        logging.error(f"主程序執行出錯: {e}")
        logging.error(traceback.format_exc())
        raise
    finally:
        # 清理性能優化資源
        if PERFORMANCE_PATCH_AVAILABLE:
            cleanup_performance_resources()

if __name__ == '__main__':
    main()