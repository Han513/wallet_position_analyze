#!/usr/bin/env python3
"""
analyze_history_optimized.py 性能優化補丁
可以直接替換現有函數來大幅提升性能
"""

import time
import logging
import threading
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text
from collections import defaultdict

# 全局性能配置
PERFORMANCE_CONFIG = {
    'max_workers': min(16, max(4, psutil.cpu_count() - 2)),
    'ultra_batch_size': min(1000, max(200, int(psutil.virtual_memory().total / (1024**3) * 100))),
    'connection_pool_size': min(100, max(20, psutil.cpu_count() * 5)),
    'enable_parallel_insert': True,
    'enable_fast_compute': True,
    'skip_balance_query': True,  # 跳過SOL餘額查詢以提升速度
    'skip_marketcap_calc': True,  # 跳過市值計算以提升速度
}

logging.info(f"性能優化配置: {PERFORMANCE_CONFIG}")

# 全局線程池
_executor = ThreadPoolExecutor(max_workers=PERFORMANCE_CONFIG['max_workers'])

def ultra_fast_bulk_insert_transactions(rows, engine):
    """
    超高速批量插入函數 - 替換 optimized_bulk_insert_transactions
    """
    if not rows:
        logging.info("無資料可寫入 wallet_transaction")
        return 0
    
    start_time = time.time()
    logging.info(f"開始超高速批量插入 wallet_transaction，原始數據量: {len(rows)}")
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['signature'], r['wallet_address'], r['token_address'], r['transaction_time'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    logging.info(f"去重後數據量: {len(deduped_rows)}")
    
    if not deduped_rows:
        logging.info("去重後無資料需要插入")
        return 0
    
    # 使用超大批次
    ultra_batch_size = PERFORMANCE_CONFIG['ultra_batch_size']
    successful_inserts = 0
    
    if PERFORMANCE_CONFIG['enable_parallel_insert'] and len(deduped_rows) > ultra_batch_size:
        # 並行批量插入
        futures = []
        for i in range(0, len(deduped_rows), ultra_batch_size):
            batch = deduped_rows[i:i + ultra_batch_size]
            future = _executor.submit(_single_ultra_batch_insert, batch, engine, i // ultra_batch_size + 1)
            futures.append(future)
        
        # 等待所有批次完成
        for future in as_completed(futures):
            try:
                batch_result = future.result(timeout=60)
                successful_inserts += batch_result
            except Exception as e:
                logging.error(f"並行批次插入失敗: {e}")
    else:
        # 單線程超大批次插入
        for i in range(0, len(deduped_rows), ultra_batch_size):
            batch = deduped_rows[i:i + ultra_batch_size]
            batch_result = _single_ultra_batch_insert(batch, engine, i // ultra_batch_size + 1)
            successful_inserts += batch_result
    
    total_time = time.time() - start_time
    tps = successful_inserts / total_time if total_time > 0 else 0
    logging.info(f"超高速批量插入完成！成功插入/更新記錄數: {successful_inserts}，總耗時: {total_time:.2f}s，TPS: {tps:.0f}")
    return successful_inserts

def _single_ultra_batch_insert(batch, engine, batch_num):
    """單個超大批次插入"""
    try:
        batch_start_time = time.time()
        
        # 預處理數據
        for r in batch:
            if 'token_name' in r and r['token_name']:
                r['token_name'] = r['token_name'][:255] if isinstance(r['token_name'], str) else r['token_name']
            if 'from_token_symbol' in r and r['from_token_symbol']:
                r['from_token_symbol'] = r['from_token_symbol'][:100] if isinstance(r['from_token_symbol'], str) else r['from_token_symbol']
            if 'dest_token_symbol' in r and r['dest_token_symbol']:
                r['dest_token_symbol'] = r['dest_token_symbol'][:100] if isinstance(r['dest_token_symbol'], str) else r['dest_token_symbol']
        
        with engine.begin() as conn:
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
                INSERT INTO dex_query_v1.wallet_transaction ({columns_str})
                VALUES {values_str}
                ON CONFLICT (signature, wallet_address, token_address, transaction_time) 
                DO UPDATE SET {update_clause}
            ''')
            
            conn.execute(upsert_sql, params)
            
            batch_time = time.time() - batch_start_time
            logging.info(f"超大批次 {batch_num} 完成，處理 {len(batch)} 條記錄，耗時 {batch_time:.2f}s")
            return len(batch)
            
    except Exception as e:
        logging.error(f"超大批次 {batch_num} 插入失敗: {str(e)}")
        return 0

def ultra_fast_wallet_buy_data_compute(position_cache):
    """
    超高速wallet_buy_data計算 - 替換 position_cache.get_wallet_buy_data_records()
    """
    start_time = time.time()
    
    if not PERFORMANCE_CONFIG['enable_fast_compute']:
        # 回退到原始方法
        return position_cache.get_wallet_buy_data_records()
    
    wallet_buy_data_records = []
    
    # 並行計算每個錢包-代幣對
    futures = []
    for (wallet_address, token_address, chain), transactions in position_cache.wallet_token_transactions.items():
        if not transactions:
            continue
        future = _executor.submit(_compute_single_wallet_stats, wallet_address, token_address, chain, transactions)
        futures.append(future)
    
    # 收集結果
    for future in as_completed(futures):
        try:
            result = future.result(timeout=30)
            if result:
                wallet_buy_data_records.extend(result)
        except Exception as e:
            logging.error(f"計算wallet統計失敗: {e}")
    
    elapsed = time.time() - start_time
    logging.info(f"超高速計算完成: {len(wallet_buy_data_records)} 筆wallet_buy_data，耗時 {elapsed:.2f}s")
    return wallet_buy_data_records

def _compute_single_wallet_stats(wallet_address, token_address, chain, transactions):
    """計算單個錢包-代幣對的統計"""
    # 按時間排序
    transactions.sort(key=lambda x: x['transaction_time'])
    
    # 按日期分組
    date_groups = defaultdict(list)
    for tx in transactions:
        from datetime import datetime, timezone, timedelta
        date_key = datetime.fromtimestamp(tx['transaction_time'], timezone(timedelta(hours=8))).date()
        date_groups[date_key].append(tx)
    
    results = []
    all_historical_txs = []
    
    for date_key in sorted(date_groups.keys()):
        all_historical_txs.extend(date_groups[date_key])
        
                # 快速統計計算
        buys = [tx for tx in all_historical_txs if tx['transaction_type'] in ["build", "buy"]]
        sells = [tx for tx in all_historical_txs if tx['transaction_type'] in ["clean", "sell"]]
        
        # 基本統計
        total_buy_amount = sum(tx['amount'] for tx in buys)
        total_buy_cost = sum(tx['amount'] * tx['price'] for tx in buys)
        total_sell_amount = sum(tx['amount'] for tx in sells)
        total_sell_value = sum(tx['amount'] * tx['price'] for tx in sells)
        
        avg_buy_price = total_buy_cost / total_buy_amount if total_buy_amount > 0 else 0
        avg_sell_price = total_sell_value / total_sell_amount if total_sell_amount > 0 else 0
        
        current_position = max(0, total_buy_amount - total_sell_amount)
        current_cost = current_position * avg_buy_price if current_position > 0 else 0
        
        # P&L計算
        cost_of_goods_sold = total_sell_amount * avg_buy_price
        realized_profit = total_sell_value - cost_of_goods_sold
        realized_profit_percentage = (realized_profit / cost_of_goods_sold) * 100 if cost_of_goods_sold > 0 else 0
        
        # 限制百分比範圍
        realized_profit_percentage = min(1000000, max(-100, realized_profit_percentage))
        
        chain_id = 501 if chain == 'SOLANA' else 9006 if chain == 'BSC' else 501
        
        record = {
            'wallet_address': wallet_address,
            'token_address': token_address,
            'chain_id': chain_id,
            'date': date_key,
            'total_amount': current_position,
            'total_cost': current_cost,
            'avg_buy_price': avg_buy_price,
            'position_opened_at': 0,  # 簡化計算
            'historical_total_buy_amount': total_buy_amount,
            'historical_total_buy_cost': total_buy_cost,
            'historical_total_sell_amount': total_sell_amount,
            'historical_total_sell_value': total_sell_value,
            'last_active_position_closed_at': 0,  # 簡化計算
            'historical_avg_buy_price': avg_buy_price,
            'historical_avg_sell_price': avg_sell_price,
            'last_transaction_time': max(tx['transaction_time'] for tx in all_historical_txs),
            'realized_profit': realized_profit,
            'realized_profit_percentage': realized_profit_percentage,
            'total_buy_count': len(buys),
            'total_sell_count': len(sells),
            'total_holding_seconds': 0,  # 簡化計算
            'chain': chain,
            'updated_at': datetime.now(timezone(timedelta(hours=8)))
        }
        results.append(record)
    
    return results

def ultra_fast_bulk_upsert_wallet_buy_data(rows, engine_smartmoney):
    """
    超高速wallet_buy_data批量更新函數 - 替換 optimized_bulk_upsert_wallet_buy_data
    """
    if not rows:
        logging.info("bulk_upsert_wallet_buy_data: 無資料可寫入")
        return 0
    
    start_time = time.time()
    logging.info(f"開始超高速批量更新 wallet_buy_data，原始數據量: {len(rows)}")
    
    # 去重處理
    unique_map = {}
    for r in rows:
        key = (r['wallet_address'], r['token_address'], r['chain'], r['date'])
        unique_map[key] = r
    deduped_rows = list(unique_map.values())
    
    if not deduped_rows:
        logging.info("去重後無資料需要寫入")
        return 0
    
    # 使用超大批次
    ultra_batch_size = min(500, len(deduped_rows))
    successful_upserts = 0
    
    if PERFORMANCE_CONFIG['enable_parallel_insert'] and len(deduped_rows) > ultra_batch_size:
        # 並行批量插入
        futures = []
        for i in range(0, len(deduped_rows), ultra_batch_size):
            batch = deduped_rows[i:i + ultra_batch_size]
            future = _executor.submit(_single_wallet_buy_data_ultra_insert, batch, engine_smartmoney, i // ultra_batch_size + 1)
            futures.append(future)
        
        # 等待所有批次完成
        for future in as_completed(futures):
            try:
                batch_result = future.result(timeout=60)
                successful_upserts += batch_result
            except Exception as e:
                logging.error(f"wallet_buy_data並行批次插入失敗: {e}")
    else:
        # 單線程超大批次插入
        for i in range(0, len(deduped_rows), ultra_batch_size):
            batch = deduped_rows[i:i + ultra_batch_size]
            batch_result = _single_wallet_buy_data_ultra_insert(batch, engine_smartmoney, i // ultra_batch_size + 1)
            successful_upserts += batch_result
    
    total_time = time.time() - start_time
    tps = successful_upserts / total_time if total_time > 0 else 0
    logging.info(f"超高速wallet_buy_data批量更新完成！成功更新記錄數: {successful_upserts}，總耗時: {total_time:.2f}s，TPS: {tps:.0f}")
    return successful_upserts

def _single_wallet_buy_data_ultra_insert(batch, engine_smartmoney, batch_num):
    """單個wallet_buy_data超大批次插入"""
    try:
        batch_start_time = time.time()
        
        with engine_smartmoney.begin() as conn:
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
            
            update_clauses = []
            for col in columns:
                if col not in ['wallet_address', 'token_address', 'chain', 'date']:
                    update_clauses.append(f"{col} = EXCLUDED.{col}")
            
            update_clause = ', '.join(update_clauses)
            
            upsert_sql = text(f'''
                INSERT INTO dex_query_v1.wallet_buy_data ({columns_str})
                VALUES {values_str}
                ON CONFLICT (wallet_address, token_address, chain, date) 
                DO UPDATE SET {update_clause}
            ''')
            
            conn.execute(upsert_sql, params)
            
            batch_time = time.time() - batch_start_time
            logging.info(f"wallet_buy_data超大批次 {batch_num} 完成，處理 {len(batch)} 條記錄，耗時 {batch_time:.2f}s")
            return len(batch)
            
    except Exception as e:
        logging.error(f"wallet_buy_data超大批次 {batch_num} 插入失敗: {e}")
        return 0

def apply_performance_patches():
    """
    應用性能補丁到現有模塊
    在 analyze_history_optimized.py 的 main() 函數開始時調用此函數
    """
    import analyze_history_optimized
    
    # 替換關鍵函數
    analyze_history_optimized.optimized_bulk_insert_transactions = ultra_fast_bulk_insert_transactions
    analyze_history_optimized.optimized_bulk_upsert_wallet_buy_data = ultra_fast_bulk_upsert_wallet_buy_data
    
    # 修改WalletPositionCache類的方法
    if hasattr(analyze_history_optimized, 'WalletPositionCache'):
        original_get_records = analyze_history_optimized.WalletPositionCache.get_wallet_buy_data_records
        
        def patched_get_records(self):
            return ultra_fast_wallet_buy_data_compute(self)
        
        analyze_history_optimized.WalletPositionCache.get_wallet_buy_data_records = patched_get_records
    
    logging.info("性能補丁已應用！")

def optimize_data_processing():
    """
    優化數據處理流程的建議
    """
    optimizations = {
        'skip_sol_balance': PERFORMANCE_CONFIG['skip_balance_query'],
        'skip_marketcap': PERFORMANCE_CONFIG['skip_marketcap_calc'],
        'batch_token_info': True,
        'parallel_processing': PERFORMANCE_CONFIG['enable_parallel_insert'],
        'ultra_batching': True
    }
    
    return optimizations

# 清理函數
def cleanup_performance_resources():
    """清理性能優化資源"""
    global _executor
    if _executor:
        _executor.shutdown(wait=True)
        logging.info("性能優化資源已清理")

if __name__ == '__main__':
    print("這是一個性能優化補丁文件")
    print("使用方法：")
    print("1. 在 analyze_history_optimized.py 的開頭添加：")
    print("   from analyze_history_performance_patch import apply_performance_patches")
    print("2. 在 main() 函數開始時調用：")
    print("   apply_performance_patches()")
    print("3. 在程序結束時調用：")
    print("   cleanup_performance_resources()") 