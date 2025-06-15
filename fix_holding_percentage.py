#!/usr/bin/env python3
"""
修復 holding_percentage 為 NULL 的記錄
將 NULL 值設置為 0，並重新計算合理的百分比值
"""

import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import text
from config import engine_smartmoney

logging.basicConfig(
    level=logging.INFO,
    format='[FIX_HP] %(asctime)s - %(levelname)s - %(message)s'
)

def fix_null_holding_percentage():
    """修復 holding_percentage 為 NULL 的記錄"""
    
    try:
        with engine_smartmoney.begin() as conn:
            # 首先統計有多少 NULL 記錄
            count_sql = text("""
                SELECT COUNT(*) as null_count 
                FROM dex_query_v1.wallet_transaction 
                WHERE holding_percentage IS NULL
            """)
            
            result = conn.execute(count_sql)
            null_count = result.fetchone()[0]
            
            logging.info(f"發現 {null_count} 筆 holding_percentage 為 NULL 的記錄")
            
            if null_count == 0:
                logging.info("沒有需要修復的記錄")
                return
            
            # 將所有 NULL 值設為 0
            update_sql = text("""
                UPDATE dex_query_v1.wallet_transaction 
                SET holding_percentage = 0.0 
                WHERE holding_percentage IS NULL
            """)
            
            conn.execute(update_sql)
            logging.info(f"已將 {null_count} 筆記錄的 holding_percentage 設為 0")
            
            # 驗證修復結果
            verify_sql = text("""
                SELECT COUNT(*) as remaining_null 
                FROM dex_query_v1.wallet_transaction 
                WHERE holding_percentage IS NULL
            """)
            
            result = conn.execute(verify_sql)
            remaining = result.fetchone()[0]
            
            if remaining == 0:
                logging.info("✅ 所有 NULL 值已成功修復")
            else:
                logging.warning(f"⚠️ 仍有 {remaining} 筆記錄為 NULL")
                
    except Exception as e:
        logging.error(f"修復過程中發生錯誤: {str(e)}")
        raise

def recalculate_holding_percentage_for_recent_data():
    """重新計算最近24小時的 holding_percentage"""
    
    try:
        logging.info("開始重新計算最近24小時的 holding_percentage...")
        
        with engine_smartmoney.begin() as conn:
            # 獲取最近24小時的交易記錄
            recent_sql = text("""
                SELECT 
                    wallet_address, 
                    token_address, 
                    chain,
                    transaction_type, 
                    amount, 
                    price, 
                    wallet_balance,
                    signature,
                    transaction_time
                FROM dex_query_v1.wallet_transaction 
                WHERE time >= NOW() - INTERVAL '24 hours'
                ORDER BY wallet_address, token_address, transaction_time
            """)
            
            df = pd.read_sql(recent_sql, conn)
            
            if df.empty:
                logging.info("最近24小時沒有交易記錄需要重新計算")
                return
            
            logging.info(f"找到 {len(df)} 筆最近24小時的交易記錄")
            
            updates = []
            
            # 按錢包和代幣分組處理
            for (wallet, token, chain), group in df.groupby(['wallet_address', 'token_address', 'chain']):
                group = group.sort_values('transaction_time')
                holding_amount = 0
                
                for _, row in group.iterrows():
                    holding_percentage = 0.0
                    
                    if row['transaction_type'] == 'buy':
                        # 買入：計算買入value佔wallet_balance的百分比
                        value = row['price'] * row['amount']
                        if row['wallet_balance'] > 0:
                            holding_percentage = min(100.0, max(0.0, (value / row['wallet_balance']) * 100))
                        holding_amount += row['amount']
                        
                    elif row['transaction_type'] == 'sell':
                        # 賣出：計算賣出數量佔原持倉數量的百分比
                        if holding_amount > 0:
                            sell_percentage = (row['amount'] / holding_amount) * 100
                            holding_percentage = min(100.0, max(0.0, sell_percentage))
                        holding_amount = max(0, holding_amount - row['amount'])
                    
                    updates.append({
                        'signature': row['signature'],
                        'wallet_address': row['wallet_address'],
                        'token_address': row['token_address'],
                        'transaction_time': row['transaction_time'],
                        'holding_percentage': holding_percentage
                    })
            
            # 批量更新
            if updates:
                logging.info(f"準備更新 {len(updates)} 筆記錄的 holding_percentage")
                
                batch_size = 100
                updated_count = 0
                
                for i in range(0, len(updates), batch_size):
                    batch = updates[i:i + batch_size]
                    
                    for update in batch:
                        update_sql = text("""
                            UPDATE dex_query_v1.wallet_transaction 
                            SET holding_percentage = :holding_percentage
                            WHERE signature = :signature 
                            AND wallet_address = :wallet_address 
                            AND token_address = :token_address 
                            AND transaction_time = :transaction_time
                        """)
                        
                        conn.execute(update_sql, update)
                        updated_count += 1
                    
                    if i % (batch_size * 10) == 0:
                        logging.info(f"已更新 {min(i + batch_size, len(updates))} / {len(updates)} 筆記錄")
                
                logging.info(f"✅ 成功重新計算並更新了 {updated_count} 筆記錄的 holding_percentage")
            
    except Exception as e:
        logging.error(f"重新計算過程中發生錯誤: {str(e)}")
        raise

def main():
    """主函數"""
    logging.info("=== 開始修復 holding_percentage ===")
    
    try:
        # Step 1: 修復 NULL 值
        fix_null_holding_percentage()
        
        # Step 2: 重新計算最近數據的百分比
        recalculate_holding_percentage_for_recent_data()
        
        logging.info("=== holding_percentage 修復完成 ===")
        
        # 顯示修復後的統計信息
        with engine_smartmoney.begin() as conn:
            stats_sql = text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN holding_percentage IS NULL THEN 1 END) as null_count,
                    AVG(holding_percentage) as avg_percentage,
                    MIN(holding_percentage) as min_percentage,
                    MAX(holding_percentage) as max_percentage
                FROM dex_query_v1.wallet_transaction
                WHERE time >= NOW() - INTERVAL '24 hours'
            """)
            
            result = conn.execute(stats_sql)
            stats = result.fetchone()
            
            logging.info("=== 修復後統計信息 ===")
            logging.info(f"最近24小時總記錄數: {stats[0]}")
            logging.info(f"NULL 記錄數: {stats[1]}")
            logging.info(f"平均 holding_percentage: {stats[2]:.2f}%" if stats[2] else "N/A")
            logging.info(f"最小值: {stats[3]:.2f}%" if stats[3] is not None else "N/A")
            logging.info(f"最大值: {stats[4]:.2f}%" if stats[4] is not None else "N/A")
        
    except Exception as e:
        logging.error(f"修復過程失敗: {str(e)}")
        raise

if __name__ == '__main__':
    main() 