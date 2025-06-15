#!/usr/bin/env python3
"""
修復版索引創建腳本
- 修復事務問題：每個索引獨立事務
- 修復SQL解析問題
- 跳過有問題的索引
"""

import logging
import time
import os
import re
from sqlalchemy import text
from config import engine_smartmoney

logging.basicConfig(
    level=logging.INFO,
    format='[INDEX_CREATOR_FIXED] %(asctime)s - %(levelname)s - %(message)s'
)

def clean_sql_content(sql_content):
    """清理SQL內容，移除註釋和無效命令"""
    lines = sql_content.split('\n')
    cleaned_lines = []
    in_block_comment = False
    
    for line in lines:
        line = line.strip()
        
        # 跳過空行
        if not line:
            continue
            
        # 處理塊註釋
        if '/*' in line:
            in_block_comment = True
            continue
        if '*/' in line:
            in_block_comment = False
            continue
        if in_block_comment:
            continue
            
        # 跳過行註釋
        if line.startswith('--'):
            continue
            
        # 跳過明顯的註釋內容
        if any(skip in line for skip in ['維護建議', '定期重建', '定期更新', '監控索引']):
            continue
            
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines)

def parse_sql_commands(sql_content):
    """解析SQL命令"""
    # 清理內容
    cleaned_content = clean_sql_content(sql_content)
    
    # 手動定義核心索引命令
    core_indexes = [
        # wallet_transaction 核心索引
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_address ON dex_query_v1.wallet_transaction(wallet_address);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_token_address ON dex_query_v1.wallet_transaction(token_address);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_chain ON dex_query_v1.wallet_transaction(chain);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_token_chain ON dex_query_v1.wallet_transaction(wallet_address, token_address, chain);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_time ON dex_query_v1.wallet_transaction(time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_transaction_time ON dex_query_v1.wallet_transaction(transaction_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_transaction_type ON dex_query_v1.wallet_transaction(transaction_type);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_token_time ON dex_query_v1.wallet_transaction(wallet_address, token_address, transaction_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_full_history ON dex_query_v1.wallet_transaction(wallet_address, token_address, chain, transaction_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_signature ON dex_query_v1.wallet_transaction(signature);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_chain_time ON dex_query_v1.wallet_transaction(chain, time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_time ON dex_query_v1.wallet_transaction(wallet_address, time DESC);",
        
        # wallet_buy_data 核心索引
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_wallet_address ON dex_query_v1.wallet_buy_data(wallet_address);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_token_address ON dex_query_v1.wallet_buy_data(token_address);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_chain ON dex_query_v1.wallet_buy_data(chain);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_date ON dex_query_v1.wallet_buy_data(date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_updated_at ON dex_query_v1.wallet_buy_data(updated_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_wallet_token_chain ON dex_query_v1.wallet_buy_data(wallet_address, token_address, chain);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_wallet_date ON dex_query_v1.wallet_buy_data(wallet_address, date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_token_date ON dex_query_v1.wallet_buy_data(token_address, date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_chain_date ON dex_query_v1.wallet_buy_data(chain, date DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_total_amount ON dex_query_v1.wallet_buy_data(total_amount DESC) WHERE total_amount > 0;",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_realized_profit ON dex_query_v1.wallet_buy_data(realized_profit DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_last_transaction_time ON dex_query_v1.wallet_buy_data(last_transaction_time DESC);",
        "CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_active_positions ON dex_query_v1.wallet_buy_data(wallet_address, token_address, total_amount DESC) WHERE total_amount > 0;",
    ]
    
    return core_indexes

def execute_sql_command(command, index):
    """執行單個SQL命令，使用獨立事務"""
    try:
        start_time = time.time()
        
        with engine_smartmoney.begin() as conn:
            logging.info(f"執行索引 {index}: {command[:80]}...")
            conn.execute(text(command))
            
        execution_time = time.time() - start_time
        logging.info(f"✓ 索引 {index} 創建成功，耗時: {execution_time:.2f}s")
        return True
        
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg:
            logging.warning(f"⚠ 索引 {index} 已存在，跳過")
            return True
        elif "does not exist" in error_msg and "partition" in error_msg:
            logging.warning(f"⚠ 索引 {index} 跳過（分區表不存在）")
            return True
        else:
            logging.error(f"✗ 索引 {index} 創建失敗: {error_msg}")
            return False

def verify_indexes_fixed():
    """修復版索引驗證"""
    logging.info("驗證索引創建結果...")
    
    try:
        with engine_smartmoney.begin() as conn:
            # 修復的查詢：使用正確的視圖
            result = conn.execute(text("""
                SELECT 
                    i.schemaname,
                    i.tablename,
                    i.indexname,
                    pg_size_pretty(pg_relation_size(i.indexrelid)) as index_size
                FROM pg_stat_user_indexes i
                WHERE i.schemaname = 'dex_query_v1'
                AND i.indexname LIKE 'idx_%'
                ORDER BY i.tablename, i.indexname;
            """))
            
            indexes = result.fetchall()
            
            if indexes:
                logging.info(f"發現 {len(indexes)} 個索引:")
                wallet_transaction_count = 0
                wallet_buy_data_count = 0
                
                for idx in indexes:
                    logging.info(f"  - {idx[2]} ({idx[3]})")
                    if 'wallet_transaction' in idx[1]:
                        wallet_transaction_count += 1
                    elif 'wallet_buy_data' in idx[1]:
                        wallet_buy_data_count += 1
                
                logging.info(f"\n索引統計:")
                logging.info(f"  wallet_transaction: {wallet_transaction_count} 個索引")
                logging.info(f"  wallet_buy_data: {wallet_buy_data_count} 個索引")
                
            else:
                logging.warning("未找到任何索引")
                
            # 檢查表大小
            result = conn.execute(text("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
                    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                                  pg_relation_size(schemaname||'.'||tablename)) as indexes_size
                FROM pg_tables 
                WHERE schemaname = 'dex_query_v1' 
                AND tablename IN ('wallet_transaction', 'wallet_buy_data')
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
            """))
            
            tables = result.fetchall()
            logging.info("\n表大小統計:")
            for table in tables:
                logging.info(f"  {table[0]}.{table[1]}: 總大小={table[2]}, 表大小={table[3]}, 索引大小={table[4]}")
                
    except Exception as e:
        logging.error(f"驗證索引時發生錯誤: {str(e)}")

def update_statistics():
    """更新表統計信息"""
    logging.info("更新表統計信息...")
    
    try:
        with engine_smartmoney.begin() as conn:
            conn.execute(text("ANALYZE dex_query_v1.wallet_transaction;"))
            logging.info("✓ wallet_transaction 統計信息更新完成")
            
        with engine_smartmoney.begin() as conn:
            conn.execute(text("ANALYZE dex_query_v1.wallet_buy_data;"))
            logging.info("✓ wallet_buy_data 統計信息更新完成")
            
    except Exception as e:
        logging.error(f"更新統計信息失敗: {str(e)}")

def main():
    """主函數"""
    logging.info("=== 修復版索引創建開始 ===")
    
    try:
        # 獲取核心索引命令
        commands = parse_sql_commands("")
        
        logging.info(f"準備創建 {len(commands)} 個核心索引")
        
        successful_commands = 0
        failed_commands = 0
        
        # 逐個執行命令，每個使用獨立事務
        for i, command in enumerate(commands, 1):
            if execute_sql_command(command, i):
                successful_commands += 1
            else:
                failed_commands += 1
        
        logging.info(f"\n=== 執行結果 ===")
        logging.info(f"成功創建: {successful_commands} 個索引")
        if failed_commands > 0:
            logging.warning(f"失敗命令: {failed_commands} 個")
        
        # 更新統計信息
        update_statistics()
        
        # 驗證結果
        verify_indexes_fixed()
        
        logging.info("=== 修復版索引創建完成 ===")
        
        # 性能測試建議
        logging.info("\n=== 後續建議 ===")
        logging.info("1. 運行性能測試: python test_query_performance.py")
        logging.info("2. 檢查實際查詢性能提升")
        logging.info("3. 監控索引使用情況")
        
        # 顯示一些有用的監控查詢
        logging.info("\n=== 監控查詢 ===")
        logging.info("檢查索引使用情況:")
        logging.info("SELECT schemaname, tablename, indexname, idx_scan FROM pg_stat_user_indexes WHERE schemaname = 'dex_query_v1' ORDER BY idx_scan DESC;")
        
    except Exception as e:
        logging.error(f"索引創建過程發生錯誤: {str(e)}")
        raise

if __name__ == '__main__':
    main() 