#!/usr/bin/env python3
"""
執行索引創建腳本
"""

import logging
import time
import os
from sqlalchemy import text
from config import engine_smartmoney

logging.basicConfig(
    level=logging.INFO,
    format='[INDEX_CREATOR] %(asctime)s - %(levelname)s - %(message)s'
)

def read_sql_file(file_path):
    """讀取SQL文件內容"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logging.error(f"讀取SQL文件失敗: {str(e)}")
        raise

def execute_sql_commands(sql_content):
    """執行SQL命令"""
    logging.info("開始執行索引創建...")
    
    # 分割SQL命令（以分號分隔，但要注意註釋和字符串）
    commands = []
    current_command = ""
    in_comment = False
    in_string = False
    
    lines = sql_content.split('\n')
    for line in lines:
        line = line.strip()
        
        # 跳過註釋行
        if line.startswith('--') or line.startswith('/*') or not line:
            continue
            
        current_command += line + "\n"
        
        # 檢查是否是完整的命令（以分號結尾）
        if line.endswith(';'):
            if current_command.strip():
                commands.append(current_command.strip())
            current_command = ""
    
    # 執行每個命令
    successful_commands = 0
    failed_commands = 0
    
    with engine_smartmoney.begin() as conn:
        for i, command in enumerate(commands, 1):
            try:
                # 跳過一些不需要執行的命令
                if any(skip_word in command.upper() for skip_word in ['BEGIN;', 'COMMIT;', 'ROLLBACK;']):
                    continue
                
                start_time = time.time()
                logging.info(f"執行命令 {i}/{len(commands)}: {command[:100]}...")
                
                result = conn.execute(text(command))
                
                execution_time = time.time() - start_time
                logging.info(f"✓ 命令 {i} 執行成功，耗時: {execution_time:.2f}s")
                successful_commands += 1
                
            except Exception as e:
                error_msg = str(e)
                if "already exists" in error_msg:
                    logging.warning(f"⚠ 命令 {i} 跳過（索引已存在）: {error_msg}")
                    successful_commands += 1
                else:
                    logging.error(f"✗ 命令 {i} 執行失敗: {error_msg}")
                    failed_commands += 1
                    # 不拋出異常，繼續執行其他命令
    
    return successful_commands, failed_commands

def verify_indexes():
    """驗證索引創建結果"""
    logging.info("驗證索引創建結果...")
    
    try:
        with engine_smartmoney.begin() as conn:
            # 檢查創建的索引
            result = conn.execute(text("""
                SELECT 
                    schemaname,
                    tablename,
                    indexname,
                    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
                FROM pg_stat_user_indexes 
                WHERE schemaname = 'dex_query_v1'
                AND indexname LIKE 'idx_%'
                ORDER BY tablename, indexname;
            """))
            
            indexes = result.fetchall()
            
            if indexes:
                logging.info(f"成功創建 {len(indexes)} 個索引:")
                for idx in indexes:
                    logging.info(f"  - {idx[0]}.{idx[1]}.{idx[2]} ({idx[3]})")
            else:
                logging.warning("未找到任何新創建的索引")
                
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
            
            conn.execute(text("ANALYZE dex_query_v1.wallet_buy_data;"))
            logging.info("✓ wallet_buy_data 統計信息更新完成")
            
    except Exception as e:
        logging.error(f"更新統計信息失敗: {str(e)}")

def main():
    """主函數"""
    logging.info("=== 開始執行索引創建 ===")
    
    # 檢查SQL文件是否存在
    sql_file = "create_indexes.sql"
    if not os.path.exists(sql_file):
        logging.error(f"SQL文件不存在: {sql_file}")
        return
    
    try:
        # 讀取SQL文件
        sql_content = read_sql_file(sql_file)
        logging.info(f"成功讀取SQL文件: {sql_file}")
        
        # 執行SQL命令
        successful, failed = execute_sql_commands(sql_content)
        
        logging.info(f"\n=== 執行結果 ===")
        logging.info(f"成功執行: {successful} 個命令")
        if failed > 0:
            logging.warning(f"失敗命令: {failed} 個")
        
        # 更新統計信息
        update_statistics()
        
        # 驗證結果
        verify_indexes()
        
        logging.info("=== 索引創建完成 ===")
        
        # 提供後續建議
        logging.info("\n=== 後續建議 ===")
        logging.info("1. 運行性能測試: python test_query_performance.py")
        logging.info("2. 監控索引使用情況（運行一段時間後）")
        logging.info("3. 定期執行 ANALYZE 更新統計信息")
        
    except Exception as e:
        logging.error(f"索引創建過程發生錯誤: {str(e)}")
        raise

if __name__ == '__main__':
    main() 