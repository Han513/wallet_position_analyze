#!/usr/bin/env python3
"""
數據庫驗證腳本
用於檢查數據庫連接、表結構和數據
"""

import logging
from sqlalchemy import create_engine, text
from config import engine_smartmoney
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='[VERIFY] %(asctime)s - %(levelname)s - %(message)s'
)

def check_database_connection():
    """檢查數據庫連接信息"""
    try:
        with engine_smartmoney.begin() as conn:
            # 基本連接信息
            db_info_sql = text("""
                SELECT 
                    current_database() as database_name,
                    current_user as current_user,
                    inet_server_addr() as server_addr,
                    inet_server_port() as server_port,
                    version() as version
            """)
            result = conn.execute(db_info_sql).fetchone()
            
            logging.info("=== 數據庫連接信息 ===")
            logging.info(f"數據庫名稱: {result[0]}")
            logging.info(f"當前用戶: {result[1]}")
            logging.info(f"服務器地址: {result[2]}")
            logging.info(f"服務器端口: {result[3]}")
            logging.info(f"PostgreSQL版本: {result[4][:50]}...")
            
    except Exception as e:
        logging.error(f"數據庫連接檢查失敗: {e}")

def check_table_structure():
    """檢查表結構"""
    try:
        with engine_smartmoney.begin() as conn:
            # 檢查 wallet_transaction 表
            transaction_sql = text("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'dex_query_v1' 
                AND table_name = 'wallet_transaction'
                ORDER BY ordinal_position
            """)
            transaction_columns = conn.execute(transaction_sql).fetchall()
            
            logging.info("=== wallet_transaction 表結構 ===")
            for col in transaction_columns:
                logging.info(f"  {col[0]} ({col[1]}) - nullable: {col[2]}")
            
            # 檢查 wallet_buy_data 表和分區
            buy_data_sql = text("""
                SELECT schemaname, tablename, tableowner
                FROM pg_tables 
                WHERE schemaname = 'dex_query_v1' 
                AND tablename LIKE 'wallet_buy_data%'
                ORDER BY tablename
            """)
            buy_data_tables = conn.execute(buy_data_sql).fetchall()
            
            logging.info("=== wallet_buy_data 表和分區 ===")
            for table in buy_data_tables:
                logging.info(f"  {table[1]} (owner: {table[2]})")
            
    except Exception as e:
        logging.error(f"表結構檢查失敗: {e}")

def check_recent_data():
    """檢查最近的數據"""
    try:
        with engine_smartmoney.begin() as conn:
            # 檢查 wallet_transaction 最近數據
            transaction_count_sql = text("""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN time >= NOW() - INTERVAL '1 hour' THEN 1 END) as recent_1h,
                    COUNT(CASE WHEN time >= NOW() - INTERVAL '1 day' THEN 1 END) as recent_1d,
                    MAX(time) as latest_time
                FROM dex_query_v1.wallet_transaction
                WHERE chain = 'SOLANA'
            """)
            transaction_stats = conn.execute(transaction_count_sql).fetchone()
            
            logging.info("=== wallet_transaction 數據統計 ===")
            logging.info(f"總記錄數: {transaction_stats[0]}")
            logging.info(f"最近1小時: {transaction_stats[1]}")
            logging.info(f"最近1天: {transaction_stats[2]}")
            logging.info(f"最新時間: {transaction_stats[3]}")
            
            # 檢查 wallet_buy_data 最近數據
            buy_data_count_sql = text("""
                SELECT 
                    COUNT(*) as total_count,
                    COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '1 hour' THEN 1 END) as recent_1h,
                    COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '1 day' THEN 1 END) as recent_1d,
                    MAX(updated_at) as latest_time
                FROM dex_query_v1.wallet_buy_data
                WHERE chain = 'SOLANA'
            """)
            buy_data_stats = conn.execute(buy_data_count_sql).fetchone()
            
            logging.info("=== wallet_buy_data 數據統計 ===")
            logging.info(f"總記錄數: {buy_data_stats[0]}")
            logging.info(f"最近1小時: {buy_data_stats[1]}")
            logging.info(f"最近1天: {buy_data_stats[2]}")
            logging.info(f"最新時間: {buy_data_stats[3]}")
            
    except Exception as e:
        logging.error(f"數據檢查失敗: {e}")

def check_specific_records():
    """檢查特定記錄（基於日誌中的樣例）"""
    try:
        with engine_smartmoney.begin() as conn:
            # 查詢最近的一些 transaction 記錄
            recent_transactions_sql = text("""
                SELECT 
                    signature, wallet_address, token_address, transaction_time, time
                FROM dex_query_v1.wallet_transaction
                WHERE chain = 'SOLANA'
                ORDER BY time DESC
                LIMIT 5
            """)
            recent_transactions = conn.execute(recent_transactions_sql).fetchall()
            
            logging.info("=== 最近的 wallet_transaction 記錄 ===")
            for i, row in enumerate(recent_transactions, 1):
                logging.info(f"  {i}. signature: {row[0][:20]}...")
                logging.info(f"     wallet: {row[1][:20]}...")
                logging.info(f"     token: {row[2][:20]}...")
                logging.info(f"     tx_time: {row[3]}, insert_time: {row[4]}")
            
            # 查詢最近的一些 buy_data 記錄
            recent_buy_data_sql = text("""
                SELECT 
                    wallet_address, token_address, chain, date, updated_at
                FROM dex_query_v1.wallet_buy_data
                WHERE chain = 'SOLANA'
                ORDER BY updated_at DESC
                LIMIT 5
            """)
            recent_buy_data = conn.execute(recent_buy_data_sql).fetchall()
            
            logging.info("=== 最近的 wallet_buy_data 記錄 ===")
            for i, row in enumerate(recent_buy_data, 1):
                logging.info(f"  {i}. wallet: {row[0][:20]}...")
                logging.info(f"     token: {row[1][:20]}...")
                logging.info(f"     chain: {row[2]}, date: {row[3]}, updated: {row[4]}")
                
    except Exception as e:
        logging.error(f"特定記錄檢查失敗: {e}")

def main():
    logging.info("開始數據庫驗證...")
    
    check_database_connection()
    check_table_structure()
    check_recent_data()
    check_specific_records()
    
    logging.info("數據庫驗證完成")

if __name__ == '__main__':
    main() 