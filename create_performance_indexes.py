import logging
from sqlalchemy import text
from config import engine_trade, engine_smartmoney

logging.basicConfig(
    level=logging.INFO,
    format='[INDEX] %(asctime)s - %(levelname)s - %(message)s',
    force=True
)

def create_performance_indexes():
    """創建性能優化索引"""
    logging.info("開始創建性能優化索引...")
    
    indexes = [
        {
            'name': 'idx_trades_chain_signer_optimized',
            'sql': '''
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_chain_signer_optimized 
                ON dex_query_v1.trades (chain_id, signer) 
                WHERE chain_id IN (501, 9006)
            ''',
            'description': '鏈ID和簽名者的複合索引（僅限主要鏈）',
            'concurrent': True
        },
        {
            'name': 'idx_trades_signer_timestamp',
            'sql': '''
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_signer_timestamp 
                ON dex_query_v1.trades (signer, timestamp) 
                WHERE chain_id IN (501, 9006)
            ''',
            'description': '簽名者和時間戳索引',
            'concurrent': True
        },
        {
            'name': 'idx_trades_chain_timestamp',
            'sql': '''
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_chain_timestamp 
                ON dex_query_v1.trades (chain_id, timestamp DESC) 
                WHERE chain_id IN (501, 9006)
            ''',
            'description': '鏈ID和時間戳索引（降序）',
            'concurrent': True
        },
        {
            'name': 'idx_wallet_transaction_composite',
            'sql': '''
                CREATE INDEX IF NOT EXISTS idx_wallet_transaction_composite 
                ON dex_query_v1.wallet_transaction (wallet_address, token_address, chain, transaction_time DESC)
            ''',
            'description': 'wallet_transaction表的複合索引',
            'concurrent': False
        },
        {
            'name': 'idx_wallet_buy_data_composite',
            'sql': '''
                CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_composite 
                ON dex_query_v1.wallet_buy_data (wallet_address, token_address, chain, date DESC)
            ''',
            'description': 'wallet_buy_data表的複合索引',
            'concurrent': False
        }
    ]
    
    success_count = 0
    
    for idx in indexes:
        try:
            logging.info(f"創建索引: {idx['name']} - {idx['description']}")
            
            # 先檢查索引是否已存在
            with engine_trade.begin() as conn:
                check_sql = text("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE schemaname = 'dex_query_v1' 
                        AND indexname = :index_name
                    )
                """)
                
                exists = conn.execute(check_sql, {'index_name': idx['name']}).fetchone()[0]
                
                if exists:
                    logging.info(f"  ✅ 索引 {idx['name']} 已存在，跳過")
                    success_count += 1
                    continue
            
            # 創建索引
            if idx['concurrent']:
                # CONCURRENT索引需要在autocommit模式下創建
                conn = engine_trade.connect()
                conn.execute(text("COMMIT"))  # 確保沒有活躍事務
                conn.execute(text(idx['sql']))
                conn.close()
            else:
                # 普通索引可以在事務中創建
                with engine_trade.begin() as conn:
                    conn.execute(text(idx['sql']))
            
            logging.info(f"  ✅ 索引 {idx['name']} 創建成功")
            success_count += 1
                
        except Exception as e:
            logging.error(f"  ❌ 創建索引 {idx['name']} 失敗: {str(e)}")
            # 如果CONCURRENT失敗，嘗試創建普通索引
            if idx['concurrent']:
                try:
                    logging.info(f"  🔄 嘗試創建普通索引: {idx['name']}")
                    normal_sql = idx['sql'].replace('CONCURRENTLY ', '')
                    with engine_trade.begin() as conn:
                        conn.execute(text(normal_sql))
                    logging.info(f"  ✅ 普通索引 {idx['name']} 創建成功")
                    success_count += 1
                except Exception as e2:
                    logging.error(f"  ❌ 普通索引 {idx['name']} 也失敗: {str(e2)}")
    
    logging.info(f"索引創建完成: {success_count}/{len(indexes)} 成功")
    return success_count == len(indexes)

def analyze_table_statistics():
    """更新表統計信息以優化查詢計劃"""
    logging.info("更新表統計信息...")
    
    tables = [
        'dex_query_v1.trades',
        'dex_query_v1.wallet_transaction',
        'dex_query_v1.wallet_buy_data'
    ]
    
    try:
        with engine_trade.begin() as conn:
            for table in tables:
                logging.info(f"分析表: {table}")
                conn.execute(text(f"ANALYZE {table}"))
                logging.info(f"  ✅ {table} 分析完成")
        
        logging.info("✅ 所有表統計信息更新完成")
        return True
        
    except Exception as e:
        logging.error(f"❌ 更新表統計信息失敗: {str(e)}")
        return False

def check_query_performance():
    """檢查查詢性能"""
    logging.info("檢查查詢性能...")
    
    test_queries = [
        {
            'name': '原始signer查詢',
            'sql': '''
                SELECT DISTINCT signer, 501 AS chain_id
                FROM dex_query_v1.trades
                WHERE chain_id = 501
                  AND signer > '39zsPJy1yRdzGd4berRNuKtvmXJQeC9EqxtJcLz3ZMWz'
                  AND mod(abs(hashtext(signer)), 5) = 0
                ORDER BY signer
                LIMIT 10
            '''
        },
        {
            'name': '優化後signer查詢',
            'sql': '''
                WITH signer_candidates AS (
                    SELECT signer
                    FROM dex_query_v1.trades
                    WHERE chain_id = 501
                      AND signer > '39zsPJy1yRdzGd4berRNuKtvmXJQeC9EqxtJcLz3ZMWz'
                    ORDER BY signer
                    LIMIT 50
                )
                SELECT DISTINCT signer, 501 AS chain_id
                FROM signer_candidates
                WHERE mod(abs(hashtext(signer)), 5) = 0
                ORDER BY signer
                LIMIT 10
            '''
        }
    ]
    
    for query in test_queries:
        try:
            logging.info(f"測試查詢: {query['name']}")
            
            with engine_trade.begin() as conn:
                import time
                start_time = time.time()
                
                result = conn.execute(text(query['sql']))
                rows = result.fetchall()
                
                query_time = time.time() - start_time
                logging.info(f"  ✅ 查詢完成: {len(rows)} 行，耗時 {query_time:.3f}s")
                
        except Exception as e:
            logging.error(f"  ❌ 查詢失敗: {str(e)}")

def main():
    """主函數"""
    logging.info("=== 數據庫性能優化開始 ===")
    
    # 1. 創建索引
    index_success = create_performance_indexes()
    
    # 2. 更新統計信息
    stats_success = analyze_table_statistics()
    
    # 3. 測試查詢性能
    check_query_performance()
    
    if index_success and stats_success:
        logging.info("🎉 數據庫性能優化完成！")
    else:
        logging.warning("⚠️ 部分優化操作失敗，請檢查日誌")

if __name__ == '__main__':
    main() 