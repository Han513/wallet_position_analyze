#!/usr/bin/env python3
"""
查詢性能測試腳本
用於測試索引創建前後的性能差異
"""

import logging
import time
import psutil
from sqlalchemy import text
from config import engine_smartmoney

logging.basicConfig(
    level=logging.INFO,
    format='[PERF_TEST] %(asctime)s - %(levelname)s - %(message)s'
)

class QueryPerformanceTester:
    """查詢性能測試器"""
    
    def __init__(self):
        self.engine = engine_smartmoney
        self.test_results = []
    
    def execute_query(self, query_name, sql, params=None):
        """執行查詢並記錄性能"""
        try:
            start_time = time.time()
            start_cpu = psutil.cpu_percent()
            
            with self.engine.begin() as conn:
                result = conn.execute(text(sql), params or {})
                row_count = len(result.fetchall())
            
            end_time = time.time()
            end_cpu = psutil.cpu_percent()
            
            execution_time = end_time - start_time
            cpu_usage = end_cpu - start_cpu
            
            result_data = {
                'query_name': query_name,
                'execution_time': execution_time,
                'row_count': row_count,
                'cpu_usage': cpu_usage
            }
            
            self.test_results.append(result_data)
            
            logging.info(f"{query_name}: {execution_time:.3f}s, {row_count} rows, CPU: {cpu_usage:.1f}%")
            
            return result_data
            
        except Exception as e:
            logging.error(f"{query_name} 執行失敗: {str(e)}")
            return None
    
    def test_wallet_transaction_queries(self):
        """測試 wallet_transaction 表的查詢"""
        logging.info("=== 測試 wallet_transaction 查詢性能 ===")
        
        # 測試1: 按錢包地址查詢
        self.execute_query(
            "錢包交易查詢",
            """
            SELECT * FROM dex_query_v1.wallet_transaction 
            WHERE wallet_address LIKE '2d%' 
            ORDER BY transaction_time DESC 
            LIMIT 100
            """
        )
        
        # 測試2: 按錢包+代幣查詢
        self.execute_query(
            "錢包代幣交易查詢",
            """
            SELECT * FROM dex_query_v1.wallet_transaction 
            WHERE wallet_address LIKE '2d%' 
            AND token_address LIKE '1%'
            ORDER BY transaction_time DESC 
            LIMIT 50
            """
        )
        
        # 測試3: 按時間範圍查詢
        self.execute_query(
            "最近24小時交易查詢",
            """
            SELECT wallet_address, token_address, transaction_type, amount, price 
            FROM dex_query_v1.wallet_transaction 
            WHERE time >= NOW() - INTERVAL '24 hours'
            ORDER BY time DESC 
            LIMIT 200
            """
        )
        
        # 測試4: 按鏈查詢
        self.execute_query(
            "SOLANA鏈交易查詢",
            """
            SELECT wallet_address, COUNT(*) as tx_count, SUM(amount * price) as total_value
            FROM dex_query_v1.wallet_transaction 
            WHERE chain = 'SOLANA'
            AND time >= NOW() - INTERVAL '7 days'
            GROUP BY wallet_address
            ORDER BY total_value DESC
            LIMIT 100
            """
        )
        
        # 測試5: 複雜聚合查詢
        self.execute_query(
            "交易統計查詢",
            """
            SELECT 
                token_address,
                transaction_type,
                COUNT(*) as tx_count,
                AVG(amount) as avg_amount,
                SUM(amount * price) as total_value,
                AVG(holding_percentage) as avg_holding_pct
            FROM dex_query_v1.wallet_transaction 
            WHERE chain = 'SOLANA'
            AND time >= NOW() - INTERVAL '3 days'
            GROUP BY token_address, transaction_type
            HAVING COUNT(*) > 5
            ORDER BY total_value DESC
            LIMIT 50
            """
        )
        
        # 測試6: 簽名查重
        self.execute_query(
            "交易去重檢查",
            """
            SELECT signature, COUNT(*) as duplicate_count
            FROM dex_query_v1.wallet_transaction 
            WHERE time >= NOW() - INTERVAL '1 day'
            GROUP BY signature
            HAVING COUNT(*) > 1
            LIMIT 100
            """
        )
    
    def test_wallet_buy_data_queries(self):
        """測試 wallet_buy_data 表的查詢"""
        logging.info("=== 測試 wallet_buy_data 查詢性能 ===")
        
        # 測試1: 按錢包查詢持倉
        self.execute_query(
            "錢包持倉查詢",
            """
            SELECT * FROM dex_query_v1.wallet_buy_data 
            WHERE wallet_address LIKE '2d%'
            AND total_amount > 0
            ORDER BY total_cost DESC
            LIMIT 100
            """
        )
        
        # 測試2: 按代幣查詢持有者
        self.execute_query(
            "代幣持有者查詢",
            """
            SELECT wallet_address, total_amount, total_cost, realized_profit
            FROM dex_query_v1.wallet_buy_data 
            WHERE token_address LIKE '1%'
            AND chain = 'SOLANA'
            AND total_amount > 0
            ORDER BY total_amount DESC
            LIMIT 50
            """
        )
        
        # 測試3: 按日期範圍查詢
        self.execute_query(
            "最近持倉變化查詢",
            """
            SELECT * FROM dex_query_v1.wallet_buy_data 
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            AND total_amount > 0
            ORDER BY updated_at DESC
            LIMIT 200
            """
        )
        
        # 測試4: 收益排行
        self.execute_query(
            "收益排行查詢",
            """
            SELECT 
                wallet_address,
                token_address,
                total_amount,
                realized_profit,
                realized_profit_percentage
            FROM dex_query_v1.wallet_buy_data 
            WHERE chain = 'SOLANA'
            AND realized_profit IS NOT NULL
            ORDER BY realized_profit DESC
            LIMIT 100
            """
        )
        
        # 測試5: 活躍錢包統計
        self.execute_query(
            "活躍錢包統計",
            """
            SELECT 
                wallet_address,
                COUNT(*) as token_count,
                SUM(total_cost) as total_investment,
                SUM(realized_profit) as total_profit,
                AVG(realized_profit_percentage) as avg_profit_pct
            FROM dex_query_v1.wallet_buy_data 
            WHERE chain = 'SOLANA'
            AND date >= CURRENT_DATE - INTERVAL '30 days'
            AND total_amount > 0
            GROUP BY wallet_address
            HAVING COUNT(*) > 3
            ORDER BY total_investment DESC
            LIMIT 50
            """
        )
        
        # 測試6: 跨表關聯查詢
        self.execute_query(
            "持倉與交易關聯查詢",
            """
            SELECT 
                b.wallet_address,
                b.token_address,
                b.total_amount,
                COUNT(t.id) as recent_tx_count,
                MAX(t.time) as last_tx_time
            FROM dex_query_v1.wallet_buy_data b
            LEFT JOIN dex_query_v1.wallet_transaction t ON 
                b.wallet_address = t.wallet_address 
                AND b.token_address = t.token_address
                AND t.time >= NOW() - INTERVAL '7 days'
            WHERE b.total_amount > 0
            AND b.chain = 'SOLANA'
            GROUP BY b.wallet_address, b.token_address, b.total_amount
            ORDER BY recent_tx_count DESC
            LIMIT 100
            """
        )
    
    def test_explain_plans(self):
        """測試查詢計劃"""
        logging.info("=== 查詢計劃分析 ===")
        
        explain_queries = [
            {
                'name': '錢包代幣查詢計劃',
                'sql': """
                EXPLAIN (ANALYZE, BUFFERS) 
                SELECT * FROM dex_query_v1.wallet_transaction 
                WHERE wallet_address LIKE '2d%' 
                AND token_address LIKE '1%'
                ORDER BY transaction_time DESC 
                LIMIT 10
                """
            },
            {
                'name': '持倉查詢計劃',
                'sql': """
                EXPLAIN (ANALYZE, BUFFERS)
                SELECT * FROM dex_query_v1.wallet_buy_data 
                WHERE wallet_address LIKE '2d%' 
                AND chain = 'SOLANA' 
                AND total_amount > 0
                ORDER BY total_cost DESC
                LIMIT 10
                """
            }
        ]
        
        for query in explain_queries:
            try:
                with self.engine.begin() as conn:
                    result = conn.execute(text(query['sql']))
                    plan = result.fetchall()
                    
                logging.info(f"\n{query['name']}:")
                for row in plan:
                    logging.info(f"  {row[0]}")
                    
            except Exception as e:
                logging.error(f"{query['name']} 執行失敗: {str(e)}")
    
    def generate_report(self):
        """生成性能報告"""
        if not self.test_results:
            logging.warning("沒有測試結果可報告")
            return
        
        logging.info("\n=== 性能測試報告 ===")
        
        # 按執行時間排序
        sorted_results = sorted(self.test_results, key=lambda x: x['execution_time'], reverse=True)
        
        logging.info(f"總測試數量: {len(self.test_results)}")
        logging.info(f"平均執行時間: {sum(r['execution_time'] for r in self.test_results) / len(self.test_results):.3f}s")
        
        logging.info("\n最慢的5個查詢:")
        for i, result in enumerate(sorted_results[:5], 1):
            logging.info(f"{i}. {result['query_name']}: {result['execution_time']:.3f}s ({result['row_count']} rows)")
        
        logging.info("\n最快的5個查詢:")
        for i, result in enumerate(sorted_results[-5:], 1):
            logging.info(f"{i}. {result['query_name']}: {result['execution_time']:.3f}s ({result['row_count']} rows)")
        
        # 識別需要優化的查詢
        slow_queries = [r for r in self.test_results if r['execution_time'] > 1.0]
        if slow_queries:
            logging.warning(f"\n發現 {len(slow_queries)} 個慢查詢（>1秒）:")
            for query in slow_queries:
                logging.warning(f"  - {query['query_name']}: {query['execution_time']:.3f}s")

def main():
    """主函數"""
    logging.info("=== 開始查詢性能測試 ===")
    
    tester = QueryPerformanceTester()
    
    try:
        # 測試基本查詢性能
        tester.test_wallet_transaction_queries()
        tester.test_wallet_buy_data_queries()
        
        # 分析查詢計劃
        tester.test_explain_plans()
        
        # 生成報告
        tester.generate_report()
        
        logging.info("=== 性能測試完成 ===")
        
    except Exception as e:
        logging.error(f"性能測試失敗: {str(e)}")
        raise

if __name__ == '__main__':
    main() 