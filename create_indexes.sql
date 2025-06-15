-- =====================================================
-- 錢包交易分析系統 - 索引優化腳本
-- =====================================================

-- 開始索引創建
BEGIN;

-- =====================================================
-- wallet_transaction 表索引
-- =====================================================

-- 1. 錢包地址索引（高頻查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_address 
ON dex_query_v1.wallet_transaction(wallet_address);

-- 2. 代幣地址索引（高頻查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_token_address 
ON dex_query_v1.wallet_transaction(token_address);

-- 3. 鏈索引（用於分區查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_chain 
ON dex_query_v1.wallet_transaction(chain);

-- 4. 複合索引：錢包+代幣+鏈（最常用的組合查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_token_chain 
ON dex_query_v1.wallet_transaction(wallet_address, token_address, chain);

-- 5. 時間索引（按時間範圍查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_time 
ON dex_query_v1.wallet_transaction(time DESC);

-- 6. 交易時間戳索引（用於排序和範圍查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_transaction_time 
ON dex_query_v1.wallet_transaction(transaction_time DESC);

-- 7. 交易類型索引（買入/賣出查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_transaction_type 
ON dex_query_v1.wallet_transaction(transaction_type);

-- 8. 複合索引：錢包+代幣+時間（歷史查詢優化）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_token_time 
ON dex_query_v1.wallet_transaction(wallet_address, token_address, transaction_time DESC);

-- 9. 複合索引：錢包+代幣+鏈+時間（完整歷史查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_full_history 
ON dex_query_v1.wallet_transaction(wallet_address, token_address, chain, transaction_time DESC);

-- 10. 簽名索引（用於去重和查找特定交易）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_signature 
ON dex_query_v1.wallet_transaction(signature);

-- 11. 複合索引：鏈+時間（按鏈分析最近數據）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_chain_time 
ON dex_query_v1.wallet_transaction(chain, time DESC);

-- 12. 複合索引：錢包+時間（用戶活動分析）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_wallet_time 
ON dex_query_v1.wallet_transaction(wallet_address, time DESC);

-- 13. 部分索引：只索引最近30天的數據（節省空間，提高寫入性能）
CREATE INDEX IF NOT EXISTS idx_wallet_transaction_recent_30days 
ON dex_query_v1.wallet_transaction(wallet_address, token_address, transaction_time DESC)
WHERE time >= NOW() - INTERVAL '30 days';

-- =====================================================
-- wallet_buy_data 表索引
-- =====================================================

-- 1. 錢包地址索引
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_wallet_address 
ON dex_query_v1.wallet_buy_data(wallet_address);

-- 2. 代幣地址索引
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_token_address 
ON dex_query_v1.wallet_buy_data(token_address);

-- 3. 鏈索引
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_chain 
ON dex_query_v1.wallet_buy_data(chain);

-- 4. 日期索引（按日期範圍查詢）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_date 
ON dex_query_v1.wallet_buy_data(date DESC);

-- 5. 更新時間索引（查找最近更新的記錄）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_updated_at 
ON dex_query_v1.wallet_buy_data(updated_at DESC);

-- 6. 複合索引：錢包+代幣+鏈（主要查詢模式）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_wallet_token_chain 
ON dex_query_v1.wallet_buy_data(wallet_address, token_address, chain);

-- 7. 複合索引：錢包+日期（用戶每日統計）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_wallet_date 
ON dex_query_v1.wallet_buy_data(wallet_address, date DESC);

-- 8. 複合索引：代幣+日期（代幣每日統計）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_token_date 
ON dex_query_v1.wallet_buy_data(token_address, date DESC);

-- 9. 複合索引：鏈+日期（鏈級別統計）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_chain_date 
ON dex_query_v1.wallet_buy_data(chain, date DESC);

-- 10. 持倉金額索引（用於排序和篩選大戶）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_total_amount 
ON dex_query_v1.wallet_buy_data(total_amount DESC) 
WHERE total_amount > 0;

-- 11. 已實現收益索引（收益分析）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_realized_profit 
ON dex_query_v1.wallet_buy_data(realized_profit DESC);

-- 12. 最後交易時間索引（活躍度分析）
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_last_transaction_time 
ON dex_query_v1.wallet_buy_data(last_transaction_time DESC);

-- 13. 部分索引：只索引有持倉的記錄
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_active_positions 
ON dex_query_v1.wallet_buy_data(wallet_address, token_address, total_amount DESC)
WHERE total_amount > 0;

-- 14. 部分索引：最近7天的數據
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_recent_7days 
ON dex_query_v1.wallet_buy_data(wallet_address, token_address, chain, date DESC)
WHERE date >= CURRENT_DATE - INTERVAL '7 days';

-- =====================================================
-- 分區表索引（如果使用分區）
-- =====================================================

-- 為每個分區創建本地索引（以 SOLANA 分區為例）
-- 注意：這些索引需要根據實際的分區表名調整

-- wallet_buy_data_solana 分區索引
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_solana_wallet_token 
ON dex_query_v1.wallet_buy_data_solana(wallet_address, token_address);

CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_solana_date 
ON dex_query_v1.wallet_buy_data_solana(date DESC);

-- wallet_buy_data_bsc 分區索引
CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_bsc_wallet_token 
ON dex_query_v1.wallet_buy_data_bsc(wallet_address, token_address);

CREATE INDEX IF NOT EXISTS idx_wallet_buy_data_bsc_date 
ON dex_query_v1.wallet_buy_data_bsc(date DESC);

-- =====================================================
-- 統計信息更新
-- =====================================================

-- 更新表統計信息以優化查詢計劃
ANALYZE dex_query_v1.wallet_transaction;
ANALYZE dex_query_v1.wallet_buy_data;

-- 提交所有更改
COMMIT;

-- =====================================================
-- 索引使用建議和監控查詢
-- =====================================================

-- 查看表大小和索引使用情況
/*
-- 檢查表大小
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as data_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables 
WHERE schemaname = 'dex_query_v1' 
AND tablename IN ('wallet_transaction', 'wallet_buy_data')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- 檢查索引使用情況
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes 
WHERE schemaname = 'dex_query_v1'
ORDER BY idx_scan DESC;

-- 查找未使用的索引（運行一段時間後執行）
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes 
WHERE schemaname = 'dex_query_v1'
AND idx_scan = 0
ORDER BY pg_relation_size(indexrelid) DESC;

-- 常用查詢性能測試
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM dex_query_v1.wallet_transaction 
WHERE wallet_address = 'YOUR_WALLET_ADDRESS' 
AND token_address = 'YOUR_TOKEN_ADDRESS' 
ORDER BY transaction_time DESC 
LIMIT 100;

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM dex_query_v1.wallet_buy_data 
WHERE wallet_address = 'YOUR_WALLET_ADDRESS' 
AND chain = 'SOLANA' 
AND date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date DESC;
*/

-- =====================================================
-- 維護建議
-- =====================================================

/*
維護建議：

1. 定期重建索引（每月）：
   REINDEX TABLE dex_query_v1.wallet_transaction;
   REINDEX TABLE dex_query_v1.wallet_buy_data;

2. 定期更新統計信息（每週）：
   ANALYZE dex_query_v1.wallet_transaction;
   ANALYZE dex_query_v1.wallet_buy_data;

3. 監控索引膨脹：
   SELECT 
       schemaname, tablename, indexname,
       pg_size_pretty(pg_relation_size(indexrelid)) as size,
       round(100 * (pg_relation_size(indexrelid)::numeric / 
             NULLIF(pg_relation_size(schemaname||'.'||tablename), 0)), 2) as index_ratio
   FROM pg_stat_user_indexes 
   WHERE schemaname = 'dex_query_v1'
   ORDER BY pg_relation_size(indexrelid) DESC;

4. 清理舊數據時記得同時清理相關索引

5. 高頻寫入場景下，可以考慮：
   - 減少不必要的索引
   - 使用部分索引
   - 批量插入時臨時禁用索引
*/ 