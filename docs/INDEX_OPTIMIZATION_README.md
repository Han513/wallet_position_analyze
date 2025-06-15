# 數據庫索引優化指南

## 概述

此文檔提供錢包交易分析系統的數據庫索引優化策略，目標是提升查詢性能並減少數據庫負載。

## 索引創建步驟

### 1. 執行索引創建腳本

```bash
# 連接到你的PostgreSQL數據庫
psql -h your_host -U your_user -d your_database -f create_indexes.sql
```

### 2. 驗證索引創建

```sql
-- 檢查索引是否成功創建
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE schemaname = 'dex_query_v1' 
ORDER BY tablename, indexname;
```

## 主要優化策略

### 🚀 高頻查詢優化

#### wallet_transaction 表

1. **錢包地址查詢** (最高頻)
   ```sql
   -- 索引: idx_wallet_transaction_wallet_address
   SELECT * FROM wallet_transaction WHERE wallet_address = ?
   ```

2. **錢包+代幣組合查詢** (高頻)
   ```sql
   -- 索引: idx_wallet_transaction_wallet_token_chain
   SELECT * FROM wallet_transaction 
   WHERE wallet_address = ? AND token_address = ? AND chain = ?
   ```

3. **時間範圍查詢** (高頻)
   ```sql
   -- 索引: idx_wallet_transaction_time, idx_wallet_transaction_transaction_time
   SELECT * FROM wallet_transaction 
   WHERE time >= NOW() - INTERVAL '24 hours'
   ```

#### wallet_buy_data 表

1. **持倉查詢** (最高頻)
   ```sql
   -- 索引: idx_wallet_buy_data_wallet_token_chain
   SELECT * FROM wallet_buy_data 
   WHERE wallet_address = ? AND token_address = ? AND chain = ?
   ```

2. **活躍持倉查詢**
   ```sql
   -- 索引: idx_wallet_buy_data_active_positions (部分索引)
   SELECT * FROM wallet_buy_data 
   WHERE total_amount > 0
   ```

### 📊 性能優化重點

#### 1. 複合索引順序優化

索引列的順序對查詢性能至關重要：

```sql
-- ✅ 推薦：高選擇性字段在前
CREATE INDEX idx_optimal ON table_name (high_selectivity_col, low_selectivity_col);

-- ❌ 避免：低選擇性字段在前
CREATE INDEX idx_suboptimal ON table_name (low_selectivity_col, high_selectivity_col);
```

#### 2. 部分索引使用

節省存儲空間並提高性能：

```sql
-- 只索引活躍數據
CREATE INDEX idx_recent_data ON wallet_transaction (wallet_address, token_address)
WHERE time >= NOW() - INTERVAL '30 days';

-- 只索引有持倉的記錄
CREATE INDEX idx_active_positions ON wallet_buy_data (wallet_address, token_address)
WHERE total_amount > 0;
```

#### 3. 覆蓋索引策略

包含查詢所需的所有列：

```sql
-- 包含常用查詢字段
CREATE INDEX idx_covering ON wallet_transaction 
(wallet_address, token_address, transaction_time) 
INCLUDE (amount, price, transaction_type);
```

## 預期性能提升

### 查詢性能改善

| 查詢類型 | 優化前 | 優化後 | 提升幅度 |
|---------|-------|-------|---------|
| 錢包交易查詢 | 2-5秒 | 50-200ms | **10-25x** |
| 持倉查詢 | 1-3秒 | 20-100ms | **15-30x** |
| 時間範圍查詢 | 5-10秒 | 100-500ms | **20-50x** |
| 聚合統計查詢 | 10-30秒 | 1-3秒 | **10-15x** |

### 系統負載改善

- **CPU使用率**: 降低30-50%
- **I/O操作**: 減少60-80%
- **內存使用**: 更高效的緩存利用
- **並發能力**: 提升3-5倍

## 監控和維護

### 1. 性能監控

```sql
-- 監控索引使用率
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as "索引掃描次數",
    idx_tup_read as "索引讀取行數",
    idx_tup_fetch as "索引獲取行數"
FROM pg_stat_user_indexes 
WHERE schemaname = 'dex_query_v1'
ORDER BY idx_scan DESC;
```

### 2. 識別無用索引

```sql
-- 查找未使用的索引
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes 
WHERE schemaname = 'dex_query_v1'
AND idx_scan = 0
ORDER BY pg_relation_size(indexrelid) DESC;
```

### 3. 定期維護任務

#### 每週維護
```sql
-- 更新統計信息
ANALYZE dex_query_v1.wallet_transaction;
ANALYZE dex_query_v1.wallet_buy_data;
```

#### 每月維護
```sql
-- 重建索引（在低峰期執行）
REINDEX TABLE dex_query_v1.wallet_transaction;
REINDEX TABLE dex_query_v1.wallet_buy_data;
```

### 4. 索引監控查詢

```sql
-- 檢查索引大小和使用情況
SELECT 
    t.schemaname,
    t.tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch,
    round(100.0 * idx_scan / NULLIF(seq_scan + idx_scan, 0), 2) as index_usage_pct
FROM pg_stat_user_indexes s
JOIN pg_stat_user_tables t ON s.relid = t.relid
WHERE t.schemaname = 'dex_query_v1'
ORDER BY pg_relation_size(indexrelid) DESC;
```

## 高頻場景優化建議

### 🔥 實時Kafka處理優化

對於高頻的Kafka數據處理：

1. **批量操作優化**
   ```python
   # 使用批量插入而非單條插入
   bulk_insert_size = 100  # 推薦批次大小
   ```

2. **索引策略**
   ```sql
   -- 關鍵索引：支持快速查重和查詢
   CREATE INDEX idx_kafka_lookup ON wallet_transaction 
   (signature, wallet_address, transaction_time);
   ```

### 📈 歷史數據分析優化

對於大量歷史數據處理：

1. **分區表策略**
   ```sql
   -- 按時間分區
   CREATE TABLE wallet_transaction_2025_01 PARTITION OF wallet_transaction
   FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
   ```

2. **數據歸檔策略**
   ```sql
   -- 歸檔90天前的數據
   CREATE TABLE wallet_transaction_archive AS 
   SELECT * FROM wallet_transaction 
   WHERE time < NOW() - INTERVAL '90 days';
   ```

## 故障排除

### 1. 索引未生效

**症狀**: 查詢仍然很慢
**檢查方法**:
```sql
EXPLAIN (ANALYZE, BUFFERS) 
SELECT * FROM wallet_transaction WHERE wallet_address = 'xxx';
```

**可能原因**:
- 統計信息過期 → 執行 `ANALYZE`
- 查詢條件不匹配索引 → 調整WHERE條件
- 數據量太小 → PostgreSQL選擇全表掃描

### 2. 索引膨脹

**症狀**: 索引占用空間過大
**檢查方法**:
```sql
SELECT 
    schemaname, tablename, indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as size
FROM pg_stat_user_indexes 
WHERE schemaname = 'dex_query_v1'
ORDER BY pg_relation_size(indexrelid) DESC;
```

**解決方案**:
```sql
REINDEX INDEX problematic_index_name;
```

### 3. 寫入性能下降

**症狀**: INSERT/UPDATE變慢
**原因**: 索引過多導致寫入開銷
**解決方案**:
- 刪除未使用的索引
- 使用部分索引替代全量索引
- 考慮在批量導入時臨時禁用索引

## 最佳實踐總結

### ✅ 推薦做法

1. **索引設計**
   - 基於實際查詢模式設計索引
   - 優先考慮複合索引
   - 使用部分索引節省空間

2. **維護策略**
   - 定期監控索引使用率
   - 及時清理無用索引
   - 定期更新統計信息

3. **性能測試**
   - 索引創建前後對比測試
   - 使用 EXPLAIN ANALYZE 分析查詢計劃
   - 監控系統資源使用

### ❌ 避免的做法

1. **過度索引**
   - 不要為每個列都創建索引
   - 避免重複或冗余的索引

2. **忽略維護**
   - 不定期清理導致索引膨脹
   - 統計信息過期影響查詢優化

3. **盲目創建**
   - 不基於實際查詢需求
   - 不考慮寫入性能影響

## 聯繫和支持

如果在索引優化過程中遇到問題，請：

1. 先檢查本文檔的故障排除章節
2. 使用性能測試腳本進行對比分析
3. 記錄具體的查詢語句和EXPLAIN結果

---

**注意**: 在生產環境中執行索引創建時，建議選擇業務低峰期，並做好數據備份。 