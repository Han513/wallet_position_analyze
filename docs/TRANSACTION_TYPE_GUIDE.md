# 交易型態分類系統說明

## 概述

錢包交易分析系統已升級為更詳細的交易型態分類，從原來的簡單 'buy'/'sell' 擴展為四種精確的交易行為分類。

## 交易型態定義

### 0: 清倉 (LIQUIDATION)
- **定義**: 持倉賣出全部，即為清倉
- **條件**: 
  - 交易方向為 `sell`
  - 賣出數量 >= 當前持倉數量
- **業務意義**: 投資者完全退出該代幣的投資

### 1: 建倉 (POSITION_OPEN)
- **定義**: 持倉為0時的買入，即為建倉
- **條件**: 
  - 交易方向為 `buy`
  - 當前持倉數量 <= 0 (基本無持倉)
- **業務意義**: 投資者首次進入該代幣的投資

### 2: 加倉 (POSITION_ADD)
- **定義**: 有持倉數據時的買入，即為加倉
- **條件**: 
  - 交易方向為 `buy`
  - 當前持倉數量 > 0
- **業務意義**: 投資者增加對該代幣的投資力度

### 3: 減倉 (POSITION_REDUCE)
- **定義**: 有持倉數據時的部分賣出，即為減倉
- **條件**: 
  - 交易方向為 `sell`
  - 賣出數量 < 當前持倉數量
- **業務意義**: 投資者部分獲利了結或降低風險

## 數據庫存儲

在 `wallet_transaction` 表中，`transaction_type` 欄位現在存儲：
- `0` - 清倉
- `1` - 建倉  
- `2` - 加倉
- `3` - 減倉

## 代碼實現

### 判斷邏輯函數
```python
def determine_transaction_type(side, amount, current_holding):
    """
    判斷交易型態
    
    Args:
        side: 'buy' 或 'sell'
        amount: 交易數量
        current_holding: 當前持倉數量
    
    Returns:
        int: 0=清倉, 1=建倉, 2=加倉, 3=減倉
    """
    if side == 'buy':
        if current_holding <= 1e-9:  # 基本上沒有持倉 (考慮浮點精度)
            return 1  # 建倉
        else:
            return 2  # 加倉
    elif side == 'sell':
        if current_holding <= 1e-9:  # 沒有持倉卻在賣出 (理論上不應該發生)
            return 3  # 減倉
        elif amount >= current_holding - 1e-9:  # 賣出數量 >= 持倉數量
            return 0  # 清倉
        else:
            return 3  # 減倉
    else:
        return 1  # 默認建倉
```

### 修改的文件
1. **analyze_history_optimized.py** - 歷史數據分析
2. **analyze_kafka_optimized.py** - 實時Kafka數據處理

## 業務分析價值

### 投資行為分析
- **建倉**: 識別新的投資機會和市場趨勢
- **加倉**: 分析投資者信心和看好程度
- **減倉**: 觀察獲利了結和風險控制行為
- **清倉**: 追蹤完全退出的時機和原因

### 統計指標
- 建倉成功率
- 加倉頻率分析
- 清倉時機分析
- 持倉週期統計

### 查詢示例

```sql
-- 查看各種交易型態的分佈
SELECT 
    transaction_type,
    CASE transaction_type
        WHEN 0 THEN '清倉'
        WHEN 1 THEN '建倉'
        WHEN 2 THEN '加倉'
        WHEN 3 THEN '減倉'
    END as type_name,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
FROM dex_query_v1.wallet_transaction
GROUP BY transaction_type
ORDER BY transaction_type;

-- 分析某個錢包的交易行為模式
SELECT 
    wallet_address,
    token_address,
    transaction_time,
    transaction_type,
    CASE transaction_type
        WHEN 0 THEN '清倉'
        WHEN 1 THEN '建倉'
        WHEN 2 THEN '加倉'
        WHEN 3 THEN '減倉'
    END as type_name,
    amount,
    price,
    value
FROM dex_query_v1.wallet_transaction
WHERE wallet_address = 'YOUR_WALLET_ADDRESS'
ORDER BY transaction_time DESC;

-- 統計建倉到清倉的週期
WITH position_cycles AS (
    SELECT 
        wallet_address,
        token_address,
        transaction_time,
        transaction_type,
        LAG(transaction_time) OVER (
            PARTITION BY wallet_address, token_address 
            ORDER BY transaction_time
        ) as prev_time,
        LAG(transaction_type) OVER (
            PARTITION BY wallet_address, token_address 
            ORDER BY transaction_time
        ) as prev_type
    FROM dex_query_v1.wallet_transaction
)
SELECT 
    wallet_address,
    token_address,
    (transaction_time - prev_time) / 86400 as holding_days
FROM position_cycles
WHERE transaction_type = 0  -- 清倉
AND prev_type = 1;  -- 上一筆是建倉
```

## 注意事項

1. **浮點精度**: 使用 `1e-9` 作為閾值來處理浮點數精度問題
2. **異常情況**: 防禦性編程處理理論上不應該出現的情況（如無持倉卻賣出）
3. **歷史數據**: 系統會按時間順序重新處理歷史數據，確保交易型態判斷的準確性
4. **性能優化**: 使用內存緩存來跟蹤持倉狀態，避免重複查詢數據庫

## 升級影響

- **數據一致性**: 所有新的交易數據將使用新的分類系統
- **向後兼容**: 現有的查詢需要適應新的數字編碼
- **分析精度**: 提供更精確的投資行為分析能力 