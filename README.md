# wallet_position_analyze

## 專案目標
高效、增量地統計每個錢包（wallet_address）在每個代幣（token_address）上的交易數據，並寫入 `wallet_buy_data` 表，減少對資料庫的頻繁查詢，避免重複分析。

## 最新流程架構
1. **查詢已分析過的錢包-代幣-鏈組合**，取得其 `last_transaction_time`。
2. **查詢 trades 表**，找出有新交易的錢包-代幣-鏈組合。
3. **只分析有新交易的組合**，全量分析該組合所有交易紀錄。
4. **分析結果 upsert 到 wallet_buy_data**，確保每組只保留最新統計。
5. **自動處理 chain 欄位**（501→'SOLANA'，9006→'BSC'）。

## 執行方式
1. 設定 `.env` 內容（需有 `DATABASE_Trade` 與 `DATABASE_SMARTMONEY`）。
2. 安裝依賴：
   ```
   pip install -r requirements.txt
   ```
3. 執行分析：
   ```
   python analyze_wallet_position.py
   ```

## 主要資料表
- `dex_query_v1.trades`：存放所有鏈的交易紀錄。
- `wallet_buy_data`：存放每個錢包每個代幣的統計結果。

## 主要欄位
- 參考 `wallet_buy_data` 表設計。

## 注意事項
- 僅分析有新交易的錢包-代幣-鏈組合，支援增量更新。
- 分析結果自動 upsert，避免重複資料。
- 可調整批次大小（BATCH_SIZE）以平衡效能與資料庫壓力。 