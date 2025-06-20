# 錢包持倉分析系統

本系統用於實時分析鏈上交易數據，計算並更新用戶錢包中代幣的持倉成本、數量、盈虧等信息。

## 功能

-   **實時數據處理**: 通過 Kafka 消費實時交易數據。
-   **歷史數據回補**: 支持手動觸發歷史數據的重新計算。
-   **高性能**: 採用多種優化手段，包括內存緩存、批量處理、並行計算等，以實現高吞吐量。
-   **進程管理**: 內置進程鎖，確保數據處理的單一實例運行。
-   **靈活部署**: 支持通過 Supervisor 進行進程管理和守護。

## 部署步驟

### 1. 執行部署腳本

運行 `deploy.sh` 腳本用來安裝依賴並創建虛擬環境。

```bash
bash deploy.sh
```

該腳本會：
- 檢查並設置 Python 環境。
- 創建一個名為 `venv` 的 Python 虛擬環境。
- 安裝 `requirements.txt` 中列出的所有依賴包。

### 2. 配置環境變量

部署腳本會從 `.env.example` 複製一份環境變量配置文件 `.env`。您需要根據您的實際環境修改此文件。

首先，如果 `.env` 文件不存在，請手動複製：

```bash
cp .env.example .env
```

然後，編輯 `.env` 文件，填寫以下關鍵變量：

```dotenv
# PostgreSQL 數據庫連接信息 (交易歷史，dex_query_v1這個schemas)
DATABASE_Trade

# PostgreSQL 數據庫連接信息 (聰明錢主庫，dex_query_v1這個schemas)
DATABASE_SMARTMONEY

# Redis 配置
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Kafka 配置 (配置生產的kafka)
KAFKA_BROKER=your_kafka_broker:9092
KAFKA_TOPIC=web3_trade_events

# 其他業務配置
CHAIN_IDS=501,9006
SHARD_COUNT=1
SHARD_INDEX=0
```

### 3. 啟動應用程序

使用 `start.sh` 腳本來啟動應用程序。該腳本支持不同的運行模式。

#### 實時數據處理 (Kafka 模式)

這是默認模式，用於連接 Kafka 並處理實時交易數據。

```bash
# 啟動 Kafka 消費者（推薦方式）
bash start.sh kafka

# 或者直接運行（默認進入 Kafka 模式）
bash start.sh
```

#### 歷史數據回補 (History 模式)

如果需要對歷史數據進行重新分析和計算，可以運行此模式。

```bash
bash start.sh history
```

### 5. (推薦) 使用 Supervisor 進行生產環境部署

為了確保程序在後台穩定運行並在意外退出時自動重啟，強烈建議使用 Supervisor 進行管理。


1.  **創建配置文件**:
    將項目中提供的 `run/wallet_position_analyze.conf` 配置文件複製到 Supervisor 的配置目錄中。

    ```bash
    sudo cp run/wallet_position_analyze.conf /etc/supervisor/conf.d/wallet_position_analyze.conf
    ```

2.  **重載配置並啟動**:
    通知 Supervisor 重新加載配置並啟動新的進程。

    ```bash
    sudo supervisorctl reread
    sudo supervisorctl update
    sudo supervisorctl start wallet_position_kafka
    ```

3.  **監控狀態**:
    您可以使用以下命令來查看進程的運行狀態和日誌。

    ```bash
    # 查看進程狀態
    sudo supervisorctl status wallet_position_kafka

    # 實時查看日誌
    tail -f /home/smart_money_wallet_buy_data/logs/wallet_position_kafka.log
    ```