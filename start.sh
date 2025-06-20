#!/bin/bash
set -e

MODE=${1:-kafka} # 默認模式為 kafka

# 設置日誌目錄
LOG_DIR="/home/smart_money_wallet_buy_data/logs"
mkdir -p $LOG_DIR

# 設置日誌文件
LOG_FILE="$LOG_DIR/wallet_position_kafka.log"
exec 1> >(tee -a "$LOG_FILE")
exec 2>&1

if [ "$MODE" = "kafka" ]; then
    echo "[$(date)] 檢查是否有其他實例在運行..."
    if pgrep -f "python.*analyze_kafka_ultra_fast.py" > /dev/null; then
        echo "[$(date)] 發現正在運行的實例，正在停止..."
        pkill -f "python.*analyze_kafka_ultra_fast.py"
        # 等待進程完全停止
        sleep 5
        if pgrep -f "python.*analyze_kafka_ultra_fast.py" > /dev/null; then
            echo "[$(date)] 進程未能正常停止，強制終止..."
            pkill -9 -f "python.*analyze_kafka_ultra_fast.py"
            sleep 2
        fi
    fi
fi

echo "[$(date)] 開始執行分析流程 (模式: $MODE)..."

# 檢查虛擬環境
if [ ! -d "venv" ]; then
    echo "[$(date)] 錯誤：未找到虛擬環境，請先執行 ./deploy.sh"
    exit 1
fi

# 啟動虛擬環境
source venv/bin/activate

# 檢查 .env 文件
if [ ! -f .env ]; then
    echo "[$(date)] 錯誤：未找到 .env 文件，請先執行 ./deploy.sh"
    exit 1
fi

# 處理 .env 文件格式
echo "[$(date)] 處理 .env 文件格式..."
# 移除 Windows 行尾符並確保變量定義格式正確
sed -i 's/\r$//' .env
sed -i 's/^[[:space:]]*//' .env
sed -i 's/[[:space:]]*$//' .env
sed -i 's/^\([^=]*\)[[:space:]]*=[[:space:]]*/\1=/' .env

# 加載 .env 文件
echo "[$(date)] 加載環境變量..."
declare -A env_vars
while IFS='=' read -r key value; do
    # 跳過空行和註釋
    [[ -z "$key" || "$key" =~ ^# ]] && continue
    # 移除引號
    value=$(echo "$value" | sed -e "s/^['\"]//" -e "s/['\"]$//")
    env_vars["$key"]="$value"
    export "$key=$value"
done < .env

# 驗證必要的環境變量
required_vars=("REDIS_HOST" "REDIS_PORT" "KAFKA_BROKER" "KAFKA_TOPIC")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${env_vars[$var]}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo "[$(date)] 錯誤：以下必要的環境變量未設置："
    printf '%s\n' "${missing_vars[@]}"
    echo "請檢查 .env 文件"
    exit 1
fi

# 輸出環境變量值（不含敏感信息）
echo "[$(date)] 環境變量配置："
sensitive_keys=("PASSWORD" "SECRET" "KEY" "TOKEN" "DATABASE")
for key in "${!env_vars[@]}"; do
    is_sensitive=false
    # 對常見的敏感關鍵字進行不區分大小寫的匹配
    for sensitive in "${sensitive_keys[@]}"; do
        if [[ "${key^^}" == *"$sensitive"* ]]; then
            is_sensitive=true
            break
        fi
    done

    if [ "$is_sensitive" = false ]; then
        echo "$key: ${env_vars[$key]}"
    else
        # 對於敏感信息，只顯示鍵名，值用 [REDACTED] 替代
        echo "$key: [REDACTED]"
    fi
done

# 檢查必要的 Python 文件
if [ ! -f "analyze_kafka_ultra_fast.py" ]; then
    echo "[$(date)] 錯誤：未找到必要的 Python 文件"
    exit 1
fi

# 檢查 Redis 連接
echo "[$(date)] 檢查 Redis 連接..."
redis_cli_base_cmd="redis-cli -h \"${env_vars[REDIS_HOST]}\" -p \"${env_vars[REDIS_PORT]}\""
if [ -n "${env_vars[REDIS_PASSWORD]}" ]; then
    redis_cli_base_cmd="$redis_cli_base_cmd -a \"${env_vars[REDIS_PASSWORD]}\""
fi

if ! eval "$redis_cli_base_cmd ping" > /dev/null 2>&1; then
    echo "[$(date)] 錯誤：無法連接到 Redis 服務器 (${env_vars[REDIS_HOST]}:${env_vars[REDIS_PORT]})"
    exit 1
fi

# 檢查 Kafka 連接
echo "[$(date)] 檢查 Kafka 連接..."
python -c "
import sys
from kafka import KafkaConsumer
try:
    consumer = KafkaConsumer(
        bootstrap_servers='${env_vars[KAFKA_BROKER]}',
        group_id='wallet_position_analyze_group',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=1000,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        request_timeout_ms=60000,
        connections_max_idle_ms=300000
    )
    consumer.topics()
    print('Kafka 連接成功')
    sys.exit(0)
except Exception as e:
    print(f'Kafka 連接失敗: {str(e)}')
    sys.exit(1)
" || {
    echo "[$(date)] 警告：無法連接到 Kafka 服務器 (${env_vars[KAFKA_BROKER]})，但將繼續執行"
}

# 清理 Redis 鎖
echo "[$(date)] 清理 Redis 鎖..."
eval "$redis_cli_base_cmd DEL wallet_position_analyze_lock" > /dev/null 2>&1 || echo "清理 redis lock 失敗，可能 redis-cli 未安裝或連接失敗，但不影響主程序"

# 設置 Python 環境變量
export PYTHONUNBUFFERED=1
export PYTHONIOENCODING=utf-8

# 根據模式執行
case "$MODE" in
    kafka)
        # 創建 PID 文件
        PID_FILE="/tmp/analyze_kafka_ultra_fast.pid"
        echo $$ > "$PID_FILE"
        
        # 執行 Kafka 實時分析程序
        echo "[$(date)] 開始執行即時數據分析..."
        echo "[$(date)] [DEBUG] 即將執行 analyze_kafka_ultra_fast.py"
        echo "[$(date)] [DEBUG] Kafka 配置: ${env_vars[KAFKA_BROKER]}, Topic: ${env_vars[KAFKA_TOPIC]}"

        # 執行 Python 腳本並捕獲錯誤
        if ! python analyze_kafka_ultra_fast.py; then
            echo "[$(date)] 錯誤：Python 腳本執行失敗"
            rm -f "$PID_FILE"
            exit 1
        fi

        # 清理 PID 文件
        rm -f "$PID_FILE"
        ;;
    history)
        echo "[$(date)] 開始執行歷史數據回補..."
        echo "[$(date)] [DEBUG] 即將執行 analyze_history.py"
        
        if ! python analyze_history.py; then
            echo "[$(date)] 錯誤：歷史回補腳本執行失敗"
            exit 1
        fi
        ;;
    *)
        echo "[$(date)] 錯誤：未知的模式 '$MODE'。請使用 'kafka' 或 'history'。"
        exit 1
        ;;
esac

echo "[$(date)] 模式 '$MODE' 執行完畢。" 