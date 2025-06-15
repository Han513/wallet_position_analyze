#!/bin/bash
set -e

echo "開始執行分析流程..."

# 檢查虛擬環境
if [ ! -d "venv" ]; then
    echo "錯誤：未找到虛擬環境，請先執行 ./deploy.sh"
    exit 1
fi

# 啟動虛擬環境
source venv/bin/activate

# 檢查 .env 文件
if [ ! -f .env ]; then
    echo "錯誤：未找到 .env 文件，請先執行 ./deploy.sh"
    exit 1
fi

# 檢查必要的 Python 文件
if [ ! -f "analyze_history_optimized.py" ] || [ ! -f "analyze_kafka_optimized.py" ]; then
    echo "錯誤：未找到必要的 Python 文件"
    exit 1
fi

# 根據參數決定執行哪個腳本
if [ "$1" = "history" ]; then
    echo "[$(date)] 開始執行歷史數據分析..."
    python analyze_history_optimized.py
elif [ "$1" = "kafka" ]; then
    echo "[$(date)] 開始執行即時數據分析..."
    echo "[DEBUG] 即將執行 analyze_kafka_optimized.py"
    python analyze_kafka_optimized.py
else
    echo "請指定執行模式："
    echo "  ./start.sh history  # 執行歷史數據分析"
    echo "  ./start.sh kafka    # 執行即時數據分析"
    exit 1
fi 