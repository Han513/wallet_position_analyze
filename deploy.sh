#!/bin/bash
set -e

echo "開始部署流程..."

# 檢查 pyenv 是否已安裝
if command -v pyenv &> /dev/null; then
    echo "檢測到 pyenv 環境..."
    # 設置 Python 版本
    echo "設置 Python 版本為 3.9.6..."
    pyenv global 3.9.6
    # 確保 PATH 包含 pyenv 的 Python
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
else
    # 檢查 Python 是否已安裝
    if ! command -v python3 &> /dev/null; then
        echo "錯誤：未找到 Python3，請先安裝 Python3"
        exit 1
    fi
fi

# 建立虛擬環境
echo "[1/4] 建立 Python 虛擬環境..."
if [ -d "venv" ]; then
    echo "虛擬環境已存在，正在更新..."
    rm -rf venv
fi

# 使用 python3 創建虛擬環境
python3 -m venv venv

# 啟動虛擬環境
source venv/bin/activate

# 安裝依賴
echo "[2/4] 安裝依賴..."
pip install --upgrade pip
pip install -r requirements.txt

# 複製 .env 範例
echo "[3/4] 複製 .env.example 為 .env (如已存在則略過)"
[ -f .env ] || cp .env.example .env

# 檢查 .env 文件
echo "[4/4] 檢查環境配置..."
if [ ! -f .env ]; then
    echo "錯誤：未找到 .env 文件，請確保 .env.example 存在"
    exit 1
fi

echo "部署完成！"
echo "請確保已正確配置 .env 文件中的以下參數："
echo "- DATABASE_Trade"
echo "- DATABASE_SMARTMONEY"
echo "- REDIS_HOST"
echo "- REDIS_PORT"
echo "- REDIS_DB"
echo "- KAFKA_BROKER"
echo "- KAFKA_TOPIC"
echo "- CHAIN_IDS"
echo "- SHARD_COUNT"
echo "- SHARD_INDEX"
echo ""
echo "配置完成後，請執行 ./start.sh 啟動分析流程" 