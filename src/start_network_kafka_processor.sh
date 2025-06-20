#!/bin/bash
set -e

# 1. 載入 .env 變數
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# 2. 檢查 temp_wallet_buy_data 是否存在，若不存在則創建
python3 <<EOF
import os
from sqlalchemy import create_engine, text

db_url = os.getenv('DATABASE_SMARTMONEY')
engine = create_engine(db_url)
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'dex_query_v1' 
            AND table_name = 'temp_wallet_buy_data'
        )
    """))
    exists = result.scalar()
    if not exists:
        print('Table temp_wallet_buy_data 不存在，自動創建...')
        conn.execute(text("""
CREATE TABLE dex_query_v1.temp_wallet_buy_data (
    wallet_address character varying(255) NOT NULL,
    token_address character varying(255) NOT NULL,
    chain character varying(50) NOT NULL,
    date date NOT NULL,
    total_amount numeric(40, 18) NOT NULL DEFAULT 0,
    total_cost numeric(40, 18) NOT NULL DEFAULT 0,
    avg_buy_price numeric(40, 18) NOT NULL DEFAULT 0,
    position_opened_at bigint NOT NULL DEFAULT 0,
    historical_total_buy_amount numeric(40, 18) NOT NULL DEFAULT 0,
    historical_total_buy_cost numeric(40, 18) NOT NULL DEFAULT 0,
    historical_total_sell_amount numeric(40, 18) NOT NULL DEFAULT 0,
    historical_total_sell_value numeric(40, 18) NOT NULL DEFAULT 0,
    historical_avg_buy_price numeric(40, 18) NOT NULL DEFAULT 0,
    historical_avg_sell_price numeric(40, 18) NOT NULL DEFAULT 0,
    total_buy_count integer NOT NULL DEFAULT 0,
    total_sell_count integer NOT NULL DEFAULT 0,
    total_holding_seconds bigint NOT NULL DEFAULT 0,
    last_transaction_time bigint NOT NULL DEFAULT 0,
    realized_profit numeric(40, 18) NOT NULL DEFAULT 0,
    realized_profit_percentage numeric(40, 18) NOT NULL DEFAULT 0,
    chain_id integer NOT NULL DEFAULT 0,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (wallet_address, token_address, chain, date)
);
        """))
        print('Table temp_wallet_buy_data 已創建')
    else:
        print('Table temp_wallet_buy_data 已存在')
EOF

# 3. 啟動 network_kafka_processor.py
python3 src/network_kafka_processor.py 