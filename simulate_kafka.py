import time
import uuid
import random
from datetime import datetime, timedelta
from analyze_kafka_ultra_fast import UltraFastKafkaProcessor

processor = UltraFastKafkaProcessor()

token_list = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",  # mSOL
    "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",  # ETH
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",  # RAY
    "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",  # SAMO
    "AGFEad2et2ZJif9jaGpdMixQqvW5i81aBdvKe7PHNfz3",  # FTT
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac"   # MNGO
]

user_list = [
    "userA111111111111111111111111111111111111111",
    "userB222222222222222222222222222222222222222",
    "userC333333333333333333333333333333333333333",
    "userD444444444444444444444444444444444444444",
    "userE555555555555555555555555555555555555555",
    "userF666666666666666666666666666666666666666",
    "userG777777777777777777777777777777777777777",
    "userH888888888888888888888888888888888888888",
    "userI999999999999999999999999999999999999999",
    "userJ000000000000000000000000000000000000000"
]

# 生成每個 (user, token) 的最新時間戳
latest_timestamp = {}
def get_ordered_timestamp(user, token):
    key = (user, token)
    now = int(time.time())
    if key not in latest_timestamp:
        # 初始給一個隨機過去7天的時間
        days_ago = random.randint(0, 7)
        random_time = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        latest_timestamp[key] = int(random_time.timestamp())
    else:
        latest_timestamp[key] += 1  # 每次自增1秒
    return latest_timestamp[key]

def random_event():
    user = random.choice(user_list)
    token_address = random.choice(token_list)
    timestamp = get_ordered_timestamp(user, token_address)
    is_buy = random.choice([True, False])
    tx_hash = uuid.uuid4().hex
    base_mint = token_address
    quote_mint = random.choice([t for t in token_list if t != token_address])
    
    # 根據代幣設置合理的價格範圍
    if token_address == token_list[0]:  # SOL
        price_range = (15, 25)
        amount_range = (0.1, 10)
    elif token_address in [token_list[1], token_list[2]]:  # USDC, USDT
        price_range = (0.99, 1.01)
        amount_range = (10, 1000)
    else:  # 其他代幣
        price_range = (0.1, 100)
        amount_range = (1, 100)
    
    price = round(random.uniform(*price_range), 6)
    amount = round(random.uniform(*amount_range), 6)
    
    return {
        "event": {
            "brand": "BYD",
            "eventTime": timestamp,
            "id": uuid.uuid4().hex,
            "network": "SOLANA",
            "tokenAddress": token_address,
            "poolAddress": "pool_" + token_address[:6],
            "h24": round(random.uniform(-10, 10), 2),
            "dex": "dex_" + token_address[:6],
            "time": timestamp,
            "side": "buy" if is_buy else "sell",
            "volumeUsd": round(amount * price, 2),
            "txnValue": amount,
            "fromTokenAmount": amount,
            "toTokenAmount": amount * price,
            "address": user,
            "hash": tx_hash,
            "price": price,
            "priceNav": price * (1 + random.uniform(-0.001, 0.001)),
            "source": "dex",
            "baseMint": base_mint,
            "quoteMint": quote_mint,
            "baseBalance": round(random.uniform(1000, 100000), 6),
            "quoteBalance": round(random.uniform(1000, 100000), 6),
            "timestamp": timestamp,
        },
        "type": "com.zeroex.web3.core.event.data.TradeEvent"
    }

if __name__ == "__main__":
    print("開始模擬推送，每秒100筆，共模擬60秒...")
    print(f"使用 {len(user_list)} 個錢包地址")
    print(f"使用 {len(token_list)} 個代幣地址")
    print("數據跨越過去7天")
    print("請觀察日誌與資料表...")
    
    try:
        for i in range(60):  # 模擬60秒
            for _ in range(100):  # 每秒100筆
                event = random_event()
                processor.process_trade_ultra_fast(event)
            print(f"已處理 {(i+1)*100} 筆交易")
            time.sleep(1)
        print("模擬結束，共產生 6000 筆交易")
    except KeyboardInterrupt:
        print("\n使用者中斷執行")
    finally:
        print("程序結束")