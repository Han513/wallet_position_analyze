# 高頻交易場景性能優化指南

## 📊 性能對比

### 標準版本 vs 高性能版本

| 指標 | 標準版本 | 高性能版本 | 提升倍數 |
|------|----------|------------|----------|
| **理論TPS** | ~100 | ~2000+ | 20x |
| **內存使用** | 低 | 中等 | - |
| **數據庫連接** | 20 | 50-150 | 3-7x |
| **批次大小** | 50 | 200-500 | 4-10x |
| **並發處理** | 單線程 | 多線程 | 8x |
| **緩存策略** | 基本 | 多級緩存 | - |

## 🚀 高性能版本的關鍵優化

### 1. **多級緩衝架構**
```python
# 標準版本
transaction_buffer = []  # 簡單列表

# 高性能版本  
transaction_buffer = deque(maxlen=1000)    # 高效雙端隊列
processing_queue = Queue(maxsize=5000)     # 異步處理隊列
position_cache = defaultdict()             # 內存位置緩存
```

### 2. **並行處理能力**
```python
# 標準版本：順序處理
for message in messages:
    process_message(message)

# 高性能版本：並行處理
futures = []
for message in messages:
    future = executor.submit(process_message, message)
    futures.append(future)
```

### 3. **優化的數據庫操作**
```python
# 高性能配置
engine_hp = engine.execution_options(
    pool_size=50,           # 大幅增加連接池
    max_overflow=100,       # 更多溢出連接
    pool_recycle=1800,      # 更長連接生命週期
    pool_timeout=30         # 適當超時
)
```

### 4. **智能緩存策略**
- **位置緩存**：內存中保存持倉狀態，避免重複查詢
- **交易緩存**：保留最近100筆交易用於快速計算
- **過期清理**：自動清理1小時未更新的緩存

### 5. **簡化計算邏輯**
```python
# 高頻場景下的權衡：
- 跳過SOL餘額查詢（減少外部API調用）
- 簡化wallet_buy_data計算（只計算當日統計）
- 延後詳細統計（可以通過定時任務補充）
```

## ⚙️ 部署配置建議

### 硬件要求

#### 輕量級場景（< 500 TPS）
- **CPU**: 4核心
- **內存**: 8GB
- **網絡**: 100Mbps
- **數據庫**: 中等配置

#### 高性能場景（> 1000 TPS）
- **CPU**: 8核心或以上
- **內存**: 16GB+
- **網絡**: 1Gbps
- **數據庫**: 高性能SSD + 足夠IOPS

### 數據庫調優

```sql
-- PostgreSQL 高性能配置
shared_buffers = '4GB'
effective_cache_size = '12GB' 
work_mem = '256MB'
maintenance_work_mem = '1GB'
checkpoint_completion_target = 0.9
wal_buffers = '64MB'
random_page_cost = 1.1
```

### 監控指標

```python
# 關鍵監控指標
- TPS (每秒交易數)
- 錯誤率
- 數據庫連接使用率
- 內存使用量
- 隊列堆積情況
- 緩存命中率
```

## 🔧 使用方式

### 1. 標準場景（< 100 TPS）
```bash
python analyze_kafka_optimized.py
```

### 2. 高頻場景（> 500 TPS）
```bash
python analyze_kafka_high_performance.py
```

### 3. 性能測試
```python
# 測試腳本
import time
from analyze_kafka_high_performance import HighPerformanceKafkaProcessor

processor = HighPerformanceKafkaProcessor()

# 模擬高頻交易
start_time = time.time()
for i in range(10000):
    fake_trade = {
        'event': {
            'address': f'wallet_{i % 100}',
            'tokenAddress': f'token_{i % 50}',
            'network': 'SOLANA',
            'time': int(time.time() * 1000),
            'side': 'buy' if i % 2 == 0 else 'sell',
            'txnValue': 1000.0,
            'price': 0.1,
            'hash': f'tx_{i}'
        }
    }
    processor._process_single_message(fake_trade)

elapsed = time.time() - start_time
tps = 10000 / elapsed
print(f"測試TPS: {tps:.2f}")
```

## ⚠️ 注意事項

### 權衡考量
1. **準確性 vs 性能**：高頻版本為了性能犧牲了一些計算精度
2. **內存 vs 查詢**：使用更多內存來減少數據庫查詢
3. **實時性 vs 一致性**：可能存在短暫的數據不一致

### 推薦策略
- **< 200 TPS**：使用標準版本
- **200-1000 TPS**：使用高性能版本，定期校驗數據
- **> 1000 TPS**：高性能版本 + 分庫分表 + 讀寫分離

### 數據校驗
建議每天運行一次完整的歷史數據重算來確保數據準確性：
```bash
python analyze_history_optimized.py --verify-mode
``` 