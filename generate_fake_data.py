import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

np.random.seed(42)

base_path = "data/spark_out"
os.makedirs(f"{base_path}/tumbling_1m", exist_ok=True)
os.makedirs(f"{base_path}/sliding_5m_1m", exist_ok=True)

symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
start = datetime.now() - timedelta(minutes=60)

rows_tumbling = []
rows_sliding = []

for i in range(60):
    current_time = start + timedelta(minutes=i)
    for sym in symbols:
        base_price = 50000 if sym == "BTCUSDT" else 3000
        avg_price = np.random.normal(base_price, 200)
        trade_count = np.random.randint(50, 200)
        sum_value = trade_count * avg_price

        rows_tumbling.append({
            "window_start": current_time,
            "window_end": current_time + timedelta(minutes=1),
            "symbol": sym,
            "trade_count": trade_count,
            "avg_price": avg_price,
            "sum_qty": np.random.randint(100, 500),
            "sum_value": sum_value,
            "spike_count": np.random.randint(0, 5),
            "event_date": current_time.date()
        })

        rows_sliding.append({
            "window_start": current_time,
            "window_end": current_time + timedelta(minutes=5),
            "symbol": sym,
            "trade_count": trade_count,
            "avg_price": avg_price,
            "min_price": avg_price - np.random.uniform(50, 100),
            "max_price": avg_price + np.random.uniform(50, 100),
            "sum_value": sum_value,
            "event_date": current_time.date()
        })

tumbling_df = pd.DataFrame(rows_tumbling)
sliding_df = pd.DataFrame(rows_sliding)

tumbling_df.to_parquet(f"{base_path}/tumbling_1m/data.parquet", index=False)
sliding_df.to_parquet(f"{base_path}/sliding_5m_1m/data.parquet", index=False)

print("Fake parquet files generated successfully.")