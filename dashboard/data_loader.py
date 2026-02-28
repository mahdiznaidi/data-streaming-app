import pandas as pd
import os
import glob

TUMBLING_PATH = "data/spark_out/tumbling_1m"
SLIDING_PATH  = "data/spark_out/sliding_5m_1m"

def load_parquet(path: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(path, "*.parquet"))
    if not files:
        return pd.DataFrame()
    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    # Extract window start from struct column
    if "window" in df.columns:
        df["window_start"] = df["window"].apply(
            lambda w: w["start"] if isinstance(w, dict) else w.start
        )
        df = df.sort_values("window_start")
    return df

def load_tumbling():
    return load_parquet(TUMBLING_PATH)

def load_sliding():
    return load_parquet(SLIDING_PATH)