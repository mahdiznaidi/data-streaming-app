import pandas as pd
import os
import glob

TUMBLING_PATH = "data/spark_out/tumbling_1m"
SLIDING_PATH  = "data/spark_out/sliding_5m_1m"


def load_parquet(path: str) -> pd.DataFrame:
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df  = pd.concat(dfs, ignore_index=True)

    # window_start is already a proper column from Spark — just parse it
    if "window_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["window_start"], utc=True, errors="coerce")
    if "window_end" in df.columns:
        df["window_end"] = pd.to_datetime(df["window_end"], utc=True, errors="coerce")

    # Remove duplicate rows (same symbol + window_start)
    if "symbol" in df.columns and "window_start" in df.columns:
        df = df.drop_duplicates(subset=["symbol", "window_start"])

    df = df.sort_values("window_start").reset_index(drop=True)
    return df


def load_tumbling() -> pd.DataFrame:
    return load_parquet(TUMBLING_PATH)


def load_sliding() -> pd.DataFrame:
    return load_parquet(SLIDING_PATH)
