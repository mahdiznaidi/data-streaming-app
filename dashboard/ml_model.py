import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler


# ── Anomaly Detection (Isolation Forest) ─────────────────────────────────────
def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects anomalous 1-min windows using Isolation Forest.
    Features: avg_price, trade_count, sum_value, spike_count, price_change_pct
    Adds columns: anomaly (bool), anomaly_score (float)
    """
    if df.empty or len(df) < 5:
        df["anomaly"]       = False
        df["anomaly_score"] = 0.0
        return df

    df = df.copy().sort_values("window_start")

    # Engineered features
    df["price_change_pct"] = df.groupby("symbol")["avg_price"].pct_change().fillna(0) * 100
    df["value_per_trade"]  = df["sum_value"] / df["trade_count"].replace(0, 1)
    if "spike_count" in df.columns:
        df["spike_rate"] = df["spike_count"] / df["trade_count"].replace(0, 1)
    else:
        df["spike_rate"] = 0.0

    features  = ["avg_price", "trade_count", "sum_value",
                 "price_change_pct", "value_per_trade", "spike_rate"]
    available = [f for f in features if f in df.columns]

    X = df[available].fillna(0)
    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
    preds = model.fit_predict(X_scaled)

    df["anomaly"]       = preds == -1
    df["anomaly_score"] = model.decision_function(X_scaled)
    return df


# ── Price Prediction (Linear Regression) ─────────────────────────────────────
def predict_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Predicts next window's avg_price per symbol using Linear Regression.
    Features: rolling means, std, price_change, volume_zscore
    Adds column: predicted_price (float)
    """
    if df.empty or len(df) < 15:
        df["predicted_price"] = df.get("avg_price", pd.Series(dtype=float))
        return df

    result_frames = []

    for symbol, sym_df in df.groupby("symbol"):
        sym_df = sym_df.sort_values("window_start").copy()

        # Feature engineering
        sym_df["rolling_mean_5"]  = sym_df["avg_price"].rolling(5,  min_periods=1).mean()
        sym_df["rolling_mean_10"] = sym_df["avg_price"].rolling(10, min_periods=1).mean()
        sym_df["rolling_std_5"]   = sym_df["avg_price"].rolling(5,  min_periods=1).std().fillna(0)
        sym_df["price_change"]    = sym_df["avg_price"].diff().fillna(0)
        sym_df["price_change_pct"]= sym_df["avg_price"].pct_change().fillna(0) * 100

        vol_mean = sym_df["sum_value"].mean()
        vol_std  = sym_df["sum_value"].std()
        sym_df["volume_zscore"] = (
            (sym_df["sum_value"] - vol_mean) / vol_std if vol_std > 0 else 0.0
        )

        features  = ["rolling_mean_5", "rolling_mean_10", "rolling_std_5",
                     "price_change", "price_change_pct", "volume_zscore"]
        available = [f for f in features if f in sym_df.columns]

        X = sym_df[available].fillna(0)
        y = sym_df["avg_price"].shift(-1)  # target = next window price
        mask = y.notna()

        if mask.sum() < 10:
            sym_df["predicted_price"] = sym_df["avg_price"]
            result_frames.append(sym_df)
            continue

        scaler  = StandardScaler()
        X_train = scaler.fit_transform(X[mask])
        y_train = y[mask]

        model = LinearRegression()
        model.fit(X_train, y_train)

        X_all = scaler.transform(X)
        sym_df["predicted_price"] = model.predict(X_all)
        result_frames.append(sym_df)

    if not result_frames:
        df["predicted_price"] = df["avg_price"]
        return df

    return pd.concat(result_frames, ignore_index=True).sort_values("window_start")
