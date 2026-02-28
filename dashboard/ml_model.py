import pandas as pd
from sklearn.ensemble import IsolationForest

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df) < 5:
        df["anomaly"] = False
        return df

    features = ["avg_price", "trade_count", "sum_value"]
    available = [f for f in features if f in df.columns]

    model = IsolationForest(
        contamination=0.1,   # expect ~10% anomalies
        random_state=42,
        n_estimators=100
    )
    X = df[available].fillna(0)
    preds = model.fit_predict(X)          # -1 = anomaly, 1 = normal
    df["anomaly"] = preds == -1
    df["anomaly_score"] = model.decision_function(X)  # lower = more anomalous
    return df