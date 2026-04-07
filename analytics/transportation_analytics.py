import pandas as pd
import os


# ==========================
# LOAD DATA
# ==========================
def load_data(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]

    if not files:
        return pd.DataFrame()
    
    df = pd.concat(
        [pd.read_parquet(os.path.join(path, f)) for f in files],
        ignore_index=True
    )

    return df


# =============================
# PREPROCESS
# =============================
def preprocess(df):
    if df.empty:
        return df
    
    if "timestamp" not in df.columns:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    return df


# ===================================
# METRICS
# ===================================
def compute_metrics(df):
    if df.empty:
        return {
            "total_trips": 0,
            "total_fare": 0,
            "top_location": "-"
        }

    total_fare = df["fare"].sum() if "fare" in df.columns else 0

    top_location = "-"
    if "location" in df.columns and "fare" in df.columns:
        if not df.groupby("location")["fare"].sum().empty:
            top_location = df.groupby("location")["fare"].sum().idxmax()

    return {
        "total_trips": len(df),
        "total_fare": total_fare,
        "top_location": top_location
    }


# ========================================
# PEAK HOUR
# ========================================
def detect_peak_hour(df):
    if df.empty or "timestamp" not in df.columns:
        return None

    df["hour"] = df["timestamp"].dt.hour

    if df["hour"].empty:
        return None

    return df.groupby("hour").size().idxmax()


# =========================================
# VISUALIZATION DATA
# =========================================
def fare_per_location(df):
    if df.empty or "location" not in df.columns or "fare" not in df.columns:
        return pd.Series(dtype=float)
    
    return df.groupby("location")["fare"].sum().sort_values(ascending=False)


def vehicle_distribution(df):
    if df.empty or "vehicle_type" not in df.columns:
        return pd.Series(dtype=float)
    
    return df.groupby("vehicle_type").size().sort_values(ascending=False)


def mobility_trend(df):
    if df.empty or "timestamp" not in df.columns or "fare" not in df.columns:
        return pd.Series(dtype=float)
    
    df = df.set_index("timestamp")

    return df["fare"].resample("10s").sum()


# =======================================
# ANOMALY DETECTION
# =======================================
def detect_anomaly(df):
    if df.empty or "fare" not in df.columns:
        return pd.DataFrame()
    
    # fare tinggi dianggap anomali
    return df[df["fare"] > 80000]


# =======================================
# TRAFFIC PER WINDOW
# =======================================
def traffic_per_window(df): 
    if df.empty or "timestamp" not in df.columns: 
        return None 
     
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce") 
    df = df.dropna(subset=["timestamp"])
     
    return (
        df.set_index("timestamp")
          .resample("1min")
          .size()
    )