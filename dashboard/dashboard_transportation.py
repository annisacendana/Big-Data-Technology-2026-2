import streamlit as st
import time
import sys
import os

# FIX MODULE PATH
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# IMPORT MODULE
from analytics import transportation_analytics as ta
from alerts import transportation_alert as alert

# CONFIG
DATA_PATH = "data/serving/transportation"

st.set_page_config(
    page_title="Smart Transportation Dashboard",
    layout="wide"
)

st.title("Smart Transportation - Real-Time Analytics")

# =========================================
# PERFORMANCE SETTING
# =========================================
REFRESH_INTERVAL = 5

placeholder = st.empty()

# =========================================
# MAIN LOOP (REAL-TIME)
# =========================================
while True:
    with placeholder.container():

        # =========================
        # LOAD DATA
        # =========================
        df = ta.load_data(DATA_PATH)

        if df.empty:
            st.warning("Waiting for streaming transportation data...")
            time.sleep(REFRESH_INTERVAL)
            continue

        # =========================
        # PREPROCESS
        # =========================
        df = ta.preprocess(df)

        # =========================
        # PERFORMANCE OPTIMIZATION
        # =========================
        df = df.tail(1000)  # 🔥 LIMIT DATA

        # =========================
        # METRICS
        # =========================
        try:
            metrics = ta.compute_metrics(df)
        except Exception as e:
            st.error(f"Error computing metrics: {e}")
            time.sleep(REFRESH_INTERVAL)
            continue

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Trips", metrics["total_trips"])
        col2.metric("Total Fare", int(metrics["total_fare"]))
        col3.metric("Top Location", metrics["top_location"])

        st.divider()

        # =========================
        # PEAK HOUR
        # =========================
        try:
            peak_hour = ta.detect_peak_hour(df)
            if peak_hour is not None:
                st.info(f"Peak traffic hour: {peak_hour}:00")
            else:
                st.warning("Peak hour not available")
        except Exception:
            st.warning("Tidak dapat menghitung peak hour")

        st.divider()

        # =========================
        # ALERTS
        # =========================
        try:
            alerts_list = alert.generate_alert(df)
            if alerts_list:
                st.subheader("Traffic Alerts")
                for a in alerts_list:
                    st.error(a)
        except Exception as e:
            st.warning(f"Alert error: {e}")

        st.divider()

        # =========================================
        # VISUALISASI (OPTIMIZED)
        # =========================================
        try:
            st.subheader("Analytics Visualization")

            col1, col2 = st.columns(2)

            # 1. Traffic Density
            with col1:
                st.subheader("Traffic Density (Per Location)")
                st.bar_chart(ta.fare_per_location(df))

            # 2. Vehicle Distribution
            with col2:
                st.subheader("Vehicle Distribution")
                st.bar_chart(ta.vehicle_distribution(df))

            st.divider()

            # 3. Real-Time Traffic Windowed
            st.subheader("Real-Time Traffic (Windowed)")
            traffic_window = ta.traffic_per_window(df)

            if traffic_window is not None and not traffic_window.empty:
                st.line_chart(traffic_window.tail(100))
            else:
                st.info("No traffic data available yet")

            st.divider()

            # 4. Mobility Trend (Downsampled)
            st.subheader("Mobility Trend (Downsampled)")
            df_sample = df.tail(1000)

            if "fare" in df_sample.columns:
                st.line_chart(df_sample["fare"])
            else:
                st.warning("Fare column not available")

        except Exception as e:
            st.warning(f"Visualization error: {e}")

        st.divider()

        # =========================
        # ANOMALY
        # =========================
        try:
            st.subheader("Abnormal Trips")
            anomaly_df = ta.detect_anomaly(df)

            if not anomaly_df.empty:
                st.dataframe(anomaly_df.tail(20))
            else:
                st.success("No anomalies detected")
        except Exception as e:
            st.warning(f"Anomaly error: {e}")

        st.divider()

        # =========================
        # LIVE DATA (LIMITED)
        # =========================
        st.subheader("Live Trip Data")
        st.dataframe(df.tail(50))

        time.sleep(REFRESH_INTERVAL)