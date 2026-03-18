import streamlit as st
import pandas as pd
import time
import os

st.set_page_config(page_title="Streaming Analytics Dashboard", layout="wide")

st.title("Real-Time Streaming Analytics Dashboard")

data_path = "data/serving/stream"

while True:
    try:
        files = [f for f in os.listdir(data_path) if f.endswith(".parquet")]

        if len(files) == 0:
            st.warning("Waiting for streaming data...")
            time.sleep(5)
            st.rerun()

        df = pd.concat(
            [pd.read_parquet(os.path.join(data_path, f)) for f in files]
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"])

        st.divider()

        col1, col2, col3 = st.columns(3)

        col1.metric("Total Transactions", len(df))
        col2.metric("Total Revenue", int(df["price"].sum()))
        col3.metric("Average Transaction", round(df["price"].mean(), 2))

        st.divider()

        st.subheader("Revenue per City")
        revenue_city = df.groupby("city")["price"].sum().sort_values(ascending=False)
        st.bar_chart(revenue_city)

        st.subheader("Top Products")
        top_products = df.groupby("product")["price"].sum().sort_values(ascending=False)
        st.bar_chart(top_products)

        st.subheader("Revenue Trend")
        revenue_trend = df.set_index("timestamp").resample("10s")["price"].sum()
        st.line_chart(revenue_trend)

        st.divider()

        st.subheader("Live Transactions")
        st.dataframe(
            df.sort_values("timestamp", ascending=False).head(30),
            use_container_width=True
        )

    except Exception as e:
        st.error("Streaming data not ready")
        st.text(str(e))

    time.sleep(5)