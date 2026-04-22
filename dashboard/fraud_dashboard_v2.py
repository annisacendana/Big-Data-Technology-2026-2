import streamlit as st
import pandas as pd
import os
import time

st.title("🚨 Real-Time Fraud Detection Dashboard")

path = "stream_data/realtime_output"

# auto refresh
time.sleep(2)

if not os.path.exists(path):
    st.warning("Belum ada data streaming...")
    st.stop()

try:
    df = pd.read_parquet(path)
except:
    st.warning("Data belum tersedia")
    st.stop()

st.metric("Total Transaksi", len(df))

if "status" in df.columns:
    fraud_count = len(df[df["status"] == "FRAUD"])
    st.metric("Total Fraud", fraud_count)

    st.bar_chart(df["status"].value_counts())
else:
    st.warning("Kolom 'status' belum tersedia")

st.dataframe(df.tail(10))