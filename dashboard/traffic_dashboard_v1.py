import streamlit as st
import pandas as pd
import joblib
import matplotlib.pyplot as plt

st.set_page_config(page_title="Smart Traffic AI", layout="wide")

st.title("🚦 Smart City Traffic Dashboard")

df = pd.read_csv('data/clean/traffic_smartcity_clean_v1.csv')
model = joblib.load('models/traffic_model_v1.pkl')

df['datetime'] = pd.to_datetime(df['datetime'])
df['hour'] = df['datetime'].dt.hour
df['day'] = df['datetime'].dt.dayofweek
df['lag1'] = df['traffic'].shift(1)
df = df.dropna()

# Grafik
st.subheader("Traffic Trend")
st.line_chart(df['traffic'])

# Input prediksi
st.subheader("Prediksi Traffic")

hour = st.slider("Jam", 0, 23, 12)
day = st.slider("Hari", 0, 6, 1)
lag1 = st.number_input("Traffic sebelumnya", 50, 300, 100)

if st.button("Prediksi"):
    pred = model.predict([[hour, day, lag1]])
    st.success(f"Prediksi: {int(pred[0])} kendaraan")