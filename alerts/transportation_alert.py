def generate_alert(df):
    alerts = []

    # kalau kosong, langsung return
    if df.empty:
        return alerts

    # alert jumlah data
    if len(df) > 100:
        alerts.append("High traffic volume")

    # alert fare tinggi
    if "fare" in df.columns:
        if df["fare"].max() > 90000:
            alerts.append("High fare detected")

    return alerts