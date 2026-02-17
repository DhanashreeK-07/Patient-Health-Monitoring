def apply_alerts(df):
    return df.filter(
        (df.heart_rate > 120) |
        (df.oxygen_level < 90) |
        (df.temperature > 38)
    )
