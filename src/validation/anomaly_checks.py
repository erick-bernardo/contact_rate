import pandas as pd


def detect_volume_anomaly(
    df: pd.DataFrame,
    date_col: str,
    id_col: str,
    freq: str = "W",
    history_window: int = 6,
    z_threshold: float = 3
):
    """
    Detecta anomalia de volume usando Z-score.

    Parameters
    ----------
    df : DataFrame
    date_col : coluna de data
    id_col : coluna identificadora (ex: id_contato)
    freq : frequência ('D','W','M')
    history_window : número de períodos históricos
    z_threshold : limite de anomalia
    """

    df = df.copy()

    df[date_col] = pd.to_datetime(df[date_col])

    today = pd.Timestamp.today().normalize()
    ref_day = today - pd.Timedelta(days=1)

    # ==========================================
    # DAILY (mesmo dia da semana)
    # ==========================================

    if freq == "D":

        weekday = ref_day.weekday()

        df = df[df[date_col].dt.weekday == weekday]

        volume_series = (
            df.groupby(df[date_col].dt.date)[id_col]
            .nunique()
            .sort_index()
        )

        if ref_day.date() not in volume_series.index:
            print("⚠ Não há dados para o dia de referência")
            return volume_series

        current = volume_series.loc[ref_day.date()]

        historical = volume_series.drop(ref_day.date()).tail(history_window)

    # ==========================================
    # WEEKLY (WTD)
    # ==========================================

    elif freq == "W":

        start_week = ref_day - pd.Timedelta(days=ref_day.weekday())

        current = df[
            (df[date_col] >= start_week) &
            (df[date_col] <= ref_day)
        ][id_col].nunique()

        historical_volumes = []

        for i in range(1, history_window + 1):

            start_hist = start_week - pd.Timedelta(weeks=i)
            end_hist = start_hist + (ref_day - start_week)

            hist = df[
                (df[date_col] >= start_hist) &
                (df[date_col] <= end_hist)
            ]

            historical_volumes.append(hist[id_col].nunique())

        historical = pd.Series(historical_volumes)

        volume_series = historical

    # ==========================================
    # MONTHLY (MTD)
    # ==========================================

    elif freq == "M":

        start_month = ref_day.replace(day=1)

        current = df[
            (df[date_col] >= start_month) &
            (df[date_col] <= ref_day)
        ][id_col].nunique()

        historical_volumes = []

        for i in range(1, history_window + 1):

            hist_month = start_month - pd.DateOffset(months=i)
            end_hist = hist_month + (ref_day - start_month)

            hist = df[
                (df[date_col] >= hist_month) &
                (df[date_col] <= end_hist)
            ]

            historical_volumes.append(hist[id_col].nunique())

        historical = pd.Series(historical_volumes)

        volume_series = historical

    else:
        raise ValueError("freq deve ser 'D', 'W' ou 'M'")

    # ==========================================
    # Z-SCORE
    # ==========================================

    if len(historical) < history_window:
        print("⚠ Histórico insuficiente para análise")
        return volume_series

    mean = historical.mean()
    std = historical.std()

    if std == 0:
        print("⚠ Desvio padrão zero — não é possível avaliar anomalia")
        return volume_series

    z_score = (current - mean) / std

    # intervalo esperado
    lower_bound = mean - (z_threshold * std)
    upper_bound = mean + (z_threshold * std)

    print("\n=== Volume Anomaly Check ===")
    print(f"Volume atual: {current}")
    print(f"Média histórica: {mean:.2f}")
    print(f"Desvio padrão: {std:.2f}")
    print(f"Expected range: {lower_bound:.0f} — {upper_bound:.0f}")
    print(f"Z-score: {z_score:.2f}")

    if abs(z_score) > z_threshold:
        print(f"🚨 Anomalia detectada no volume (Z-score={z_score:.2f})")

    elif abs(z_score) > 2:
        print("⚠ Volume fora do padrão esperado")

    else:
        print("✔ Volume dentro da normalidade")

    return volume_series