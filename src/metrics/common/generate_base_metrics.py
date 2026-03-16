import pandas as pd


def generate_base_metrics(
    df: pd.DataFrame,
    reprocess_window_days: int = 30
) -> pd.DataFrame:

    """
    Gera métricas base aditivas no formato long table.

    Granularidade:
    periodo x periodo_tipo x formulario x tipo_venda x tipo_contato
    x canal_atendimento x origem_contato x indicador
    """

    df = df.copy()

    # =====================================================
    # 1️⃣ FILTRO INCREMENTAL
    # =====================================================

    today = pd.Timestamp.today().normalize()

    start_date = today - pd.Timedelta(days=reprocess_window_days)

    df = df[df["data_contato"] >= start_date]

    # =====================================================
    # 2️⃣ COLUNAS DE PERÍODO
    # =====================================================

    df["periodo_daily"] = df["data_contato"].dt.date

    df["periodo_weekly"] = (
        df["data_contato"]
        .dt.to_period("W")
        .astype(str)
    )

    df["periodo_monthly"] = (
        df["data_contato"]
        .dt.to_period("M")
        .astype(str)
    )

    # =====================================================
    # 3️⃣ DIMENSÕES DO CUBO
    # =====================================================

    dimensions = [
        "formulario",
        "tipo_venda",
        "tipo_contato",
        "canal_atendimento",
        "origem_contato"
    ]

    # =====================================================
    # 4️⃣ FUNÇÃO DE AGREGAÇÃO
    # =====================================================

    def aggregate_metrics(df_group):

        metrics = {}

        # -----------------------------
        # volumes
        # -----------------------------

        metrics["volume_contatos"] = df_group["id_contato"].nunique()

        metrics["volume_contatos_com_pedido"] = df_group[
            df_group["pedido_valido"] == True
        ]["id_contato"].nunique()

        metrics["volume_pedidos"] = df_group[
            df_group["pedido_valido"] == True
        ]["id_pedido"].nunique()

        # -----------------------------
        # retenção bot
        # -----------------------------

        metrics["volume_retido_bot"] = df_group[
            df_group["retido_bot"] == "sim"
        ]["id_contato"].nunique()

        # -----------------------------
        # recontato
        # -----------------------------

        metrics["volume_recontato"] = df_group[
            df_group["flag_recontato"] == "sim"
        ]["id_contato"].nunique()

        metrics["volume_recontato_24h"] = df_group[
            df_group["flag_recontato_24h"] == "sim"
        ]["id_contato"].nunique()

        metrics["volume_sem_recontato"] = df_group[
            df_group["flag_recontato"] == "não"
        ]["id_contato"].nunique()

        # -----------------------------
        # métricas para médias
        # -----------------------------

        valid_recontato = df_group[
            df_group["dias_recontato"].notna()
        ]

        metrics["soma_dias_recontato"] = valid_recontato[
            "dias_recontato"
        ].sum()

        metrics["soma_horas_recontato"] = valid_recontato[
            "tempo_ate_recontato_horas"
        ].sum()

        metrics["volume_recontato_validos"] = valid_recontato[
            "id_contato"
        ].nunique()

        # -----------------------------
        # distribuição recontato
        # -----------------------------

        metrics["volume_recontato_d0"] = df_group[
            df_group["range_recontato"] == "d0"
        ]["id_contato"].nunique()

        metrics["volume_recontato_d3"] = df_group[
            df_group["range_recontato"] == "d3"
        ]["id_contato"].nunique()

        metrics["volume_recontato_d7"] = df_group[
            df_group["range_recontato"] == "d7"
        ]["id_contato"].nunique()

        metrics["volume_recontato_d15"] = df_group[
            df_group["range_recontato"] == "d15"
        ]["id_contato"].nunique()

        metrics["volume_recontato_d30"] = df_group[
            df_group["range_recontato"] == "d30"
        ]["id_contato"].nunique()

        metrics["volume_recontato_d30_plus"] = df_group[
            df_group["range_recontato"] == "d30+"
        ]["id_contato"].nunique()

        return pd.Series(metrics)

    # =====================================================
    # 5️⃣ FUNÇÃO PARA GERAR CUBO POR PERÍODO
    # =====================================================

    def build_period_cube(df, periodo_col, periodo_tipo):

        group_cols = [periodo_col] + dimensions

        aggregated = (
            df
            .groupby(group_cols)
            .apply(aggregate_metrics)
            .reset_index()
        )

        aggregated = aggregated.rename(
            columns={periodo_col: "periodo"}
        )

        aggregated["periodo_tipo"] = periodo_tipo

        # -----------------------------
        # transformar em long format
        # -----------------------------

        cube = aggregated.melt(

            id_vars=[
                "periodo",
                "periodo_tipo",
                *dimensions
            ],

            var_name="indicador",

            value_name="valor"
        )

        return cube

    # =====================================================
    # 6️⃣ GERAR CUBOS
    # =====================================================

    cube_daily = build_period_cube(
        df,
        "periodo_daily",
        "daily"
    )

    cube_weekly = build_period_cube(
        df,
        "periodo_weekly",
        "weekly"
    )

    cube_monthly = build_period_cube(
        df,
        "periodo_monthly",
        "monthly"
    )

    # =====================================================
    # 7️⃣ CONCATENAÇÃO FINAL
    # =====================================================

    metric_cube_base = pd.concat(
        [
            cube_daily,
            cube_weekly,
            cube_monthly
        ],
        ignore_index=True
    )

    return metric_cube_base