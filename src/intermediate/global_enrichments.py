import pandas as pd


# =====================================================
# JOIN COM VENDAS + VALIDAÇÃO DE PEDIDO
# =====================================================

def join_sales_data(
    contacts: pd.DataFrame,
    vendas: pd.DataFrame
) -> pd.DataFrame:

    df = contacts.merge(
        vendas,
        how="left",
        on="id_pedido"
    )

    # pedido válido apenas quando encontrado na base de vendas
    df["pedido_valido"] = df["data_compra_cliente"].notna()

    return df


# =====================================================
# FEATURES TEMPORAIS DO CONTATO
# =====================================================

def add_time_features(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df["hora_contato"] = df["data_contato"].dt.hour
    df["dia_contato"] = df["data_contato"].dt.day
    df["mes_contato"] = df["data_contato"].dt.month
    df["ano_contato"] = df["data_contato"].dt.year

    df["semana_contato"] = (
        df["data_contato"]
        .dt.isocalendar()
        .week
    )

    df["dia_semana"] = df["data_contato"].dt.dayofweek

    return df


# =====================================================
# RECONTATO
# =====================================================

def add_recontact_features(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    group_cols = ["id_pedido", "formulario"]

    # ordenar cronologicamente
    df = df.sort_values(group_cols + ["data_contato"])

    # ordem do contato
    df["ordem_contato_pedido"] = (
        df.groupby(group_cols)
        .cumcount() + 1
    )

    # primeiro contato
    df["primeiro_contato_pedido"] = (
        df["ordem_contato_pedido"] == 1
    ).map({True: "sim", False: "não"})

    # total de contatos
    df["total_contatos"] = (
        df.groupby(group_cols)["id_contato"]
        .transform("count")
    )

    # contatos posteriores
    df["contatos_posteriores"] = (
        df["total_contatos"] - df["ordem_contato_pedido"]
    )

    # próximo contato
    df["next_data_contato"] = (
        df.groupby(group_cols)["data_contato"]
        .shift(-1)
    )

    df["origem_recontato"] = (
        df.groupby(group_cols)["origem_contato"]
        .shift(-1)
    )

    df["canal_recontato"] = (
        df.groupby(group_cols)["canal_atendimento"]
        .shift(-1)
    )

    # flag recontato
    df["flag_recontato"] = df["next_data_contato"].notna()
    df["flag_recontato"] = df["flag_recontato"].map({
        True: "sim",
        False: "não"
    })

    # dias até próximo contato
    df["dias_recontato"] = (
        df["next_data_contato"] - df["data_contato"]
    ).dt.days

    # horas até próximo contato
    df["tempo_ate_recontato_horas"] = (
        df["next_data_contato"] - df["data_contato"]
    ).dt.total_seconds() / 3600

    # flag 24h
    df["flag_recontato_24h"] = (
        df["dias_recontato"] <= 1
    ).map({True: "sim", False: "não"})

    df.loc[df["flag_recontato"] == "não", "flag_recontato_24h"] = "não"

    # =====================================================
    # RANGE RECONTATO
    # =====================================================

    df["range_recontato"] = "não houve recontato"

    mask = df["dias_recontato"].notna()

    df.loc[mask & (df["dias_recontato"] == 0), "range_recontato"] = "d0"

    df.loc[
        mask & (df["dias_recontato"].between(1, 3)),
        "range_recontato"
    ] = "d3"

    df.loc[
        mask & (df["dias_recontato"].between(4, 7)),
        "range_recontato"
    ] = "d7"

    df.loc[
        mask & (df["dias_recontato"].between(8, 15)),
        "range_recontato"
    ] = "d15"

    df.loc[
        mask & (df["dias_recontato"].between(16, 30)),
        "range_recontato"
    ] = "d30"

    df.loc[
        mask & (df["dias_recontato"] > 30),
        "range_recontato"
    ] = "d30+"

    # =====================================================
    # TRATAR PEDIDOS INVÁLIDOS
    # =====================================================

    invalid_mask = ~df["pedido_valido"]

    cols_indef = [
        "flag_recontato",
        "dias_recontato",
        "tempo_ate_recontato_horas",
        "range_recontato",
        "flag_recontato_24h",
        "origem_recontato",
        "canal_recontato"
    ]

    df.loc[invalid_mask, cols_indef] = "indefinido"

    df = df.drop(columns=["next_data_contato"])

    return df


# =====================================================
# SAFRA DA VENDA
# =====================================================

def add_sales_cohort(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df["safra_venda_week"] = (
        df["data_compra_cliente"]
        .dt.strftime("%Y-W%U")
    )

    df["safra_venda_month"] = (
        df["data_compra_cliente"]
        .dt.strftime("%Y-%m")
    )

    return df

# =====================================================
# GRUPO VENDA
# =====================================================

def add_grupo_venda(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    mm_empresas = [
        "mm",
        "mm_app",
        "guide shop",
        "casatema"
    ]

    df["grupo_venda"] = df["empresa_venda"].isin(mm_empresas)

    df["grupo_venda"] = df["grupo_venda"].map({
        True: "mm",
        False: "marketplaces"
    })

    # se não houver venda (pedido inválido)
    df.loc[~df["pedido_valido"], "grupo_venda"] = "indefinido"

    return df


# =====================================================
# JORNADA DE ATENDIMENTO
# =====================================================

def add_jornada_atendimento(df: pd.DataFrame) -> pd.DataFrame:

    df = df.sort_values(["id_pedido", "data_contato"])

    jornada = (
        df.groupby("id_pedido")["formulario"]
        .apply(lambda x: " > ".join(x.astype(str)))
        .rename("jornada_atendimento")
    )

    # se não houver venda (pedido inválido)
    df.loc[~df["pedido_valido"], "jornada_atendimento"] = "indefinido"

    df = df.merge(jornada, on="id_pedido", how="left")

    return df


# =====================================================
# JORNADA ÚNICA
# =====================================================

def add_jornada_atendimento_unica(df: pd.DataFrame) -> pd.DataFrame:

    df = df.sort_values(["id_pedido", "data_contato"])

    def build_unique(series):

        seq = []

        for s in series:
            if not seq or s != seq[-1]:
                seq.append(s)

        return " > ".join(seq)

    jornada = (
        df.groupby("id_pedido")["formulario"]
        .apply(build_unique)
        .rename("jornada_atendimento_unica")
    )

    # se não houver venda (pedido inválido)
    df.loc[~df["pedido_valido"], "jornada_atendimento_unica"] = "indefinido"

    df = df.merge(jornada, on="id_pedido", how="left")

    return df


# =====================================================
# TIPO CONTATO (PLACEHOLDER)
# =====================================================

def add_tipo_contato(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    df["tipo_contato"] = pd.NA

    return df


# =====================================================
# PIPELINE GLOBAL
# =====================================================

def apply_global_enrichments(
    contacts: pd.DataFrame,
    vendas_stg: pd.DataFrame
) -> pd.DataFrame:

    df = contacts.copy()

    df = join_sales_data(df, vendas_stg)

    df = add_time_features(df)

    df = add_recontact_features(df)

    df = add_sales_cohort(df)

    df = add_grupo_venda(df)

    df = add_jornada_atendimento(df)

    df = add_jornada_atendimento_unica(df)

    df = add_tipo_contato(df)

    return df