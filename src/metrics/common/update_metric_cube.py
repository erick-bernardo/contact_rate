import pandas as pd

from src.metrics.common.generate_base_metrics import generate_base_metrics


def update_metric_cube(
    client_contact_rate: pd.DataFrame,
    existing_cube: pd.DataFrame | None = None,
    reprocess_window_days: int = 30
) -> pd.DataFrame:

    """
    Atualiza o metric_cube_base de forma incremental.

    Parameters
    ----------
    client_contact_rate : DataFrame base de contatos
    existing_cube : cubo existente (se houver)
    reprocess_window_days : janela de recálculo

    Returns
    -------
    metric_cube_base atualizado
    """

    # =====================================================
    # 1️⃣ GERAR NOVO CUBO PARA JANELA INCREMENTAL
    # =====================================================

    new_cube = generate_base_metrics(
        client_contact_rate,
        reprocess_window_days=reprocess_window_days
    )

    # =====================================================
    # 2️⃣ SE NÃO EXISTIR CUBO ANTIGO
    # =====================================================

    if existing_cube is None or existing_cube.empty:

        return new_cube

    existing_cube = existing_cube.copy()

    # =====================================================
    # 3️⃣ DEFINIR PERÍODO DE RECÁLCULO
    # =====================================================

    today = pd.Timestamp.today().normalize()

    reprocess_start = today - pd.Timedelta(days=reprocess_window_days)

    # converter periodo daily para datetime para filtro seguro

    existing_cube["periodo_date"] = pd.to_datetime(
        existing_cube["periodo"],
        errors="coerce"
    )

    # =====================================================
    # 4️⃣ REMOVER PERÍODOS QUE SERÃO RECALCULADOS
    # =====================================================

    historical_cube = existing_cube[
        (existing_cube["periodo_tipo"] == "daily")
        & (existing_cube["periodo_date"] < reprocess_start)
    ]

    historical_cube = historical_cube.drop(columns=["periodo_date"])

    # manter weekly / monthly antigos
    weekly_monthly = existing_cube[
        existing_cube["periodo_tipo"].isin(["weekly", "monthly"])
    ]

    historical_cube = pd.concat(
        [
            historical_cube,
            weekly_monthly
        ],
        ignore_index=True
    )

    # =====================================================
    # 5️⃣ CONCATENAR CUBO HISTÓRICO + NOVO
    # =====================================================

    updated_cube = pd.concat(
        [
            historical_cube,
            new_cube
        ],
        ignore_index=True
    )

    # =====================================================
    # 6️⃣ REMOVER DUPLICATAS
    # =====================================================

    updated_cube = updated_cube.drop_duplicates(
        subset=[
            "periodo",
            "periodo_tipo",
            "formulario",
            "tipo_venda",
            "tipo_contato",
            "canal_atendimento",
            "origem_contato",
            "indicador"
        ],
        keep="last"
    )

    return updated_cube