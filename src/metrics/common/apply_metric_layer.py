import pandas as pd

from src.metrics.client.metric_layer_definitions import METRIC_LAYER


def apply_metric_layer(metric_cube_base: pd.DataFrame) -> pd.DataFrame:

    df = metric_cube_base.copy()

    dimension_cols = [
        "periodo",
        "periodo_tipo",
        "formulario",
        "tipo_venda",
        "tipo_contato",
        "canal_atendimento",
        "origem_contato"
    ]

    results = []

    for metric_name, rule in METRIC_LAYER.items():

        numerator = rule["numerator"]
        denominator = rule["denominator"]

        num_df = df[df["indicador"] == numerator]
        den_df = df[df["indicador"] == denominator]

        merged = num_df.merge(
            den_df,
            on=dimension_cols,
            suffixes=("_num", "_den")
        )

        merged["valor"] = (
            merged["valor_num"] /
            merged["valor_den"]
        )

        merged["indicador"] = metric_name

        merged = merged[
            dimension_cols + ["indicador", "valor"]
        ]

        results.append(merged)

    metric_cube_metrics = pd.concat(results, ignore_index=True)

    metric_cube_final = pd.concat(
        [df, metric_cube_metrics],
        ignore_index=True
    )

    return metric_cube_final