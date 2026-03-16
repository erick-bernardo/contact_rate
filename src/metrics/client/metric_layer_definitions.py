METRIC_LAYER = {

    "contact_rate": {
        "numerator": "volume_contatos",
        "denominator": "volume_pedidos"
    },

    "p_retencao_bot": {
        "numerator": "volume_retido_bot",
        "denominator": "volume_contatos"
    },

    "p_recontato": {
        "numerator": "volume_recontato",
        "denominator": "volume_contatos"
    },

    "p_recontato_24h": {
        "numerator": "volume_recontato_24h",
        "denominator": "volume_recontato"
    },

    "fcr": {
        "numerator": "volume_sem_recontato",
        "denominator": "volume_contatos"
    },

    "avg_dias_recontato": {
        "numerator": "soma_dias_recontato",
        "denominator": "volume_recontato_validos"
    },

    "avg_horas_recontato": {
        "numerator": "soma_horas_recontato",
        "denominator": "volume_recontato_validos"
    }

}