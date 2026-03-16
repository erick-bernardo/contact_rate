SELECT
    CAST(s."order" AS INT) AS id_contato,
    s.created_at AS data_atendimento,
    CAST(fc.id_pedped AS INT) AS id_pedido,
    CAST(fc.parent_pedped AS INT) AS id_pedido_pai,
    CAST(prd.id_prdprd AS INT) AS id_produto,
    'Nova Mensageria' AS motivo_contato,
    'Nova Mensageria' AS submotivo_contato,
    'Nova Mensageria' AS email_atendente,
    'Nova Mensageria' AS email_solicitante,
    'Nova Mensageria' AS email_atribuido,
    'Nova Mensageria' AS group_name,
    'Nova Mensageria' AS origem_atendimento,
    'Mensageria Seller' AS origem_informacao,
    'Nova Mensageria' AS flag_reputacional,
    'Nova Mensageria' AS motivo_contato_voz,
    'Nova Mensageria' AS canal_atendimento,
    'Nova Mensageria' AS tipo_ticket,
    'Nova Mensageria' AS status_ticket,
    'Nova Mensageria' AS descricao_ticket,
    'Nova Mensageria' AS flag_tags,
    'Nova Mensageria' AS formulario,
    'Nova Mensageria' AS resolucao_cx
FROM lake_mensageria_seller.sessions AS s
    INNER JOIN lake_madeira_fc.b2c_pedped AS fc ON fc.id_pedped = s."order"
    INNER JOIN lake_madeira_fc.b2c_pedprd AS prd ON fc.id_pedped = prd.id_pedped
WHERE s.created_at >= '2025-04-01'
    AND s."source" NOT IN ('marketplace')
    AND s.seller_id NOT IN (1,8)