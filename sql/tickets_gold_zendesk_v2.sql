-- =====================================================
-- TICKETS GOLD ZENDESK COM DADOS DE VENDAS
-- =====================================================
-- Padrão: Traz contatos (tickets) com dados de vendas já enriquecidos
-- Granularidade: Contato (ticket_id)
-- Join: ticket_id com pedido para trazer dados de vendas

WITH cte_tickets AS (
    SELECT
        zt.ticket_id AS id_contato,
        zt.created_at AS data_atendimento,
        CASE 
            WHEN zt.pedido_filho IS NULL OR zt.pedido_filho = '-' OR zt.pedido_filho = '' 
            THEN NULL
            ELSE REGEXP_REPLACE(CAST(zt.pedido_filho AS VARCHAR), '[^0-9]', '')
        END AS id_pedido,
        CASE 
            WHEN zt.pedido_pai IS NULL OR zt.pedido_pai = '-' OR zt.pedido_pai = '' 
            THEN NULL
            ELSE REGEXP_REPLACE(CAST(zt.pedido_pai AS VARCHAR), '[^0-9]', '')
        END AS id_pedido_pai,
        --zt.status as status_ticket,
        zt.motivo_principal,
        zt.submotivo_principal,
        zt.type as tipo_ticket,
        zt.formulario,
        zt.via_channel,
        zt.score_rating,
        zt.tipo_venda as tipo_venda_zendesk,
        --zt.status_pedido,
        zt.canal_atendimento,
        zt.brand_id,
        COALESCE(ta.flag_tag_fila_bot,'N') AS flag_tag_fila_bot,
        ta.tag_pcid,
        COALESCE(ta.retido_bot_mkt,'Não') AS retido_bot_mkt
    FROM lake_bu_cx_analytics.zendesk_tickets_gold zt
    LEFT JOIN (
        SELECT
            ztags.ticket_id,
            MAX(
                CASE 
                    WHEN ztags.tags LIKE 'fila_bot%' 
                    THEN 'Y'
                    ELSE 'N'
                END
            ) AS flag_tag_fila_bot,
            MAX(
                CASE 
                    WHEN ztags.tags LIKE 'ultimate_pcid%' 
                    THEN ztags.tags
                END
            ) AS tag_pcid,
            MAX(
                CASE 
                    WHEN ztags.tags LIKE '%fila_bot%'
                     AND ztags.tags NOT LIKE '%ticket_resolvido_mediacao_tags%'
                     AND ztags.tags NOT LIKE '%conciex_mkt_no_ticket_found%'
                    THEN 'Sim'
                    ELSE 'Não'
                END
            ) AS retido_bot_mkt,
            MAX(
                CASE 
                    WHEN ztags.tags IN (
                        'tag_legado_meli',
                        'importado_legado',
                        'importado_automatico',
                        'whatapp_ativo_cx',
                        'envio_template_disparo_cross_imper',
                        'envio_template_disparo_cross_montagem',
                        'envio_disparo_ativo_especialista_suporte_assistencia',
                        'envio_template_disparo_ativo_pendencia_pj',
                        'envio_disparo_ativo_especialista_suporte_marketplace',
                        'envio_dsiparo_ativo_especialista_relacionamento',
                        'envio_disparo_ativo_especialista_reputacionais',
                        'envio_template_disparo_ativoocorrencia_de_coleta',
                        '55pbx_type_ativo',
                        'agradecimento_marketplace'
                    )
                    THEN 1
                    ELSE 0
                END
            ) AS flag_exclusao
        FROM lake_bu_zendesk.zendesk_tags ztags
        GROUP BY 1
    ) ta ON zt.ticket_id = ta.ticket_id
    WHERE 1=1
        AND zt.canal_atendimento NOT IN ('Times internos MM', 'Rotinas e Automações')
        AND zt.formulario <> 'TDV'
        AND TO_DATE(zt.created_at, 'YYYY-MM-DD') >= '{start_date}'
        AND TO_DATE(zt.created_at, 'YYYY-MM-DD') < '{end_date}'
        AND COALESCE(ta.flag_exclusao,0) = 0
),

cte_vendas_base AS (
    SELECT
        CAST(s.id_pedido AS INT)   AS id_pedido,
        dp.nome_produto,
        dp.marca,
        s.categoria_nova_estrutura AS categoria_produto,
        s.head_categoria,
        s.empresa_venda,
        s.tipo_venda,
        s.status_pedido,
        CASE
            WHEN s.nome_fornecedor = '-' THEN s.nome_seller
            ELSE s.nome_fornecedor
        END AS nome_fornecedor,
        pt.nome AS nome_transportadora,
        DATE(pipv.data_entrega_cliente) AS data_entrega_cliente_revisada,
        DATE(s.data_compra_date) AS data_compra_cliente,
        DATE(pipv.data_entregue) AS data_entregue,
        DATE(pipv.data_entrega) AS data_entrega,
        DATE(s.data_cancelamento) AS data_cancelamento,
        DATE(s.data_aprovacao) AS data_aprovacao,
        DATE(s.data_entregue_cliente) AS data_entregue_cliente,
        DATE(s.data_prometido_entrega_cliente) AS data_prometido_entrega_cliente,
        s.situacao,
        s.id_cliente,
        s.cidade_entrega,
        s.micro_regiao_entrega,
        s.regiao_destino,
        ROW_NUMBER() OVER (
            PARTITION BY s.id_pedido
            ORDER BY 
                CASE WHEN pipv.data_entregue IS NOT NULL THEN 1 ELSE 2 END,
                pipv.data_entrega DESC
        ) AS rn
    FROM looker_plan.looker_sales AS s
    LEFT JOIN lake_produto.dim_produto_cadastro AS dp 
        ON s.id_produto = dp.id_produto 
       AND dp.id_seller = s.id_seller
    LEFT JOIN lake_brain.portal_pedido_venda AS ppv 
        ON ppv.id_pedped = s.id_pedido
    LEFT JOIN lake_brain.portal_item_pedido_venda AS pipv 
        ON ppv.id = pipv.idfk_pedido_venda
    LEFT JOIN lake_brain.portal_item_faturamento AS pif 
        ON pif.idfk_item_pedido_venda = pipv.id
    LEFT JOIN lake_brain.portal_faturamento AS pf 
        ON pf.id = pif.idfk_faturamento
    LEFT JOIN lake_brain.portal_transportadora AS pt 
        ON pt.id = pf.idfk_transportadora_filial
    WHERE EXISTS (
        SELECT 1
        FROM cte_tickets c
        WHERE c.id_pedido = CAST(s.id_pedido AS INT)
    )
),

cte_vendas AS (
    SELECT *
    FROM cte_vendas_base
    WHERE rn = 1
)

SELECT 
    t.*,
    v.nome_produto,
    v.marca,
    v.categoria_produto,
    v.head_categoria,
    v.empresa_venda,
    v.tipo_venda,
    v.status_pedido,
    v.nome_fornecedor,
    v.nome_transportadora,
    v.data_entrega_cliente_revisada,
    v.data_compra_cliente,
    v.data_entregue,
    v.data_entrega,
    v.data_cancelamento,
    v.data_aprovacao,
    v.data_entregue_cliente,
    v.data_prometido_entrega_cliente,
    v.situacao,
    v.id_cliente,
    v.cidade_entrega,
    v.micro_regiao_entrega,
    v.regiao_destino
FROM cte_tickets t
LEFT JOIN cte_vendas v
    ON t.id_pedido = v.id_pedido
