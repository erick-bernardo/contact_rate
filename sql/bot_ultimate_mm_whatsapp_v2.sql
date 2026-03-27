-- =====================================================
-- BOT ULTIMATE MM WHATSAPP COM DADOS DE VENDAS
-- =====================================================
-- Padrão: Traz contatos (ultimate) com dados de vendas já enriquecidos
-- Granularidade: Contato (id_contato ultimate)
-- Join: id_pedido com vendas para trazer dados de vendas

WITH cte_ultimate_contatos AS (
    SELECT
        uum.id AS id_contato,
        uum.created_at AS data_atendimento,
        CASE 
            WHEN uum.pedido_id IS NULL OR uum.pedido_id = '-' OR uum.pedido_id = ''
            THEN NULL
            ELSE REGEXP_REPLACE(CAST(uum.pedido_id AS VARCHAR), '[^0-9]', '')
        END AS id_pedido,
        CASE 
            WHEN uum.pedido_pai_id IS NULL OR uum.pedido_pai_id = '-' OR uum.pedido_pai_id = ''
            THEN NULL
            ELSE REGEXP_REPLACE(CAST(uum.pedido_pai_id AS VARCHAR), '[^0-9]', '')
        END AS id_pedido_pai,
        uum.motivo_contato,
        uum.submotivo_contato,
        uum.email_atendente,
        uum.email_solicitante,
        uum.email_atribuido,
        uum.group_name,
        uum.origem_atendimento,
        'Ultimate Bot MM WhatsApp' AS origem_informacao,
        uum.flag_reputacional,
        uum.motivo_contato_voz,
        uum.canal_atendimento,
        uum.tipo_ticket,
        uum.status_ticket,
        uum.descricao_ticket,
        uum.flag_tags,
        uum.formulario,
        uum.resolucao_cx
    FROM lake_ultimate.ultimate_mm_whatsapp AS uum
    WHERE 1=1
        AND TO_DATE(uum.created_at, 'YYYY-MM-DD') >= '{start_date}'
        AND TO_DATE(uum.created_at, 'YYYY-MM-DD') < '{end_date}'
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
        FROM cte_ultimate_contatos c
        WHERE c.id_pedido = CAST(s.id_pedido AS INT)
    )
),

cte_vendas AS (
    SELECT *
    FROM cte_vendas_base
    WHERE rn = 1
)

SELECT 
    u.*,
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
FROM cte_ultimate_contatos u
LEFT JOIN cte_vendas v
    ON u.id_pedido = v.id_pedido
