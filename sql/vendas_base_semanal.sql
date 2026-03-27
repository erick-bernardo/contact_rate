-- =====================================================
-- VENDAS BASE SEMANAL
-- =====================================================
-- Propósito: Base semanal de vendas que será carregada em raw/vendas
-- Substitui as bases brutas completas anteriores
-- Formato: Mesma estrutura de stg_vendas_cube após agregação
-- Granularidade: Agregado por data_venda, empresa_venda, tipo_venda, grupo_venda

WITH vendas_raw AS (
    SELECT
        CAST(s.id_pedido AS INT)   AS id_pedido,
        s.id_produto,
        s.empresa_venda,
        s.tipo_venda,
        s.status_pedido,
        DATE(s.data_compra_date) AS data_compra_cliente,
        DATE(s.data_compra_date) AS data_venda,
        DATE(s.data_cancelamento) AS data_cancelamento,
        DATE(s.data_aprovacao) AS data_aprovacao,
        DATE(s.data_entregue_cliente) AS data_entregue_cliente,
        DATE(s.data_prometido_entrega_cliente) AS data_prometido_entrega_cliente,
        s.situacao,
        s.id_cliente,
        s.cidade_entrega,
        s.micro_regiao_entrega,
        s.regiao_destino,
        -- Campos de enriquecimento
        dp.nome_produto,
        dp.marca,
        s.categoria_nova_estrutura AS categoria_produto,
        s.head_categoria,
        CASE
            WHEN s.nome_fornecedor = '-' THEN s.nome_seller
            ELSE s.nome_fornecedor
        END AS nome_fornecedor,
        pt.nome AS nome_transportadora,
        DATE(pipv.data_entrega_cliente) AS data_entrega_cliente_revisada,
        DATE(pipv.data_entregue) AS data_entregue,
        DATE(pipv.data_entrega) AS data_entrega
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
    WHERE 1=1
        AND TO_DATE(s.data_compra_date, 'YYYY-MM-DD') >= '{start_date}'
        AND TO_DATE(s.data_compra_date, 'YYYY-MM-DD') < '{end_date}'
),

vendas_filtradas AS (
    SELECT *
    FROM vendas_raw vr
    WHERE 1=1
        -- Manter apenas pedidos que passaram por aprovação
        AND vr.data_aprovacao NOT IN (
            DATE('1899-12-31'),
            DATE('1900-01-01')
        )
),

vendas_com_dimensoes AS (
    SELECT
        id_pedido,
        id_produto,
        empresa_venda,
        tipo_venda,
        status_pedido,
        data_compra_cliente,
        data_venda,
        data_cancelamento,
        data_aprovacao,
        data_entregue_cliente,
        data_prometido_entrega_cliente,
        situacao,
        id_cliente,
        cidade_entrega,
        micro_regiao_entrega,
        regiao_destino,
        nome_produto,
        marca,
        categoria_produto,
        head_categoria,
        nome_fornecedor,
        nome_transportadora,
        data_entrega_cliente_revisada,
        data_entregue,
        data_entrega,
        -- Criar grupo_venda
        CASE
            WHEN lower(empresa_venda) IN ('mm', 'mm_app', 'guide shop', 'casatema') THEN 'mm'
            ELSE 'marketplaces'
        END AS grupo_venda,
        -- Criar dimensões de tempo ISO
        TO_CHAR(data_venda, 'IYYY') || '-W' || TO_CHAR(data_venda, 'IW') AS semana_venda,
        TO_CHAR(data_venda, 'YYYY-MM') AS mes_venda
    FROM vendas_filtradas
),

vendas_cube AS (
    SELECT
        data_venda,
        semana_venda,
        mes_venda,
        empresa_venda,
        tipo_venda,
        grupo_venda,
        COUNT(*) AS itens_vendidos,
        COUNT(DISTINCT id_pedido) AS pedidos_vendidos
    FROM vendas_com_dimensoes
    GROUP BY
        data_venda,
        semana_venda,
        mes_venda,
        empresa_venda,
        tipo_venda,
        grupo_venda
)

SELECT *
FROM vendas_cube
ORDER BY 
    data_venda DESC,
    semana_venda DESC,
    mes_venda DESC
