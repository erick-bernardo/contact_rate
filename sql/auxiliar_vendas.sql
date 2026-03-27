SELECT DISTINCT
    -- =========================
    -- Identificadores principais
    -- Granularidade: item (SKU)
    -- =========================
      CAST(s.id_pedido AS INT)   AS id_pedido
    , CAST(dp.id_produto AS INT) AS id_produto

    -- =========================
    -- Informações do produto
    -- =========================
    , dp.nome_produto
    , dp.marca
    , s.categoria_nova_estrutura AS categoria_produto
    , s.head_categoria

    -- =========================
    -- Informações da venda
    -- =========================
    , s.empresa_venda
    , s.tipo_venda
    , s.status_pedido

    -- =========================
    -- Fornecedor / Seller
    -- Regra:
    --   Se nome_fornecedor = '-'
    --     → usar nome_seller
    --   Caso contrário → nome_fornecedor
    -- =========================
    , CASE
        WHEN s.nome_fornecedor = '-'
          THEN s.nome_seller
        ELSE s.nome_fornecedor
      END AS nome_fornecedor

    -- =========================
    -- Logística
    -- =========================
    , pt.nome AS nome_transportadora

    -- =========================
    -- Datas (cliente / entrega)
    -- Conversão explícita para DATE
    -- =========================
    , TO_DATE(pipv.data_entrega_cliente, 'YYYY-MM-DD') AS data_entrega_cliente_revisada
    , TO_DATE(s.data_compra_date,'YYYY-MM-DD') AS data_compra_cliente
    , TO_DATE(pipv.data_entregue,'YYYY-MM-DD') AS data_entregue
    , TO_DATE(pipv.data_entrega,'YYYY-MM-DD') AS data_entrega
    , TO_DATE(s.data_cancelamento,'YYYY-MM-DD') AS data_cancelamento
    , TO_DATE(s.data_aprovacao,'YYYY-MM-DD') AS data_aprovacao
    , s.data_entregue_cliente
    , s.data_prometido_entrega_cliente
    , s.situacao
    , s.id_cliente
    , s.cidade_entrega
    , s.micro_regiao_entrega
    , s.regiao_destino    

FROM looker_plan.looker_sales AS s

    -- =========================
    -- Pedido de venda (nível pedido)
    -- =========================
    LEFT JOIN lake_brain.portal_pedido_venda AS ppv
           ON ppv.id_pedped = s.id_pedido

    -- =========================
    -- Dimensão de produto
    -- Relaciona produto + seller
    -- =========================
    LEFT JOIN lake_produto.dim_produto_cadastro AS dp
           ON s.id_produto = dp.id_produto
          AND dp.id_seller = s.id_seller

    -- =========================
    -- Item do pedido de venda
    -- Granularidade: item (SKU)
    -- =========================
    LEFT JOIN lake_brain.portal_item_pedido_venda AS pipv
           ON ppv.id = pipv.idfk_pedido_venda
          AND pipv.codigo_sku = dp.id_produto

    -- =========================
    -- Item faturado
    -- =========================
    LEFT JOIN lake_brain.portal_item_faturamento AS pif
           ON pif.idfk_item_pedido_venda = pipv.id

    -- =========================
    -- Faturamento
    -- =========================
    LEFT JOIN lake_brain.portal_faturamento AS pf
           ON pf.id = pif.idfk_faturamento

    -- =========================
    -- Transportadora
    -- =========================
    LEFT JOIN lake_brain.portal_transportadora AS pt
           ON pt.id = pf.idfk_transportadora_filial

-- =========================
-- Filtro de extração
-- =========================
WHERE 1=1
       AND TO_DATE(s.data_compra_date, 'YYYY-MM-DD') >= '{start_date}'
       AND TO_DATE(s.data_compra_date, 'YYYY-MM-DD') < '{end_date}'