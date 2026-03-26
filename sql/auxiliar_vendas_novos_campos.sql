SELECT
	CAST(s.id_pedido AS INT) AS id_pedido,
	CAST(dp.id_produto AS INT) AS id_produto,
	s.id_produto as id_produto_ls,
	dp.nome_produto,
	dp.marca,
	s.empresa_venda,	
	s.tipo_venda,
	s.categoria_nova_estrutura AS categoria_produto,
	s.head_categoria,
	s.status_pedido,
	CASE
		WHEN s.nome_fornecedor = '-' THEN s.nome_seller 
		ELSE s.nome_fornecedor 
	END AS nome_fornecedor,
	pt.nome AS nome_transportadora,
	TO_DATE(pipv.data_entrega_cliente, 'YYYY-MM-DD') AS data_revisada,
	TO_DATE(ppv.data_compra_cliente, 'YYYY-MM-DD') AS data_compra_cliente,
	s.data_compra_date as data_compra_date_ls,
	TO_DATE(pipv.data_entregue, 'YYYY-MM-DD') AS data_entregue,
	TO_DATE(pipv.data_entrega,'YYYY-MM-DD') AS data_entrega,
	TO_DATE(s.data_cancelamento, 'YYYY-MM-DD') AS data_cancelamento_ls,
	s.data_aprovacao as data_aprovacao_ls,
	s.data_entregue_cliente,
	s.data_prometido_entrega_cliente,
	s.situacao,
	s.id_cliente,
	s.cidade_entrega,
	s.micro_regiao_entrega,
	s.regiao_destino
FROM looker_plan.looker_sales AS s 
	LEFT JOIN lake_brain.portal_pedido_venda AS ppv ON ppv.id_pedped = s.id_pedido
	LEFT JOIN lake_produto.dim_produto_cadastro AS dp ON s.id_produto = dp.id_produto AND dp.id_seller = s.id_seller
	LEFT JOIN lake_brain.portal_item_pedido_venda as pipv on ppv.id = pipv.idfk_pedido_venda AND pipv.codigo_sku = dp.id_produto
	LEFT JOIN lake_brain.portal_item_faturamento as pif on pif.idfk_item_pedido_venda = pipv.id
	LEFT JOIN lake_brain.portal_faturamento as pf on pf.id = pif.idfk_faturamento
	LEFT JOIN lake_brain.portal_transportadora as pt on pt.id = pf.idfk_transportadora_filial	
WHERE s.id_pedido IN ('64122140', '63988262', '22048854')