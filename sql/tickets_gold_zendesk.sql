WITH tickets_base AS (

    SELECT
        zt.ticket_id
      , zt.created_at
      , zt.status as status_ticket
      , zt.due_at
      , zt.motivo_principal
      , zt.submotivo_principal
      , zt.type as tipo_ticket
      , zt.formulario
      , zt.via_channel
      , zt.score_rating
      , zt.tipo_venda as tipo_venda_zendesk
      , zt.status_pedido
      , zt.pedido_filho
      , zt.pedido_pai
      , zt.seller
      , zt.sku_item_quantidade
      , zt.canal_atendimento
      , zt.pedido_marketplace
      , zt.brand_id
      , zt.pedido_canal_reputacional
    FROM lake_bu_cx_analytics.zendesk_tickets_gold zt
    WHERE 1=1
        AND zt.canal_atendimento NOT IN ('Times internos MM', 'Rotinas e Automações')
        AND zt.formulario <> 'TDV'
        AND TO_DATE(zt.created_at, 'YYYY-MM-DD') >= '{start_date}'
        AND TO_DATE(zt.created_at, 'YYYY-MM-DD') < '{end_date}'
)

-- lê tags apenas dos tickets relevantes
, tags_filtradas AS (

    SELECT
        ztags.ticket_id
      , ztags.tags
    FROM lake_bu_zendesk.zendesk_tags ztags
    JOIN tickets_base tb
        ON tb.ticket_id = ztags.ticket_id

)

-- agrega tudo em uma única passagem
, tags_aggregadas AS (

    SELECT
        tf.ticket_id

        , MAX(
            CASE 
                WHEN tf.tags LIKE 'fila_bot%' 
                THEN 'Y'
                ELSE 'N'
            END
        ) AS flag_tag_fila_bot

        , MAX(
            CASE 
                WHEN tf.tags LIKE 'ultimate_pcid%' 
                THEN tf.tags
            END
        ) AS tag_pcid

        , MAX(
            CASE 
                WHEN tf.tags LIKE '%fila_bot%'
                 AND tf.tags NOT LIKE '%ticket_resolvido_mediacao_tags%'
                 AND tf.tags NOT LIKE '%conciex_mkt_no_ticket_found%'
                THEN 'Sim'
                ELSE 'Não'
            END
        ) AS retido_bot_mkt

        , MAX(
            CASE 
                WHEN tf.tags IN (
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

    FROM tags_filtradas tf
    GROUP BY 1

)

SELECT
    tb.*
  , COALESCE(ta.flag_tag_fila_bot,'N') AS flag_tag_fila_bot
  , ta.tag_pcid
  , COALESCE(ta.retido_bot_mkt,'Não') AS retido_bot_mkt

FROM tickets_base tb
LEFT JOIN tags_aggregadas ta
    ON tb.ticket_id = ta.ticket_id

WHERE COALESCE(ta.flag_exclusao,0) = 0