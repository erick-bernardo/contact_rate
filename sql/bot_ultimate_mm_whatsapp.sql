SELECT DISTINCT
  ult.bot_id,
  ult.bot_name,
  ult.conversation_id,
  ult.platform_conversation_id,
  -- fuso utc 00
  ult.conversation_start_time,
  FORMAT_DATE('%Y-%m-%d', DATE(ult.conversation_start_time)) as conversation_start_date,
  -- fuso br
  DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo") AS conversation_start_time_br,
  FORMAT_DATE('%Y-%m-%d', DATE(DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo"))) as conversation_start_date_br,
   -- semana do ano (iniciando na segunda)
  EXTRACT(ISOWEEK FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) AS conversation_start_week,
  EXTRACT(MONTH FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) AS conversation_start_month,
  -- hora (0–23)
  EXTRACT(HOUR FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) AS conversation_start_hora,
  ( CASE
    WHEN (
      (
        EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) BETWEEN 2 AND 7
        AND EXTRACT(HOUR FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) BETWEEN 8 AND 19
      )
      OR (
        EXTRACT(DAYOFWEEK FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) = 1
        AND EXTRACT(HOUR FROM DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")) BETWEEN 9 AND 14
      )
    )
    THEN 'Dentro do horário Atendimento'
    ELSE 'Fora do horário Atendimento'
  END ) AS horario_atendimento,  
  ult.conversation_end_time,
  FORMAT_DATE('%Y-%m-%d', DATE(DATETIME(TIMESTAMP(ult.conversation_end_time), "America/Sao_Paulo"))) as conversation_end_date_br, 
  REGEXP_REPLACE(CAST(ult.order_id AS STRING), r'[\.,].*', '') AS id_pedido_pai,
  REGEXP_REPLACE(CAST(ult.children_id AS STRING), r'[\.,].*', '') AS id_pedido_filho,
  --ult.conversations_data,
  ult.tma,
  ult.channel,
  --ult.labels,
  ult.conversation_status,
  ult.last_resolution,
  ult.is_llm_conversation,
  ult.bot_messages_count,
  ult.visitor_messages_count,
  ult.not_understood_messages_count,
  --ult.insert_date,
  --ult.data_sunco_customer_id,
  ult.bsat_score as data_bsatscore,
  --ult.is_seller,
  --ult.gpt_helped,
  ult.confidence_score,
  ult.use_case as data_use_case,
  ult.wpp_number,
  ult.document,
  --ult.data_reason,
  --ult.data_reason_selected,
  --ult.name,
  --ult.reply_type,
  --ult.reply_name,
  ult.recontato
FROM `cx-dp-prd-8c34.gold.zendesk_ultimate` AS ult
WHERE DATETIME(TIMESTAMP(ult.conversation_start_time), "America/Sao_Paulo")
      BETWEEN DATETIME_SUB(DATETIME(CURRENT_TIMESTAMP(), "America/Sao_Paulo"), INTERVAL 90 DAY)
      AND DATETIME(CURRENT_TIMESTAMP(), "America/Sao_Paulo")
      AND lower(ult.bot_name) = lower('MadeiraMadeira Produção - Chat')
      AND (
           LOWER(ult.use_case) NOT IN ('i want to buy', 'store address')
           OR ult.use_case IS NULL
          )
ORDER BY ult.conversation_start_time DESC
         