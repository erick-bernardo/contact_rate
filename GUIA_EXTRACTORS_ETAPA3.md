"""
=====================================================
GUIA: AJUSTES NOS EXTRACTORS/LOADERS - ETAPA 3
=====================================================

RESUMO DA MUDANÇA:
------------------

Antes (Arquitetura Antiga):
  load_zendesk_raw() → Apenas tickets
  load_ultimate_raw() → Apenas contatos
  load_vendas_raw() → Base completa de vendas
  
Depois (Arquitetura Refatorada):
  extract_zendesk_gold() → Tickets + Vendas (via SQL)
  extract_ultimate_gold() → Contatos + Vendas (via SQL)
  extract_vendas_base() → Vendas AGREGADAS semanais (via SQL)


O QUE MUDOU:
-----------

1. ZENDESK
   Antes: Dados brutos de tickets
   Depois: Query SQL traz tickets + dados de vendas já enriquecidos
   
   Dados adicionados:
   - nome_produto, marca, categoria_produto
   - empresa_venda, tipo_venda
   - data_compra_cliente, data_aprovacao, data_entregue_cliente
   - situacao, id_cliente, cidade_entrega
   - Etc (17 colunas de vendas ao todo)

2. ULTIMATE
   Antes: Dados brutos de contatos bot
   Depois: Query SQL traz contatos + dados de vendas já enriquecidos
   
   Dados adicionados: (mesmo padrão do Zendesk)

3. VENDAS (IMPORTANTE!)
   Antes: Base COMPLETA e BRUTA com todos os pedidos
   Depois: Base AGREGADA por semana
   
   Mudança estrutural:
   - Não mais usado para join em Python
   - Agora é base de VALIDAÇÃO e MÉTRICAS secundárias
   - Dados já vêm agregados por semana/mês


IMPLEMENTAÇÃO PRÁTICA:
---------------------

Atualmente (local, sem Redshift integrado):
1. As queries SQL foram criadas em: sql/

2. Três novos extractors foram criados em src/extract/:
   - extract_zendesk_gold.py
   - extract_ultimate_gold.py
   - extract_vendas_base.py

3. Esses extractors carregam dados de data/raw/ como .parquet
   (preparando para quando Airflow executar as queries no Redshift)

4. Pipeline Python já foi atualizado para:
   ✅ Carregar dados enriquecidos (stg_zendesk, stg_ultimate)
   ✅ Não fazer join com vendas em global_enrichments.py
   ✅ Usar vendas agregadas quando necessário


FLUXO INTEGRAÇÃO REDSHIFT:
--------------------------

PASSO 1: Airflow executa queries SQL
   → Cria tabelas temporárias ou exporta resultados

PASSO 2: Resultados salvos em data/raw/ como parquet
   → data/raw/zendesk/tickets_*.parquet
   → data/raw/ultimate/contacts_*.parquet
   → data/raw/vendas/vendas_*.parquet
   → data/raw/mensageria/messages_*.parquet

PASSO 3: Python carrega os parquets
   extract_zendesk_gold() → load_raw_parquet(RAW_ZENDESK_PATH)
   extract_ultimate_gold() → load_raw_parquet(RAW_ULTIMATE_PATH)
   extract_vendas_base() → load_raw_parquet(RAW_VENDAS_PATH)

PASSO 4: Staging processa os dados
   stg_zendesk(zendesk_raw)
   stg_ultimate(ultimate_raw)
   stg_vendas_cube(RAW_VENDAS_PATH)

PASSO 5: Enriquecimento (SEM joins!)
   apply_global_enrichments(contacts) ← Apenas contatos!
   
   Nota: vendas_stg não é mais necessário


CHECKLIST DE MUDANÇAS:
---------------------

✅ SQL Queries criadas:
   - tickets_gold_zendesk_v2.sql
   - bot_ultimate_mm_whatsapp_v2.sql
   - vendas_base_semanal.sql
   - contatos_mensageria.sql (já existia)

✅ Extractors criados:
   - extract_zendesk_gold.py
   - extract_ultimate_gold.py
   - extract_vendas_base.py

✅ Staging functions atualizadas:
   - stg_zendesk() suporta 17 colunas de vendas
   - stg_ultimate() suporta 17 colunas de vendas
   - stg_mensageria() suporta 17 colunas de vendas
   - stg_vendas_cube() continua igual

✅ Global enrichments refatorado:
   - Removido join_sales_data()
   - Removido parâmetro vendas_stg
   - Removido chamada em apply_global_enrichments()

✅ Pipeline atualizado:
   - build_contact_rate_metrics.py chamadas ajustadas


COMO ATIVAR (Quando Airflow estiver pronto):
----------------------------------------------

1. Criar DAG Airflow que executa queries:
   ```python
   zendesk_task = RedshiftQueryOperator(
       task_id="extract_zendesk_gold",
       sql="sql/tickets_gold_zendesk_v2.sql",
       output_path="data/raw/zendesk/"
   )
   ```

2. DAG salva resultados em data/raw/

3. Python carrega automaticamente via:
   load_raw_dataset(RAW_ZENDESK_PATH)
   → busca data/raw/zendesk/*.parquet

4. Pipeline segue normalmente


NOTAS IMPORTANTES:
------------------

⚠️ Atualmente os dados estão em CSV
   Depois de Redshift integrado, serão PARQUET

⚠️ vendas_stg não é mais carregado em build_contact_rate_metrics.py
   Linhas 160-165 podem ser removidas:
   ```python
   # PODE SER REMOVIDO após validação:
   # vendas_stg = stg_vendas(vendas_raw)
   # logger.info(f"Vendas STG: {len(vendas_stg)} linhas")
   ```

⚠️ vendas_cube_stg continua sendo usado (dados agregados)
   Mantém-se como está

📝 Documentação detalhada em:
   ROADMAP_REDSHIFT_INTEGRATION.md
"""
