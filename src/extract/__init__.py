"""
=====================================================
EXTRACT LAYER - ARQUITETURA REFATORADA (ETAPA 3)
=====================================================

Módulo responsável por extrair dados do Redshift através de queries SQL.

MUDANÇAS ARQUITETURAIS:
-----------------------

ANTES (Arquitetura Antiga):
1. zendesk_raw: Tickets simples, sem vendas
2. ultimate_raw: Contatos simples, sem vendas
3. vendas_raw: Base completa de vendas
4. JOIN em Python (global_enrichments.py): Combinava vendas com contatos

AGORA (Arquitetura Refatorada - ETAPA 3):
1. zendesk_raw: Tickets COM vendas já enriquecidas via SQL (tickets_gold_zendesk_v2.sql)
2. ultimate_raw: Contatos COM vendas já enriquecidas via SQL (bot_ultimate_mm_whatsapp_v2.sql)
3. vendas_raw: Base AGREGADA semanal (vendas_base_semanal.sql) - substitui bruta anterior

BENEFÍCIOS:
-----------
✅ Single source of truth: Dados de vendas vêm de UM lugar (SQL)
✅ Pre-enrichment: Vendas chegam nos contatos já prontos
✅ Performance: Sem merge em Python
✅ Qualidade: Filtros e validações em SQL (mais robusto)
✅ Manutenibilidade: Lógica centralizada nas queries

FLUXO ATUAL:
-----------
Raw Layer (Redshift Queries)
    ↓
    ├─ tickets_gold_zendesk_v2.sql → zendesk_raw (contatos + vendas)
    ├─ bot_ultimate_mm_whatsapp_v2.sql → ultimate_raw (contatos + vendas)
    ├─ contatos_mensageria.sql → mensageria_raw (contatos)
    └─ vendas_base_semanal.sql → vendas_raw (vendas agregadas semanais)
    
Staging Layer
    ↓
    ├─ stg_zendesk(zendesk_raw) → + 17 colunas de vendas, 8 datas, id_cliente limpo
    ├─ stg_ultimate(ultimate_raw) → + 17 colunas de vendas, 8 datas, id_cliente limpo
    ├─ stg_mensageria(mensageria_raw) → + 17 colunas de vendas, 8 datas, id_cliente limpo
    └─ stg_vendas_cube() → Agregações semanais (já vinha de vendas_raw)

Enrichment Layer (SEM mais joins de vendas!)
    ↓
    apply_global_enrichments(contacts) → Apenas enriquecimentos sem vendas

EXTRACTORS REFATORADOS:
-----------------------

1. extract_zendesk_gold()
   - Carrega dados pré-enriquecidos de Zendesk
   - Dados já incluem: nome_produto, marca, categoria, empresa_venda, etc.
   - Substitui: load_zendesk_raw() + join com vendas em Python

2. extract_ultimate_gold()
   - Carrega dados pré-enriquecidos do Ultimate bot
   - Dados já incluem: nome_produto, marca, categoria, empresa_venda, etc.
   - Substitui: load_ultimate_raw() + join com vendas em Python

3. extract_vendas_base()
   - Carrega base de vendas AGREGADA por semana
   - Substitui: raw/vendas completo (dados brutos)
   - Usado para: Validações, metricas secundárias, análises

PRÓXIMAS ETAPAS (Airflow Integration):
--------------------------------------

Quando integrado com Airflow:
1. Airflow executará as queries SQL no Redshift
2. Resultados salvos em parquet em data/raw/{zendesk,ultimate,vendas}
3. Python carregará os parquets (load_raw_parquet.py)
4. Pipeline segue normalmente

Queries SQL estão em: sql/
- sql/tickets_gold_zendesk_v2.sql
- sql/bot_ultimate_mm_whatsapp_v2.sql
- sql/vendas_base_semanal.sql
"""

"""
Extract Layer - Módulo refatorado ETAPA 3

Extractors disponíveis:
- extract_zendesk: Extrai tickets Zendesk com vendas enriquecidas (via SQL)
- extract_mensageria: Extrai contatos Mensageria com vendas enriquecidas (via SQL)
- extract_ultimate_from_csv: Extrai contatos Ultimate com vendas enriquecidas (via CSV)
- extract_vendas_aggregated: Extrai base de vendas agregada por semana (via SQL)

Todas as extrações suportam:
- Range de processamento (últimos 7 dias vs bootstrap 90 dias)
- Salvamento por semana em ISO 8601 (YYYY-Www.parquet)
- Pre-enrichment de vendas em contatos (Zendesk + Ultimate)
"""

from .extract_zendesk import extract_zendesk
from .extract_mensageria import extract_mensageria
from .extract_ultimate_csv import extract_ultimate_from_csv
# from .extract_vendas_aggregated import extract_vendas_aggregated

__all__ = [
    "extract_zendesk",
    "extract_mensageria",
    "extract_ultimate_from_csv",
    # "extract_vendas_aggregated",
]
