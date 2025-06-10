# ===================================================================================
# DAG DE CONSOLIDAÇÃO E MASCARAMENTO DE DADOS (PII) - DEMONSTRAÇÃO
# ===================================================================================
# Orquestra a consolidação dos datasets da Olist e aplica técnicas de
# mascaramento de dados em informações pessoalmente identificáveis (PII).
#
# 🔐 SEGURANÇA E PRIVACIDADE:
# - Integração com o `DataProtection` para anonimização de dados.
# - Demonstra mascaramento estático e por hash.
# - Todo o processo é auditado para garantir compliance com a LGPD.
#
# 📌 INSTRUÇÕES:
# 1. Execute a DAG e verifique o ficheiro de saída `dados_consolidados_mascarados.csv`.
# 2. Compare as colunas `customer_city` e `customer_state` com os dados originais.
# ===================================================================================

from __future__ import annotations
import pendulum
import os
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def _consolidar_e_proteger_dados(**kwargs):
    """Consolida os datasets da Olist e aplica mascaramento em PII."""
    from plugins.security_system.audit import AuditLogger
    from plugins.security_system.vault import AirflowSecurityManager
    from plugins.security_system.data_protection import DataProtection

    # Inicializa componentes de segurança
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
    SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    
    audit = AuditLogger(AUDIT_LOG_PATH, SYSTEM_LOG_PATH)
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, audit)
    dp = DataProtection(security_manager=sec_manager, audit_logger=audit)
    
    dag_id = kwargs['dag_run'].dag_id
    audit.log("Iniciando consolidação e proteção de dados.", action="CONSOLIDATION_START", dag_id=dag_id)
    
    base_path = '{{AIRFLOW_HOME}}/data/olist'

    try:
        print("Lendo datasets da Olist...")
        clientes = pd.read_csv(f'{base_path}/olist_customers_dataset.csv')
        pedidos = pd.read_csv(f'{base_path}/olist_orders_dataset.csv')
        pagamentos = pd.read_csv(f'{base_path}/olist_order_payments_dataset.csv')
        itens = pd.read_csv(f'{base_path}/olist_order_items_dataset.csv')
        produtos = pd.read_csv(f'{base_path}/olist_products_dataset.csv')

        print("Consolidando dados...")
        dados_clientes = pd.merge(clientes, pedidos, on='customer_id', how='outer')
        dados_pedidos = pd.merge(pagamentos, itens, on='order_id', how='outer')
        dados_consolidados = pd.merge(dados_pedidos, dados_clientes, on='order_id', how='outer')
        dados_consolidados = pd.merge(dados_consolidados, produtos, on='product_id', how='left')

        print(f"Total de linhas consolidadas: {len(dados_consolidados)}")

        print("\nAplicando mascaramento de dados sensíveis (PII)...")
        dados_mascarados = dados_consolidados.copy()
        
        # Mascaramento estático da cidade
        dados_mascarados['customer_city'] = dp.mask_data(
            dados_mascarados['customer_city'],
            masking_method='static',
            column_name='customer_city',
            static_value='[CIDADE_REMOVIDA_LGPD]'
        )
        print("   -> Coluna 'customer_city' mascarada.")
        
        # Mascaramento por hash do estado
        dados_mascarados['customer_state'] = dp.mask_data(
            dados_mascarados['customer_state'],
            masking_method='hash',
            column_name='customer_state'
        )
        print("   -> Coluna 'customer_state' mascarada com hash.")

        caminho_saida = f'{base_path}/dados_consolidados_mascarados.csv'
        dados_mascarados.to_csv(caminho_saida, index=False)
        print(f"\n✅ Ficheiro consolidado e mascarado salvo em: {caminho_saida}")
        audit.log(f"Ficheiro mascarado salvo: {caminho_saida}", action="SAVE_MASKED_FILE_SUCCESS", dag_id=dag_id)

    except Exception as e:
        audit.log(f"Erro na consolidação/proteção: {e}", level="CRITICAL", action="CONSOLIDATION_FAIL", dag_id=dag_id)
        raise

with DAG(
    dag_id='dag_03_consolidacao_e_mascaramento_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="### Consolidação e Mascaramento de Dados\nETL que unifica os datasets da Olist e aplica técnicas de mascaramento de PII.",
    tags=['etl', 'pii', 'lgpd', 'security'],
) as dag:
    
    tarefa_consolidar_proteger = PythonOperator(
        task_id='consolidar_e_proteger_dados_task',
        python_callable=_consolidar_e_proteger_dados,
    )
