# ===============================================================================
# DAG DE CARGA DO STAR SCHEMA - DEMONSTRAÇÃO SEGURA
# ===============================================================================
# Orquestra a carga final dos dados transformados nas tabelas dimensionais
# e de fato do Data Mart (PostgreSQL), utilizando o pool de conexões seguras.
#
# 🔐 SEGURANÇA E ROBUSTEZ:
# - Conexão com o banco obtida via `SecureConnectionPool`.
# - Credenciais gerenciadas pelo Vault.
# - Carga transacional com rollback em caso de falha.
# - Auditoria completa de cada etapa da carga.
#
# 📌 INSTRUÇÕES:
# 1. Garanta que o Vault tem as credenciais do Data Mart (`postgres_datamart_credentials`).
# 2. As tabelas do Star Schema devem existir no banco de dados.
# 3. Execute para popular o Data Mart final.
# ===============================================================================

from __future__ import annotations
import pendulum
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def _carregar_star_schema(**kwargs):
    """Popula as tabelas de dimensão e fato do Star Schema de forma segura."""
    from plugins.security_system.audit import AuditLogger
    from plugins.security_system.connections import SecureConnectionPool
    from plugins.security_system.vault import AirflowSecurityManager
    from sqlalchemy import text

    # Configuração dos componentes de segurança
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
    SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
    
    audit = AuditLogger(AUDIT_LOG_PATH, SYSTEM_LOG_PATH)
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, audit)
    pool = SecureConnectionPool(security_manager=sec_manager, audit_logger=audit)
    
    dag_id = kwargs.get('dag_run').dag_id
    audit.log("Iniciando task de carregamento do Star Schema.", action="STAR_SCHEMA_LOAD_START", dag_id=dag_id)

    try:
        # Obtém a conexão segura do pool
        db_engine = pool.get_engine("postgres_datamart")
        print("✅ Conexão segura com o Data Mart estabelecida.")
        
        base_path = '{{AIRFLOW_HOME}}/data/olist'

        with db_engine.connect() as conn:
            # Limpa tabelas de forma transacional
            print("⚙️ Limpando tabelas de destino (dimensões e fato)...")
            conn.execute(text("TRUNCATE TABLE fato_vendas, dim_produto, dim_cliente RESTART IDENTITY;"))
            audit.log("Tabelas de destino limpas.", action="DB_TABLES_TRUNCATED", dag_id=dag_id)
            
            # Carga da dim_cliente
            print("   -> Carregando dim_cliente...")
            df_cliente = pd.read_csv(f'{base_path}/olist_customers_dataset.csv').drop_duplicates(subset='customer_id')
            df_cliente_final = df_cliente[['customer_id', 'customer_city', 'customer_state']].rename(columns={'customer_id': 'id_cliente', 'customer_city': 'cidade', 'customer_state': 'estado'})
            df_cliente_final.to_sql('dim_cliente', conn, if_exists='append', index=False)
            
            # Carga da dim_produto
            print("   -> Carregando dim_produto...")
            df_produto = pd.read_csv(f'{base_path}/olist_products_dataset.csv').drop_duplicates(subset='product_id')
            df_produto_final = df_produto[['product_id', 'product_category_name']].rename(columns={'product_id': 'id_produto', 'product_category_name': 'categoria'})
            df_produto_final.to_sql('dim_produto', conn, if_exists='append', index=False)
            
            # Carga da fato_vendas
            print("   -> Carregando fato_vendas...")
            df_fato = pd.read_csv(f'{base_path}/dados_consolidados.csv').dropna(subset=['order_id', 'customer_id', 'product_id', 'price'])
            df_fato_final = df_fato[['order_id', 'customer_id', 'product_id', 'price']].rename(columns={'order_id': 'id_venda', 'customer_id': 'id_cliente', 'product_id': 'id_produto', 'price': 'valor'})
            df_fato_final['id_tempo'] = 1 # Placeholder para dim_tempo
            df_fato_final['quantidade'] = 1 # Placeholder para quantidade
            df_fato_final.to_sql('fato_vendas', conn, if_exists='append', index=False)
            
            conn.commit() # Commit da transação

        print("✅ Carga do Star Schema concluída com sucesso!")
        audit.log("Carga do Star Schema concluída.", action="STAR_SCHEMA_LOAD_SUCCESS", dag_id=dag_id)
        
    except Exception as e:
        error_msg = f"Erro ao carregar Star Schema: {e}"
        audit.log(error_msg, level="CRITICAL", action="STAR_SCHEMA_LOAD_FAIL", dag_id=dag_id)
        raise

with DAG(
    dag_id='dag_06_carrega_star_schema_segura_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="### Carga do Data Mart\nCarrega os dados processados no modelo Star Schema do PostgreSQL de forma segura.",
    tags=['datamart', 'starschema', 'seguranca'],
) as dag:

    tarefa_carregar_schema = PythonOperator(
        task_id='carregar_star_schema_task',
        python_callable=_carregar_star_schema,
    )
