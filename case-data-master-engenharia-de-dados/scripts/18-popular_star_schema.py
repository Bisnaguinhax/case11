# ===============================================================================
# SCRIPT DE POPULAÇÃO DO STAR SCHEMA - DEMONSTRAÇÃO TÉCNICA
# ===============================================================================
# Este script standalone popula as tabelas de dimensão e fato do Data Mart,
# simulando a carga final de um modelo Star Schema.
#
# 🔐 SEGURANÇA:
# - As credenciais do PostgreSQL são recuperadas de forma segura do Vault.
#
# 📌 INSTRUÇÕES:
# 1. Garanta que o Vault está configurado com as credenciais do PostgreSQL.
# 2. Garanta que as tabelas do Star Schema existem no banco de dados.
# 3. Execute este script para popular o Data Mart.
# ===============================================================================

import pandas as pd
import psycopg2
import os
import sys

# Adiciona o diretório dos plugins ao path para encontrar o security_system
plugins_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
if plugins_path not in sys.path:
    sys.path.insert(0, plugins_path)

def _get_db_connection():
    """Obtém uma conexão com o PostgreSQL usando credenciais do Vault."""
    from security_system.vault import AirflowSecurityManager
    
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    
    # Logger simples para compatibilidade com o AirflowSecurityManager
    class SimpleLogger:
        def log(self, *args, **kwargs): pass
    
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, SimpleLogger())
    
    # Recupera as credenciais para o Data Mart PostgreSQL do Vault
    pg_creds = sec_manager.get_secret("postgres_datamart_credentials") 
    if not pg_creds:
        raise ValueError("Credenciais do Data Mart não encontradas no Vault.")
    
    # Adapta a chave 'database' para 'dbname' se necessário para psycopg2
    if 'dbname' not in pg_creds and 'database' in pg_creds:
        pg_creds['dbname'] = pg_creds.pop('database')

    # Retorna a conexão com o banco de dados PostgreSQL
    return psycopg2.connect(**pg_creds)

def inserir_dados_star_schema():
    """Popula as tabelas de dimensão e fato do Star Schema no Data Mart."""
    conn = None
    try:
        # Estabelece conexão com o banco
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        base_path = '{{AIRFLOW_HOME}}/data/olist'

        # Popula a dimensão cliente com dados únicos
        print("Populating dim_cliente...")
        df_cliente = pd.read_csv(f'{base_path}/olist_customers_dataset.csv').drop_duplicates(subset='customer_id')
        for _, row in df_cliente.iterrows():
            cursor.execute(
                "INSERT INTO dim_cliente (id_cliente, cidade, estado) VALUES (%s, %s, %s) "
                "ON CONFLICT (id_cliente) DO NOTHING",
                (row['customer_id'], row['customer_city'], row['customer_state'])
            )

        # Popula a dimensão produto com dados únicos
        print("Populating dim_produto...")
        df_produto = pd.read_csv(f'{base_path}/olist_products_dataset.csv').drop_duplicates(subset='product_id')
        for _, row in df_produto.iterrows():
            cursor.execute(
                "INSERT INTO dim_produto (id_produto, categoria) VALUES (%s, %s) "
                "ON CONFLICT (id_produto) DO NOTHING",
                (row['product_id'], row['product_category_name'])
            )

        # Popula a tabela fato de vendas com dados consolidados
        print("Populating fato_vendas...")
        df_fato = pd.read_csv(f'{base_path}/dados_consolidados.csv').dropna(subset=['order_id', 'customer_id', 'product_id', 'price'])
        for _, row in df_fato.iterrows():
            # Para a demo, o id_tempo é fixo. Idealmente, deve ser populado dinamicamente.
            id_tempo_placeholder = 1 
            cursor.execute(
                "INSERT INTO fato_vendas (id_venda, id_cliente, id_produto, id_tempo, valor) VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (id_venda) DO NOTHING",
                (row['order_id'], row['customer_id'], row['product_id'], id_tempo_placeholder, row['price'])
            )
        
        # Confirma as inserções no banco
        conn.commit()
        print("✅ Dados do Star Schema inseridos com sucesso!")

    except Exception as e:
        # Em caso de erro, desfaz as alterações para evitar dados inconsistentes
        if conn:
            conn.rollback()
        print(f"❌ Erro ao inserir dados no Star Schema: {e}")
    finally:
        # Fecha cursor e conexão com segurança
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    inserir_dados_star_schema()
