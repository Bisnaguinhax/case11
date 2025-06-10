# =================================================================================
# DAG DE CONSOLIDAÇÃO DE DADOS OLIST - DEMONSTRAÇÃO TÉCNICA
# =================================================================================
# Esta DAG orquestra o processo de ETL sobre múltiplos datasets públicos da Olist,
# criando um CSV final consolidado e pronto para análises exploratórias.
#
# 💡 INSTRUÇÕES:
# 1. Garanta que os arquivos da Olist estão em: `{{AIRFLOW_HOME}}/data/olist/`
# 2. Execute esta DAG manualmente para gerar: `dados_consolidados.csv`
# 3. O objetivo é demonstrar controle de orquestração e tratamento de merges.
# =================================================================================

from __future__ import annotations
import os
import pendulum
import pandas as pd

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


# === 🔧 Função principal de consolidação de dados ===
def _consolidar_dados_olist():
    """
    Lê múltiplos arquivos CSV da Olist, executa joins, 
    e salva um dataset unificado.
    """
    base_path = '{{AIRFLOW_HOME}}/data/olist'
    print(f"📁 Lendo datasets a partir de: {base_path}")

    try:
        # 📦 Leitura dos arquivos de dados
        clientes = pd.read_csv(f'{base_path}/olist_customers_dataset.csv')
        pedidos = pd.read_csv(f'{base_path}/olist_orders_dataset.csv')
        pagamentos = pd.read_csv(f'{base_path}/olist_order_payments_dataset.csv')
        itens = pd.read_csv(f'{base_path}/olist_order_items_dataset.csv')
        reviews = pd.read_csv(f'{base_path}/olist_order_reviews_dataset.csv')
        produtos = pd.read_csv(f'{base_path}/olist_products_dataset.csv')
    except FileNotFoundError as e:
        print(f"❌ ERRO: Arquivo não encontrado. Verifique se os dados estão em '{base_path}'.")
        raise e

    print("⚙️ Iniciando merges...")

    # === 🔗 Etapa 1: Clientes e Pedidos ===
    dados_clientes = pd.merge(
        clientes[['customer_id', 'customer_city', 'customer_state']],
        pedidos[['order_id', 'order_status', 'customer_id']],
        on='customer_id', how='outer'
    )

    # === 🔗 Etapa 2: Pagamentos, Itens e Reviews ===
    dados_pedidos = pd.merge(
        pagamentos[['order_id', 'payment_type', 'payment_value']],
        itens[['order_id', 'product_id', 'price', 'freight_value']],
        on='order_id', how='outer'
    )
    dados_pedidos = pd.merge(
        dados_pedidos,
        reviews[['order_id', 'review_score']],
        on='order_id', how='outer'
    )
    dados_pedidos = pd.merge(
        dados_pedidos,
        produtos[['product_id', 'product_category_name']],
        on='product_id', how='left'
    )

    # === 🔗 Etapa 3: União final com dados do cliente ===
    dados_consolidados = pd.merge(
        dados_pedidos,
        dados_clientes,
        on='order_id',
        how='outer',
        suffixes=('_pedido', '_cliente')
    )

    print(f"📊 Total de registros consolidados: {len(dados_consolidados)}")

    # === 💾 Salvando resultado final ===
    output_file = f'{base_path}/dados_consolidados.csv'
    dados_consolidados.to_csv(output_file, index=False)
    print(f"✅ Arquivo final salvo em: {output_file}")


# === 📅 Definição da DAG ===
with DAG(
    dag_id='dag_consolida_dados_olist_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule=None,  # Execução manual
    catchup=False,
    doc_md="""
### DAG de Consolidação Olist
Esta DAG realiza a união de múltiplos datasets públicos da Olist,
gerando um único arquivo `.csv` contendo os dados consolidados.
""",
    tags=['olist', 'etl', 'consolidacao']
) as dag:

    # === ▶️ Tarefa única de consolidação ===
    tarefa_consolidar_dados = PythonOperator(
        task_id='executar_consolidacao_olist',
        python_callable=_consolidar_dados_olist
    )
