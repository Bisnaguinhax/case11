# ===================================================================================
# DAG DE COLETA DE DADOS EXTERNOS COM VALIDAÇÃO - VERSÃO 3
# ===================================================================================
# Orquestra a coleta de dados do IPCA e do clima, seguida de uma
# verificação básica para identificar anomalias, como valores nulos.
#
# 🔐 SEGURANÇA:
# - A chave da API de clima é obtida de forma segura via Vault.
# - Nenhuma credencial ou caminho sensível é exposto diretamente no código.
#
# 📌 INSTRUÇÕES:
# 1. Certifique-se que o Vault contém a chave 'openweathermap_api_key'.
# 2. Execute esta DAG manualmente e acompanhe os logs de cada tarefa.
# ===================================================================================

from __future__ import annotations
import pendulum
import os
import requests
import pandas as pd
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# ===============================================================================
# NOTA TÉCNICA:
# As tarefas (tasks) foram desenhadas de forma modular e desacoplada,
# para garantir reusabilidade, testabilidade e clareza de propósito.
# ===============================================================================
def _get_api_key_from_vault(**kwargs):
    """
    🔐 Tarefa de segurança: recupera a chave da API do Vault e a passa via XCom.
    """
    from plugins.security_system.vault import AirflowSecurityManager

    print("🔐 Acessando Vault para obter a chave da API...")
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'

    class SimpleLogger:
        def log(self, *args, **kwargs): pass

    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, SimpleLogger())
    api_key = sec_manager.get_secret("openweathermap_api_key")

    if not api_key:
        raise ValueError("Chave 'openweathermap_api_key' não encontrada no Vault.")

    kwargs['ti'].xcom_push(key='api_key', value=api_key)
    print("✅ Chave da API recuperada e enviada via XCom.")

def _coleta_ipca():
    """
    📊 Coleta dados do IPCA diretamente do Banco Central do Brasil
    e salva localmente em CSV para processamento posterior.
    """
    print("📊 Coletando dados do IPCA...")
    base_path = '{{AIRFLOW_HOME}}/data/indicadores'
    os.makedirs(base_path, exist_ok=True)

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    df_ipca = pd.DataFrame(response.json())
    df_ipca.to_csv(f"{base_path}/ipca_coletado.csv", index=False)
    print("✅ Dados do IPCA salvos com sucesso.")

def _coleta_clima(**kwargs):
    """
    🌦️ Coleta dados climáticos de cidades pré-definidas usando a API OpenWeather,
    utilizando a chave segura recuperada do Vault via XCom.
    """
    print("🌦️ Coletando dados de Clima...")
    api_key = kwargs['ti'].xcom_pull(key='api_key', task_ids='get_api_key_task')

    if not api_key:
        raise ValueError("Chave da API não recebida via XCom.")

    base_path = '{{AIRFLOW_HOME}}/data/clima'
    os.makedirs(base_path, exist_ok=True)

    cidades = {"São Paulo": 3448439, "Rio de Janeiro": 3451190}
    dados_clima = []

    for cidade, codigo in cidades.items():
        url = (
            f"http://api.openweathermap.org/data/2.5/weather"
            f"?id={codigo}&appid={api_key}&units=metric&lang=pt"
        )
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        clima = response.json()
        dados_clima.append({
            "cidade": cidade,
            "temperatura": clima["main"]["temp"],
            "condicao": clima["weather"][0]["description"],
        })

    df_clima = pd.DataFrame(dados_clima)
    df_clima.to_csv(f"{base_path}/clima_coletado.csv", index=False)
    print("✅ Dados climáticos salvos com sucesso.")

def _verifica_anomalias():
    """
    🕵️ Realiza verificação simples para detectar valores nulos
    nos arquivos coletados, indicando possíveis anomalias nos dados.
    """
    print("🕵️ Verificando anomalias (valores nulos)...")
    base_path = '{{AIRFLOW_HOME}}/data'

    ipca_path = f'{base_path}/indicadores/ipca_coletado.csv'
    clima_path = f'{base_path}/clima/clima_coletado.csv'

    df_ipca = pd.read_csv(ipca_path)
    if df_ipca.isnull().sum().sum() > 0:
        print("   -> ⚠️ Aviso: encontrados valores nulos nos dados do IPCA!")
    else:
        print("   -> ✅ Dados do IPCA parecem consistentes.")

    df_clima = pd.read_csv(clima_path)
    if df_clima.isnull().sum().sum() > 0:
        print("   -> ⚠️ Aviso: encontrados valores nulos nos dados de Clima!")
    else:
        print("   -> ✅ Dados de Clima parecem consistentes.")

with DAG(
    dag_id='dag_extracao_e_validacao_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule='@daily',
    catchup=False,
    doc_md=(
        "### Extração de Dados Externos com Validação\n"
        "Coleta dados do IPCA e Clima, com recuperação segura de chaves "
        "e uma verificação básica de anomalias."
    ),
    tags=['ingestao', 'api', 'validacao'],
) as dag:

    get_api_key_task = PythonOperator(
        task_id='get_api_key_task',
        python_callable=_get_api_key_from_vault,
    )
    tarefa_ipca = PythonOperator(
        task_id='coleta_ipca_task',
        python_callable=_coleta_ipca,
    )
    tarefa_clima = PythonOperator(
        task_id='coleta_clima_task',
        python_callable=_coleta_clima,
    )
    tarefa_verificacao = PythonOperator(
        task_id='verifica_anomalias_task',
        python_callable=_verifica_anomalias,
    )

    get_api_key_task >> tarefa_clima
    [tarefa_ipca, tarefa_clima] >> tarefa_verificacao
