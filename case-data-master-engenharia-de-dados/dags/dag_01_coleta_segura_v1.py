# ===================================================================================
# DAG DE COLETA SEGURA COM AUDITORIA COMPLETA - DEMONSTRAÇÃO
# ===================================================================================
# Orquestra a coleta de dados de IPCA e Clima com integração total
# ao sistema de segurança e auditoria desenvolvido para o projeto.
#
# 🔐 SEGURANÇA E GOVERNANÇA:
# - Chaves de API e caminhos são gerenciados de forma centralizada e segura.
# - Cada passo da execução é logado no sistema de auditoria para compliance.
# - Utiliza o Vault para recuperar segredos em tempo de execução.
#
# 📌 INSTRUÇÕES:
# 1. Garanta que o Vault contém a 'openweathermap_api_key'.
# 2. Execute a DAG e analise os logs do Airflow e os ficheiros de auditoria gerados.
# ===================================================================================

from __future__ import annotations
import pendulum
import os
import requests
import pandas as pd
from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def _get_security_components():
    """Helper para inicializar e retornar os componentes de segurança."""
    from plugins.security_system.audit import AuditLogger
    from plugins.security_system.vault import AirflowSecurityManager
    
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
    SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    
    audit = AuditLogger(audit_file_path=AUDIT_LOG_PATH, system_log_file_path=SYSTEM_LOG_PATH)
    sec_manager = AirflowSecurityManager(vault_db_path=VAULT_DB_PATH, secret_key=SECRET_KEY, audit_logger=audit)
    
    return audit, sec_manager

def _coleta_ipca(**kwargs):
    """Coleta dados do IPCA e registra a operação no sistema de auditoria."""
    audit, _ = _get_security_components()
    dag_id = kwargs['dag_run'].dag_id
    
    audit.log("Iniciando coleta do IPCA.", action="COLETA_IPCA_START", dag_id=dag_id)
    try:
        base_path = '{{AIRFLOW_HOME}}/data/indicadores'
        os.makedirs(base_path, exist_ok=True)
        url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        df = pd.DataFrame(response.json())
        df.to_csv(f"{base_path}/ipca_coletado.csv", index=False)
        audit.log("Dados do IPCA coletados com sucesso.", action="COLETA_IPCA_SUCCESS", dag_id=dag_id)
    except Exception as e:
        audit.log(f"Erro na coleta do IPCA: {e}", level="ERROR", action="COLETA_IPCA_FAIL", dag_id=dag_id)
        raise

def _coleta_clima(**kwargs):
    """Coleta dados de clima usando chave do Vault e registra na auditoria."""
    audit, sec_manager = _get_security_components()
    dag_id = kwargs['dag_run'].dag_id
    
    audit.log("Iniciando coleta de dados climáticos.", action="COLETA_CLIMA_START", dag_id=dag_id)
    try:
        api_key = sec_manager.get_secret("openweathermap_api_key")
        if not api_key: raise ValueError("Chave 'openweathermap_api_key' não encontrada no Vault.")
        
        base_path = '{{AIRFLOW_HOME}}/data/clima'
        os.makedirs(base_path, exist_ok=True)
        cidades = {"São Paulo": 3448439, "Rio de Janeiro": 3451190}
        dados = []
        for cidade, codigo in cidades.items():
            url = f"http://api.openweathermap.org/data/2.5/weather?id={codigo}&appid={api_key}&units=metric&lang=pt"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            clima = response.json()
            dados.append({"cidade": cidade, "temperatura": clima["main"]["temp"], "condicao": clima["weather"][0]["description"]})
        
        pd.DataFrame(dados).to_csv(f"{base_path}/clima_coletado.csv", index=False)
        audit.log("Dados climáticos coletados com sucesso.", action="COLETA_CLIMA_SUCCESS", dag_id=dag_id)
    except Exception as e:
        audit.log(f"Erro na coleta de dados climáticos: {e}", level="ERROR", action="COLETA_CLIMA_FAIL", dag_id=dag_id)
        raise

with DAG(
    dag_id='dag_01_coleta_segura_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="### Coleta Segura com Auditoria\nEsta DAG demonstra a coleta de dados externos com total integração ao sistema de segurança e auditoria.",
    tags=['ingestao', 'seguranca', 'auditoria']
) as dag:
    
    tarefa_ipca = PythonOperator(task_id='coleta_ipca_segura_task', python_callable=_coleta_ipca)
    tarefa_clima = PythonOperator(task_id='coleta_clima_segura_task', python_callable=_coleta_clima)

    # As tarefas podem rodar em paralelo
    [tarefa_ipca, tarefa_clima]
