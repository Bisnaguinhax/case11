# ===============================================================================
# DAG DE PROCESSAMENTO SPARK COM INJEÇÃO SEGURA DE CREDENCIAIS
# ===============================================================================
# Esta DAG demonstra um padrão de segurança avançado: recuperar credenciais
# do Vault e injetá-las como variáveis de ambiente para um processo externo
# (neste caso, um job Spark).
#
# 🔐 SEGURANÇA:
# - A DAG atua como um "broker" de credenciais, obtendo-as do Vault.
# - O script Spark lê as credenciais do ambiente, sem nunca as ter no código.
# - Este padrão evita a exposição de segredos e promove o desacoplamento.
#
# 📌 INSTRUÇÕES:
# 1. Garanta que as 'minio_local_credentials' estão no Vault.
# 2. Execute a DAG e observe como o script Spark consegue aceder ao MinIO.
# ===============================================================================

from __future__ import annotations
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

def _get_minio_env_vars():
    """Obtém credenciais do MinIO do Vault e as formata como variáveis de ambiente."""
    from plugins.security_system.vault import AirflowSecurityManager
    from plugins.security_system.exceptions import ConfigurationError
    
    print("🔐 Acedendo ao Vault para injetar credenciais no ambiente do Spark...")
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    
    class SimpleLogger:
        def log(self, *args, **kwargs): pass
        
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, SimpleLogger())
    minio_creds = sec_manager.get_secret("minio_local_credentials")
    if not minio_creds or not isinstance(minio_creds, dict):
        raise ConfigurationError("Credenciais do MinIO não encontradas no Vault.")

    # Retorna um dicionário que será usado pelo parâmetro `env` do BashOperator
    return {
        "MINIO_ENDPOINT_URL": minio_creds.get("endpoint_url"),
        "MINIO_ACCESS_KEY": minio_creds.get("access_key"),
        "MINIO_SECRET_KEY": minio_creds.get("secret_key"),
    }

with DAG(
    dag_id='dag_04_processamento_spark_seguro_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="### Processamento Spark com Injeção Segura de Credenciais\nObtém segredos do Vault e os passa como variáveis de ambiente para o job Spark.",
    tags=['spark', 'batch', 'security', 'vault'],
) as dag:
    
    # A lógica para obter as variáveis de ambiente é executada durante o parse da DAG
    minio_env_vars = _get_minio_env_vars()
    
    tarefa_spark_segura = BashOperator(
        task_id='submeter_job_spark_seguro',
        bash_command='spark-submit --master local[*] {{AIRFLOW_HOME}}/scripts/12-processa_vendas.py',
        env=minio_env_vars, # Injeta as credenciais no ambiente da task
    )
