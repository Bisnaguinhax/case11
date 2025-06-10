# ===================================================================================
# DAG DE UPLOAD PARA DATA LAKE (CAMADA BRONZE) - DEMONSTRAÇÃO TÉCNICA
# ===================================================================================
# Esta DAG orquestra o envio de arquivos locais para o MinIO (Data Lake),
# utilizando um sistema seguro baseado em Vault e logging de auditoria.
#
# 🔐 SEGURANÇA:
# - As credenciais do MinIO são extraídas do Vault via objeto seguro.
# - Caminhos de arquivos são parametrizados via `configure.py`.
# - Os uploads são rastreados por sistema de auditoria customizado.
#
# ✅ OBJETIVO:
# Popular a camada Bronze com dados brutos prontos para ingestão futura.
#
# 📌 INSTRUÇÕES:
# 1. Execute `setup_vault_secrets.py` para registrar as credenciais no Vault.
# 2. Depois, rode esta DAG manualmente para validar os uploads.
# ===================================================================================

from __future__ import annotations
import os
import pendulum
import boto3
from pathlib import Path
from botocore.exceptions import ClientError

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


# === 🚀 Função principal de upload ===
def _upload_para_minio_seguro():
    """
    Recupera credenciais do MinIO via Vault e realiza o upload
    de arquivos locais para a camada Bronze do Data Lake.
    """
    # Importação local (isolada) do sistema de segurança
    from plugins.security_system.audit import AuditLogger
    from plugins.security_system.vault import AirflowSecurityManager

    print("🔐 Obtendo credenciais seguras do Vault...")

    # Caminhos seguros (parametrizados por `configure.py`)
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
    SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'

    audit = AuditLogger(AUDIT_LOG_PATH, SYSTEM_LOG_PATH)
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, audit)

    minio_creds = sec_manager.get_secret("minio_local_credentials")
    if not minio_creds:
        raise ValueError("❌ ERRO: Credenciais 'minio_local_credentials' não encontradas no Vault.")

    bucket_name = "bronze-layer"
    print(f"🌐 Conectando ao MinIO em: {minio_creds['endpoint_url']}")

    # Cliente MinIO
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_creds['endpoint_url'],
        aws_access_key_id=minio_creds['access_key'],
        aws_secret_access_key=minio_creds['secret_key'],
    )

    # Verifica ou cria o bucket
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✔️ Bucket '{bucket_name}' já existe.")
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"🪣 Bucket '{bucket_name}' criado com sucesso.")

    # Mapeia arquivos locais -> objetos no MinIO
    base_path = '{{AIRFLOW_HOME}}/data'
    arquivos_para_upload = {
        f"{base_path}/clima/clima_coletado.csv": "clima/clima_coletado.csv",
        f"{base_path}/indicadores/ipca_coletado.csv": "indicadores/ipca_coletado.csv",
        f"{base_path}/olist/dados_consolidados.csv": "olist/dados_consolidados.csv"
    }

    print("\n📤 Iniciando upload dos arquivos...")

    for caminho_local, caminho_minio in arquivos_para_upload.items():
        caminho = Path(caminho_local)
        if caminho.exists():
            print(f"   ⬆️ {caminho.name} → s3://{bucket_name}/{caminho_minio}")
            s3.upload_file(str(caminho), bucket_name, caminho_minio)
        else:
            print(f"   ⚠️ Arquivo não encontrado, pulando: {caminho_local}")

    print("\n✅ Upload concluído com sucesso para o MinIO Bronze.")


# === 📅 Definição da DAG ===
with DAG(
    dag_id="dag_upload_bronze_minio_v1",
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule=None,
    catchup=False,
    doc_md="""
### DAG de Upload para a Camada Bronze
Esta DAG realiza o envio de arquivos brutos para o MinIO com segurança.
As credenciais são recuperadas de um Vault local e todo o processo é auditado.
""",
    tags=['bronze', 'minio', 'upload', 'vault']
) as dag:

    tarefa_upload_minio = PythonOperator(
        task_id="upload_ficheiros_para_camada_bronze",
        python_callable=_upload_para_minio_seguro
    )
