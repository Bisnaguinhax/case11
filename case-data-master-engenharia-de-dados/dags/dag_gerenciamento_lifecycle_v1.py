# ===================================================================================
# DAG DE GERENCIAMENTO DE LIFECYCLE (HOT -> COLD STORAGE) - DEMONSTRAÇÃO
# ===================================================================================
# Esta DAG simula uma política de lifecycle que move arquivos "frios" (com mais de
# 30 dias) do bucket "hot" (bronze) para o bucket "cold" (glacier-mock).
#
# 🔐 SEGURANÇA:
# - As credenciais do MinIO são acessadas de forma segura via Vault.
#
# 📌 INSTRUÇÕES:
# 1. Verifique se o Vault está configurado corretamente com as credenciais do MinIO.
# 2. Execute a DAG para simular a política de arquivamento automática.
# ===================================================================================

from __future__ import annotations
import pendulum
import boto3
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

from airflow.models.dag import DAG
from airflow.decorators import task

def _get_minio_client():
    """Helper para criar um cliente MinIO com credenciais seguras obtidas do Vault."""
    from plugins.security_system.vault import AirflowSecurityManager
    
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    class SimpleLogger:
        def log(self, *args, **kwargs): pass
        
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, SimpleLogger())
    minio_creds = sec_manager.get_secret("minio_local_credentials")
    if not minio_creds:
        raise ValueError("Credenciais do MinIO não encontradas no Vault.")
    
    return boto3.client(
        "s3",
        endpoint_url=minio_creds['endpoint_url'],
        aws_access_key_id=minio_creds['access_key'],
        aws_secret_access_key=minio_creds['secret_key'],
    )

@task
def criar_bucket_cold_storage_task():
    """Garante que o bucket de cold storage (destino) exista antes da movimentação."""
    s3 = _get_minio_client()
    bucket_destino = "cold-storage-layer"
    try:
        s3.head_bucket(Bucket=bucket_destino)
        print(f"✔️ Bucket de cold storage '{bucket_destino}' já existe.")
    except ClientError:
        s3.create_bucket(Bucket=bucket_destino)
        print(f"🧊 Bucket de cold storage '{bucket_destino}' criado.")

@task
def mover_arquivos_antigos_task():
    """Verifica e move arquivos antigos do bucket 'hot' para o 'cold' storage."""
    s3 = _get_minio_client()
    bucket_origem = "bronze-layer"
    bucket_destino = "cold-storage-layer"
    hoje = datetime.now()

    print(f"🔎 Verificando arquivos no bucket '{bucket_origem}' com mais de 30 dias...")
    resposta = s3.list_objects_v2(Bucket=bucket_origem)

    if 'Contents' not in resposta:
        print("✅ Nenhum arquivo encontrado no bucket de origem. Nada a mover.")
        return

    for objeto in resposta['Contents']:
        data_modificacao = objeto['LastModified'].replace(tzinfo=None)
        nome_ficheiro = objeto['Key']
        
        if (hoje - data_modificacao) > timedelta(days=30):
            print(f"   -> 🥶 Arquivo '{nome_ficheiro}' é antigo. Movendo para cold storage...")
            try:
                copy_source = {'Bucket': bucket_origem, 'Key': nome_ficheiro}
                s3.copy_object(Bucket=bucket_destino, CopySource=copy_source, Key=nome_ficheiro)
                s3.delete_object(Bucket=bucket_origem, Key=nome_ficheiro)
                print(f"      -> ✅ Movido e removido da origem com sucesso.")
            except Exception as e:
                print(f"      -> ❌ Erro ao mover o arquivo '{nome_ficheiro}': {e}")
        else:
            print(f"   -> 🔥 Arquivo '{nome_ficheiro}' é recente. Nenhuma ação necessária.")

with DAG(
    dag_id='dag_gerenciamento_lifecycle_v1',
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule_interval='@daily',
    catchup=False,
    doc_md="### Gerenciamento de Lifecycle\nSimula política de arquivamento movendo dados antigos da camada 'hot' para 'cold'.",
    tags=['lifecycle', 'minio', 'storage'],
) as dag:

    criar_bucket_task = criar_bucket_cold_storage_task()
    mover_arquivos_task = mover_arquivos_antigos_task()

    criar_bucket_task >> mover_arquivos_task
