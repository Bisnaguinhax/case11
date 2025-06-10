# ===================================================================================
# UPLOAD COM CRIPTOGRAFIA SERVER-SIDE (SSE) - DEMONSTRAÇÃO AVANÇADA
# ===================================================================================
# Este script demonstra como realizar o upload de ficheiros para o MinIO
# aplicando criptografia no lado do servidor (Server-Side Encryption), um
# requisito comum em ambientes com alta demanda de segurança.
#
# 🔐 SEGURANÇA:
# - As credenciais são obtidas via Vault.
# - A criptografia SSE-S3 (AES256) é aplicada durante o upload.
# ===================================================================================

from minio import Minio
import urllib3
import os
import sys

# Adiciona o diretório dos plugins ao path
plugins_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
if plugins_path not in sys.path:
    sys.path.insert(0, plugins_path)

def _get_minio_client_com_http():
    """Obtém um cliente MinIO seguro via Vault com um http_client customizado."""
    from plugins.security_system.vault import AirflowSecurityManager
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    class SimpleLogger:
        def log(self, *args, **kwargs): pass
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, SimpleLogger())
    minio_creds = sec_manager.get_secret("minio_local_credentials")
    if not minio_creds: raise ValueError("Credenciais do MinIO não encontradas no Vault.")
    
    urllib3.disable_warnings()
    http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
    
    return Minio(
        minio_creds['endpoint_url'].replace('https://','').replace('http://',''),
        access_key=minio_creds['access_key'],
        secret_key=minio_creds['secret_key'],
        secure=True,
        http_client=http_client
    )

print("1) Configurando cliente MinIO seguro...")
client = _get_minio_client_com_http()

bucket_name = "bronze-criptografado"
print(f"2) Verificando/Criando bucket '{bucket_name}'...")
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"-> Bucket '{bucket_name}' criado.")
else:
    print(f"-> Bucket '{bucket_name}' já existe.")

base_path = "{{AIRFLOW_HOME}}/data/olist"
arquivos = {
    f"{base_path}/olist_customers_dataset.csv": "olist_customers_dataset.csv",
    f"{base_path}/olist_orders_dataset.csv": "olist_orders_dataset.csv"
}

print("3) Iniciando upload com criptografia SSE AES256...")
for local_path, object_name in arquivos.items():
    if os.path.exists(local_path):
        print(f"  -> Enviando '{local_path}' para '{bucket_name}/{object_name}' com SSE...")
        client.fput_object(
            bucket_name,
            object_name,
            local_path,
            metadata={"x-amz-server-side-encryption": "AES256"}
        )
    else:
        print(f"  -> ⚠️  AVISO: Ficheiro não encontrado, pulando: {local_path}")

print("\n✅ Upload com criptografia finalizado.")
