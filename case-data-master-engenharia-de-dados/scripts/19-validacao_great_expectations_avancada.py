# ===================================================================================
# SCRIPT DE VALIDAÇÃO DE DADOS NO DATA LAKE - DEMONSTRAÇÃO AVANÇADA
# ===================================================================================
# Este script demonstra a validação de dados diretamente de um ficheiro
# no Data Lake (MinIO), utilizando o Great Expectations.
#
# 🔐 SEGURANÇA:
# - As credenciais do MinIO são recuperadas de forma segura do Vault.
# ===================================================================================

import pandas as pd
from minio import Minio
from io import StringIO
import great_expectations as ge
from great_expectations.dataset import PandasDataset
import os
import sys

# Adiciona o diretório dos plugins ao path para encontrar o security_system
plugins_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'plugins'))
if plugins_path not in sys.path:
    sys.path.insert(0, plugins_path)

def _get_minio_client():
    """Obtém um cliente MinIO seguro via Vault."""
    from plugins.security_system.vault import AirflowSecurityManager
    SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
    VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'
    class SimpleLogger:
        def log(self, *args, **kwargs): pass
    sec_manager = AirflowSecurityManager(VAULT_DB_PATH, SECRET_KEY, SimpleLogger())
    minio_creds = sec_manager.get_secret("minio_local_credentials")
    if not minio_creds: raise ValueError("Credenciais do MinIO não encontradas no Vault.")
    return Minio(
        minio_creds['endpoint_url'].replace('http://',''),
        access_key=minio_creds['access_key'],
        secret_key=minio_creds['secret_key'],
        secure=False
    )

print("🔗 Conectando ao MinIO de forma segura...")
client = _get_minio_client()

bucket_name = "silver-layer" # Exemplo de bucket
file_name = "vendas/consolidado_vendas.csv" # Exemplo de ficheiro

print(f"📥 Baixando ficheiro '{file_name}' do bucket '{bucket_name}'...")
data = client.get_object(bucket_name, file_name)
df = pd.read_csv(StringIO(data.read().decode('utf-8')))
ge_df = ge.from_pandas(df)

print("\n🔍 Aplicando expectativas de qualidade...")
ge_df.expect_column_to_exist("customer_state")
ge_df.expect_column_values_to_not_be_null("order_id")
ge_df.expect_column_values_to_be_between("price", min_value=0)

results = ge_df.validate()
print("\n📋 Resultados da validação:")
print(results)
if not results['success']:
    print("\n❌ VALIDAÇÃO FALHOU!")
    sys.exit(1)
else:
    print("\n✅ VALIDAÇÃO APROVADA!")
    sys.exit(0)
