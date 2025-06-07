import os
import sys
import traceback

# --- DEFINIR AS CONFIGURAÇÕES DIRETAMENTE NO SCRIPT ---
SECRET_KEY = 'Nggk-vXHT7kr3M4VLLGeYixWcOrjMu505Q90fjzO7A0='
AUDIT_LOG_PATH = '/Users/felps/airflow/logs/security_audit/audit.csv'
SYSTEM_LOG_PATH = '/Users/felps/airflow/logs/security_audit/system.log'
VAULT_DB_PATH = '/Users/felps/airflow/data/security_vault.db'

# Adiciona o diretório de plugins ao path do Python para encontrar seus módulos
plugins_path = '/Users/felps/airflow/plugins'
if plugins_path not in sys.path:
    sys.path.insert(0, os.path.abspath(plugins_path))

try:
    from security_system.audit import AuditLogger
    from security_system.vault import AirflowSecurityManager
except ImportError as e:
    print(f"--- ERRO DE IMPORTAÇÃO ---")
    print(f"Não foi possível importar os módulos de 'security_system': {e}")
    sys.exit(1)

print("--- Configurando segredos no Vault ---")
try:
    # 1. Crie o logger passando os paths
    audit = AuditLogger(
        audit_file_path=AUDIT_LOG_PATH,
        system_log_file_path=SYSTEM_LOG_PATH
    )

    # 2. Crie o security manager passando tudo que ele precisa
    sec_manager = AirflowSecurityManager(
        vault_db_path=VAULT_DB_PATH,
        secret_key=SECRET_KEY,
        audit_logger=audit
    )

    # 3. Adicione os segredos
    print("\nAdicionando credenciais...")

    # OpenWeatherMap
    sec_manager.add_secret("openweathermap_api_key", "005a8da0a976ff926b5edca0d80f363f")
    print("-> Segredo 'openweathermap_api_key' adicionado.")

    # MinIO
    minio_creds = {"endpoint_url": "https://192.168.0.116:9000", "access_key": "minioadmin", "secret_key": "minioadmin"}
    sec_manager.add_secret("minio_local_credentials", minio_creds)
    print("-> Segredo 'minio_local_credentials' adicionado.")

    # PostgreSQL (banco 'indicativos')
    postgres_indicativos_creds = {"user": "felipebonatti", "password": "senha123", "host": "127.0.0.1", "port": "5432", "dbname": "indicativos"}
    sec_manager.add_secret("postgres_indicativos_credentials", postgres_indicativos_creds)
    print("-> Segredo 'postgres_indicativos_credentials' adicionado.")

    # PostgreSQL (Data Mart)
    postgres_dm_creds = {
        'dbname': 'dm_st_gestao_indicadores',
        'user': 'felps',
        'password': 'senha123@', 
        'host': '127.0.0.1',
        'port': '5432'
    }
    sec_manager.add_secret("postgres_datamart_credentials", postgres_dm_creds)
    print("-> Segredo 'postgres_datamart_credentials' adicionado.")

    print("\n--- Script de configuração de segredos concluído com sucesso! ---")

except Exception as e:
    print(f"\n--- OCORREU UM ERRO GERAL ---")
    print(f"Erro: {e}")
    traceback.print_exc()
