import os
from security_system.vault import AirflowSecurityManager
from security_system.audit import AuditLogger 
from dotenv import load_dotenv


dotenv_config_path = '/Users/felps/airflow/config/security.env'
if os.path.exists(dotenv_config_path):
    load_dotenv(dotenv_config_path, override=True)
else:
    print(f"AVISO: Arquivo .env não encontrado em {dotenv_config_path}. Se as variáveis de ambiente não estiverem configuradas globalmente, o vault pode falhar.")


VAULT_DB_PATH = '/Users/felps/airflow/data/security_vault.db'
SECRET_KEY = 'Nggk-vXHT7kr3M4VLLGeYixWcOrjMu505Q90fjzO7A0='

try:
    # Inicializa o AuditLogger para passar para o SecurityManager
    AUDIT_LOG_PATH_FOR_TEST = '/Users/felps/airflow/logs/security_audit/audit_test.csv'
    SYSTEM_LOG_PATH_FOR_TEST = '/Users/felps/airflow/logs/security_audit/system_test.log'

    # Garanta que os diretórios existam para os logs de teste
    os.makedirs(os.path.dirname(AUDIT_LOG_PATH_FOR_TEST), exist_ok=True)
    os.makedirs(os.path.dirname(SYSTEM_LOG_PATH_FOR_TEST), exist_ok=True)

    test_audit_logger = AuditLogger(
        audit_file_path=AUDIT_LOG_PATH_FOR_TEST,
        system_log_file_path=SYSTEM_LOG_PATH_FOR_TEST
    )

    # Passar o audit_logger para o AirflowSecurityManager
    security_manager = AirflowSecurityManager(
        vault_db_path=VAULT_DB_PATH,
        secret_key=SECRET_KEY,
        audit_logger=test_audit_logger 
    )
    
    print("\n--- Verificando Segredos do Vault ---")
    
    minio_endpoint = security_manager.get_secret("minio_endpoint")
    minio_access_key = security_manager.get_secret("minio_access_key")
    minio_secret_key = security_manager.get_secret("minio_secret_key")

    print(f"MinIO Endpoint: {'ENCONTRADO' if minio_endpoint else 'NÃO ENCONTRADO'}")
    print(f"MinIO Access Key: {'ENCONTRADO' if minio_access_key else 'NÃO ENCONTRADO'}")
    print(f"MinIO Secret Key: {'ENCONTRADO' if minio_secret_key else 'NÃO ENCONTRADO'}")
    
    pg_host = security_manager.get_secret("postgresql_host")
    pg_port = security_manager.get_secret("postgresql_port") 
    pg_database = security_manager.get_secret("postgresql_database") 
    pg_user = security_manager.get_secret("postgresql_user")
    pg_password = security_manager.get_secret("postgresql_password")
    
    print(f"PostgreSQL Host: {'ENCONTRADO' if pg_host else 'NÃO ENCONTRADO'}")
    print(f"PostgreSQL Port: {'ENCONTRADO' if pg_port else 'NÃO ENCONTRADO'}")
    print(f"PostgreSQL Database: {'ENCONTRADO' if pg_database else 'NÃO ENCONTRADO'}")
    print(f"PostgreSQL User: {'ENCONTRADO' if pg_user else 'NÃO ENCONTRADO'}")
    print(f"PostgreSQL Password: {'ENCONTRADO' if pg_password else 'NÃO ENCONTRADO'}")

 

except Exception as e:
    print(f"\nERRO CRÍTICO ao acessar o Vault: {e}")
    print("Por favor, verifique:")
    print(f"- Se o arquivo do vault existe em: {VAULT_DB_PATH}")
    print(f"- Se a SECRET_KEY_CONFIG ('{SECRET_KEY}') é a mesma usada para criar os segredos.")
    print("- Se as variáveis de ambiente necessárias para AuditLogger (AUDIT_LOG_PATH, SYSTEM_LOG_PATH) estão configuradas ou se os paths hardcoded estão corretos.")

