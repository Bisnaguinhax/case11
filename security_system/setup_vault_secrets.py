import os
from security_system.vault import AirflowSecurityManager
from security_system.audit import AuditLogger
from dotenv import load_dotenv

# --- CONFIGURAÇÕES DO VAULT ---
VAULT_DB_PATH = '/Users/felps/airflow/data/security_vault.db'
SECRET_KEY = 'Nggk-vXHT7kr3M4VLLGeYixWcOrjMu505Q90fjzO7A0=' 

# --- CONFIGURAÇÕES DE LOG DE AUDITORIA ---
AUDIT_LOG_PATH = '/Users/felps/airflow/logs/security_audit/audit.csv'
SYSTEM_LOG_PATH = '/Users/felps/airflow/logs/security_audit/system.log'

os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)
os.makedirs(os.path.dirname(SYSTEM_LOG_PATH), exist_ok=True)

audit_logger = AuditLogger(
    audit_file_path=AUDIT_LOG_PATH,
    system_log_file_path=SYSTEM_LOG_PATH
)

security_manager = AirflowSecurityManager(
    vault_db_path=VAULT_DB_PATH,
    secret_key=SECRET_KEY,
    audit_logger=audit_logger
)

def setup_secrets():
    print(f"Configurando segredos no Vault em: {VAULT_DB_PATH}")
    audit_logger.log("Iniciando configuração de segredos no Vault.", action="VAULT_SETUP_START")

    try:
        # Credenciais do MinIO
        minio_endpoint = "192.168.0.116:9000" 
        minio_access_key = "minioadmin"   
        minio_secret_key = "minioadmin"   

        security_manager.add_secret("minio_endpoint", minio_endpoint)
        security_manager.add_secret("minio_access_key", minio_access_key)
        security_manager.add_secret("minio_secret_key", minio_secret_key)
        audit_logger.log("Credenciais MinIO adicionadas/atualizadas no Vault.", action="MINIO_SECRETS_ADD")

        # Credenciais do PostgreSQL
        pg_host = "localhost" 
        pg_port = "5432"      
        pg_database = "indicativos"
        pg_user = "felipebonatti"   
        pg_password = "senha123" 

        security_manager.add_secret("postgresql_host", pg_host)
        security_manager.add_secret("postgresql_port", pg_port)
        security_manager.add_secret("postgresql_database", pg_database)
        security_manager.add_secret("postgresql_user", pg_user)
        security_manager.add_secret("postgresql_password", pg_password)
        audit_logger.log("Credenciais PostgreSQL adicionadas/atualizadas no Vault.", action="PG_SECRETS_ADD")

        # Credencial do OpenWeatherMap
        openweathermap_api_key = "15f9495b6a74288062831e78c8d8b248" 
        security_manager.add_secret("openweathermap_api_key", openweathermap_api_key)
        audit_logger.log("Chave OpenWeatherMap adicionada/atualizada no Vault.", action="OPENWEATHERMAP_SECRET_ADD")

        print("Segredos configurados com sucesso no Vault.")
        audit_logger.log("Configuração de segredos no Vault concluída com sucesso.", action="VAULT_SETUP_SUCCESS")

    except Exception as e:
        print(f"Erro ao configurar segredos: {e}")
        audit_logger.log(
            f"Erro na configuração de segredos do Vault: {e}",
            level="CRITICAL",
            action="VAULT_SETUP_FAIL",
            error_message=str(e),
            stack_trace_needed=True
        )

if __name__ == "__main__":
    setup_secrets()

