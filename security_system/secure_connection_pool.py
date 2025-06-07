import os
from minio import Minio
import psycopg2
from security_system.vault import AirflowSecurityManager
from security_system.audit import AuditLogger
import urllib3 # Mantenha a importação

# Isso é para garantir que a verificação SSL seja ignorada para a conexão MinIO
insecure_http_client = urllib3.PoolManager(
    cert_reqs='CERT_NONE', 
    assert_hostname=False  
)

# --- CONFIGURAÇÕES DO SECURITY MANAGER ---
VAULT_DB_PATH_CONFIG = '/Users/felps/airflow/data/security_vault.db'
SECRET_KEY_CONFIG = 'Nggk-vXHT7kr3M4VLLGeYixWcOrjMu505Q90fjzO7A0='
AUDIT_LOG_PATH_CONFIG = '/Users/felps/airflow/logs/security_audit/audit.csv'
SYSTEM_LOG_PATH_CONFIG = '/Users/felps/airflow/logs/security_audit/system.log'


class SecureConnectionPool:
    def __init__(self):
        self.audit = AuditLogger(
            audit_file_path=AUDIT_LOG_PATH_CONFIG,
            system_log_file_path=SYSTEM_LOG_PATH_CONFIG
        )
        self.security_manager = AirflowSecurityManager(
            vault_db_path=VAULT_DB_PATH_CONFIG,
            secret_key=SECRET_KEY_CONFIG,
            audit_logger=self.audit
        )
        self.audit.log("SecureConnectionPool inicializado.", action="CONN_POOL_INIT")

    def get_minio_client(self):
        """Retorna um cliente MinIO seguro."""
        try:
            minio_endpoint = self.security_manager.get_secret("minio_endpoint")
            minio_access_key = self.security_manager.get_secret("minio_access_key")
            minio_secret_key = self.security_manager.get_secret("minio_secret_key")

            if not all([minio_endpoint, minio_access_key, minio_secret_key]):
                self.audit.log(
                    "Credenciais MinIO incompletas ou não encontradas no Vault.",
                    level="ERROR",
                    action="MINIO_CRED_MISSING",
                    risk_level="HIGH"
                )
                raise ValueError("Credenciais MinIO incompletas ou não encontradas.")

            client = Minio(
                endpoint=minio_endpoint,
                access_key=minio_access_key,
                secret_key=minio_secret_key,
                secure=True,                 
                http_client=insecure_http_client 
            )
            self.audit.log("Cliente MinIO obtido com sucesso.", action="MINIO_CONN_SUCCESS")
            return client
        except Exception as e:
            self.audit.log(
                f"Erro ao obter cliente MinIO: {e}",
                level="CRITICAL",
                action="MINIO_CONN_FAIL",
                error_message=str(e),
                stack_trace_needed=True,
                risk_level="CRITICAL"
            )
            raise

    def get_postgresql_conn(self):
        """Retorna uma conexão PostgreSQL segura."""
        try:
            pg_host = self.security_manager.get_secret("postgresql_host")
            pg_port = self.security_manager.get_secret("postgresql_port")
            pg_database = self.security_manager.get_secret("postgresql_database")
            pg_user = self.security_manager.get_secret("postgresql_user")
            pg_password = self.security_manager.get_secret("postgresql_password")

            if not all([pg_host, pg_port, pg_database, pg_user, pg_password]):
                self.audit.log(
                    "Credenciais PostgreSQL incompletas ou não encontradas no Vault.",
                    level="ERROR",
                    action="PG_CRED_MISSING",
                    risk_level="HIGH"
                )
                raise ValueError("Credenciais PostgreSQL incompletas ou não encontradas.")

            conn = psycopg2.connect(
                host=pg_host,
                port=pg_port,
                database=pg_database,
                user=pg_user,
                password=pg_password
            )
            self.audit.log("Conexão PostgreSQL obtida com sucesso.", action="PG_CONN_SUCCESS")
            return conn
        except Exception as e:
            self.audit.log(
                f"Erro ao obter conexão PostgreSQL: {e}",
                level="CRITICAL",
                action="PG_CONN_FAIL",
                error_message=str(e),
                stack_trace_needed=True,
                risk_level="CRITICAL"
            )
            raise

