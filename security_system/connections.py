import os
from minio import Minio
from sqlalchemy import create_engine
import urllib3

from security_system.vault import AirflowSecurityManager
from security_system.exceptions import SecureConnectionError, ConfigurationError

class SecureConnectionPool:
    def __init__(self, security_manager=None, audit_logger=None):
        if security_manager:
            self.security_manager = security_manager
        else:
            self.security_manager = AirflowSecurityManager(audit_logger=audit_logger)

        self.engines = {}
        self.clients = {}

    def get_engine(self, service_name: str):
        if service_name in self.engines:
            return self.engines[service_name]

        secret_name = f"{service_name}_credentials"
        secret_data = self.security_manager.get_secret(secret_name)

        if not secret_data or not isinstance(secret_data, dict):
            raise ConfigurationError(f"Credenciais para o serviço de DB '{service_name}' não encontradas ou inválidas.")

        try:
            # Passando os parâmetros de conexão de forma explícita para o create_engine
            # para forçar o uso de TCP/IP e evitar o erro de socket.

            # A URL base que define o dialeto
            db_url = "postgresql+psycopg2://"

            # Argumentos de conexão que serão passados para o driver psycopg2
            connect_args = {
                "host": secret_data['host'],
                "port": secret_data['port'],
                "user": secret_data['user'],
                "password": secret_data['password'],
                "dbname": secret_data['dbname'],
            }

            engine = create_engine(db_url, connect_args=connect_args)

            self.engines[service_name] = engine
            return engine
        except KeyError as e:
            raise ConfigurationError(f"Credencial faltando para o serviço de DB '{service_name}': {e}")
        except Exception as e:
            raise SecureConnectionError(f"Falha ao criar engine para '{service_name}': {e}")

    def get_client(self, service_name: str):
        if service_name in self.clients:
            return self.clients[service_name]

        secret_name = f"{service_name}_credentials"
        secret_data = self.security_manager.get_secret(secret_name)

        if not secret_data or not isinstance(secret_data, dict):
            raise ConfigurationError(f"Credenciais para o serviço de client '{service_name}' não encontradas ou inválidas.")

        try:
            endpoint = secret_data['endpoint_url'].replace('http://', '').replace('https://', '')
            access_key = secret_data['access_key']
            secret_key = secret_data['secret_key']

            http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            client = Minio(
                endpoint, access_key=access_key, secret_key=secret_key,
                secure=True, http_client=http_client
            )
            self.clients[service_name] = client
            return client
        except KeyError as e:
            raise ConfigurationError(f"Credencial faltando para o serviço de client '{service_name}': {e}")
        except Exception as e:
            raise SecureConnectionError(f"Falha ao criar cliente para '{service_name}': {e}")
