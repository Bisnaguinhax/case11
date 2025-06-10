"""
Módulo de Conexões Seguras para Airflow
========================================
Gerencia pools de conexão seguros para bancos de dados e serviços externos.
Integra-se com o sistema de vault para recuperação segura de credenciais.

"""

import os
from minio import Minio
from sqlalchemy import create_engine
import urllib3
from typing import Optional, Dict, Any

from .vault import AirflowSecurityManager
from .exceptions import SecureConnectionError, ConfigurationError

class SecureConnectionPool:
    """
    Pool de conexões seguras que integra com o sistema de vault.
    
    Gerencia conexões para bancos de dados (PostgreSQL) e object storage (MinIO/S3)
    com recuperação automática de credenciais do vault seguro.
    """
    
    def __init__(self, security_manager: Optional[AirflowSecurityManager] = None, audit_logger=None):
        """
        Inicializa o pool de conexões seguras.
        
        Args:
            security_manager: Instância do gerenciador de segurança
            audit_logger: Logger para auditoria de acessos
        """
        if security_manager:
            self.security_manager = security_manager
        else:
            # Fallback para casos onde o security_manager não é fornecido
            self.security_manager = AirflowSecurityManager(audit_logger=audit_logger)

        self.engines: Dict[str, Any] = {}
        self.clients: Dict[str, Any] = {}

    def get_engine(self, service_name: str):
        """
        Obtém ou cria uma engine de banco de dados PostgreSQL.
        
        Args:
            service_name: Nome do serviço (ex: 'warehouse', 'analytics')
            
        Returns:
            SQLAlchemy Engine configurada e pronta para uso
            
        Raises:
            ConfigurationError: Se credenciais estão ausentes ou inválidas
            SecureConnectionError: Se falha na criação da conexão
        """
        if service_name in self.engines:
            return self.engines[service_name]

        secret_name = f"{service_name}_credentials"
        secret_data = self.security_manager.get_secret(secret_name)

        if not secret_data or not isinstance(secret_data, dict):
            raise ConfigurationError(
                f"Credenciais para o serviço de DB '{service_name}' não encontradas ou inválidas."
            )

        try:
            # Constrói URL de conexão PostgreSQL
            db_url = (
                f"postgresql+psycopg2://{secret_data['user']}:{secret_data['password']}"
                f"@{secret_data['host']}:{secret_data['port']}/{secret_data['dbname']}"
            )
            
            engine = create_engine(
                db_url,
                pool_pre_ping=True,  # Testa conexões antes de usar
                pool_recycle=3600,   # Recicla conexões a cada hora
                echo=False
            )
            
            self.engines[service_name] = engine
            return engine
            
        except KeyError as e:
            raise ConfigurationError(
                f"Credencial faltando para o serviço de DB '{service_name}': {e}"
            )
        except Exception as e:
            raise SecureConnectionError(
                f"Falha ao criar engine para '{service_name}': {e}"
            )

    def get_client(self, service_name: str):
        """
        Obtém ou cria um cliente MinIO/S3 para object storage.
        
        Args:
            service_name: Nome do serviço (ex: 'datalake', 'backup')
            
        Returns:
            Cliente MinIO configurado e autenticado
            
        Raises:
            ConfigurationError: Se credenciais estão ausentes ou inválidas
            SecureConnectionError: Se falha na criação do cliente
        """
        if service_name in self.clients:
            return self.clients[service_name]

        secret_name = f"{service_name}_credentials"
        secret_data = self.security_manager.get_secret(secret_name)

        if not secret_data or not isinstance(secret_data, dict):
            raise ConfigurationError(
                f"Credenciais para o serviço de client '{service_name}' não encontradas ou inválidas."
            )

        try:
            # Limpa protocolo do endpoint
            endpoint = secret_data['endpoint_url'].replace('http://', '').replace('https://', '')
            access_key = secret_data['access_key']
            secret_key = secret_data['secret_key']

            # Configuração para ambientes de desenvolvimento/demonstração
            # Em produção, configurar certificados SSL apropriados
            http_client = urllib3.PoolManager(cert_reqs='CERT_NONE')
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

            client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=True,
                http_client=http_client
            )
            
            self.clients[service_name] = client
            return client
            
        except KeyError as e:
            raise ConfigurationError(
                f"Credencial faltando para o serviço de client '{service_name}': {e}"
            )
        except Exception as e:
            raise SecureConnectionError(
                f"Falha ao criar cliente para '{service_name}': {e}"
            )

    def close_all_connections(self):
        """
        Fecha todas as conexões ativas do pool.
        Útil para cleanup em testes ou shutdown da aplicação.
        """
        for engine in self.engines.values():
            engine.dispose()
        
        # MinIO clients não precisam de dispose explícito
        self.engines.clear()
        self.clients.clear()
