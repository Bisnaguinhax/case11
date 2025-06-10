"""
Security System Plugin for Apache Airflow
==========================================

Sistema de segurança customizado desenvolvido para demonstração técnica.
Fornece funcionalidades avançadas de:
- Gerenciamento de chaves e secrets (Vault)
- Proteção de dados sensíveis
- Auditoria e compliance
- Monitoramento de segurança em tempo real
- Rotação automática de chaves
- Pool de conexões seguras

"""

from .vault import AirflowSecurityManager
from .data_protection import DataProtection
from .audit import AuditLogger
from .monitoring import SecurityMonitor
from .key_rotation import KeyRotator
from .connections import SecureConnectionPool
from .exceptions import (
    SecuritySystemBaseError,
    KeyManagementError,
    ConfigurationError,
    AuditLogError,
    VaultAccessError,
    SecurityViolation,
    ValidationError,
    SecureConnectionError
)


__all__ = [
    # Core Security Components
    'AirflowSecurityManager',
    'DataProtection',
    'AuditLogger',
    'SecurityMonitor',
    'KeyRotator',
    'SecureConnectionPool',
    
    # Exception Classes
    'SecuritySystemBaseError',
    'KeyManagementError',
    'ConfigurationError',
    'AuditLogError',
    'VaultAccessError',
    'SecurityViolation', 
    'ValidationError',
    'SecureConnectionError'
]

# Module initialization
def initialize_security_system():
    """Inicializa o sistema de segurança com configurações padrão."""
    print("🔒 Security System Plugin carregado com sucesso!")
    print(f"   Versão: {__version__}")
    print("   Componentes: Vault, DataProtection, Audit, Monitoring")

# Auto-initialize when imported
initialize_security_system()
