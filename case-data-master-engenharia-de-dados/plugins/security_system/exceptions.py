# Sistema de Exceções Customizadas para Security System
# 
# O Airflow pode não estar instalado no ambiente de teste,
# fallback para a Exception padrão do Python.
try:
    from airflow.exceptions import AirflowException
except ImportError:
    # Fallback para ambientes sem Airflow
    AirflowException = Exception

class SecuritySystemBaseError(AirflowException):
    """
    Exceção base para todos os erros relacionados ao sistema de segurança.
    
    Args:
        message (str): Mensagem de erro
        original_exception (Exception): Exceção original que causou o erro
        details (dict): Detalhes adicionais sobre o erro
    """
    def __init__(self, message, original_exception=None, details=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.details = details if details is not None else {}

class KeyManagementError(SecuritySystemBaseError):
    """Levantada quando há um problema com o gerenciamento de chaves."""
    pass

class ConfigurationError(SecuritySystemBaseError):
    """Levantada quando configurações necessárias estão faltando ou são inválidas."""
    pass

class AuditLogError(SecuritySystemBaseError):
    """Levantada quando há um problema com o sistema de log de auditoria."""
    pass

class VaultAccessError(SecuritySystemBaseError):
    """Levantada quando há um problema ao acessar ou modificar o vault de segurança."""
    pass

class SecurityViolation(SecuritySystemBaseError):
    """Levantada quando uma violação de segurança é detectada."""
    pass

class ValidationError(SecuritySystemBaseError):
    """Levantada quando a validação de dados falha."""
    pass

class SecureConnectionError(SecuritySystemBaseError):
    """Levantada quando há um erro nas operações de conexão segura."""
    pass
