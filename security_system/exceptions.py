from airflow.exceptions import AirflowException

class SecuritySystemBaseError(AirflowException):
    """Exceção base para todos os erros relacionados ao sistema de segurança."""
    def __init__(self, message, original_exception=None, details=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.details = details if details is not None else {}

class KeyManagementError(SecuritySystemBaseError):
    """Levantada quando há um problema com o gerenciamento de chaves."""
    def __init__(self, message="Erro durante a operação de gerenciamento de chaves.", operation="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.operation = operation
        if "operation" not in self.details: self.details["operation"] = operation

class ConfigurationError(SecuritySystemBaseError):
    """Levantada quando configurações necessárias estão faltando ou são inválidas."""
    def __init__(self, message="Configuração inválida ou ausente.", config_item="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.config_item = config_item
        if "config_item" not in self.details: self.details["config_item"] = config_item

class AuditLogError(SecuritySystemBaseError):
    """Levantada quando há um problema com o sistema de log de auditoria."""
    def __init__(self, message="Erro durante a operação de log de auditoria.", log_event="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.log_event = log_event
        if "log_event" not in self.details: self.details["log_event"] = log_event

class VaultAccessError(SecuritySystemBaseError):
    """Levantada quando há um problema ao acessar ou modificar o vault de segurança."""
    def __init__(self, message="Erro ao acessar o vault de segurança.", vault_path="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.vault_path = vault_path
        if "vault_path" not in self.details: self.details["vault_path"] = vault_path

class SecurityViolation(SecuritySystemBaseError):
    """Levantada quando uma violação de segurança é detectada."""
    def __init__(self, message="Violação de segurança detectada.", violation_type="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.violation_type = violation_type
        if "violation_type" not in self.details: self.details["violation_type"] = violation_type

class ValidationError(SecuritySystemBaseError):
    """Levantada quando a validação de dados falha."""
    def __init__(self, message="Erro de validação de dados.", field="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.field = field
        if "field" not in self.details: self.details["field"] = field

class SecureConnectionError(SecuritySystemBaseError):
    """Levantada quando há um erro nas operações de conexão segura."""
    def __init__(self, message="Erro em operação de conexão segura.", conn_id="unknown", **kwargs):
        super().__init__(message, **kwargs)
        self.conn_id = conn_id
        if "conn_id" not in self.details: self.details["conn_id"] = conn_id
