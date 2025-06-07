import pandas as pd
import hashlib
from faker import Faker
import re
import logging
import os
import numpy as np

from security_system.exceptions import SecurityViolation, ValidationError, KeyManagementError, SecuritySystemBaseError
from security_system.audit import AuditLogger
from security_system.vault import AirflowSecurityManager

class DataProtection:
    def __init__(self, security_manager=None, audit_logger=None):
        self.faker = Faker('pt_BR')
        if audit_logger:
            self.audit = audit_logger
        else:
            try:
                self.audit = AuditLogger()
            except ValueError as e:
                print(f"DataProtection: Falha ao instanciar AuditLogger no __init__ (ValueError): {e}. O logging de auditoria pode estar desabilitado.")
                self.audit = None
            except Exception as e_gen:
                print(f"DataProtection: Falha inesperada ao instanciar AuditLogger no __init__: {e_gen}. O logging de auditoria pode estar desabilitado.")
                self.audit = None

        if security_manager:
            self.security_manager = security_manager
        else:
            # Passar o self.audit 
            self.security_manager = AirflowSecurityManager(audit_logger=self.audit)
            
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self._key = None

    def _get_masking_key(self):
        try:
            if self._key is None:
                key_secret_info = self.security_manager.get_secret("data_masking_key")
                if key_secret_info and isinstance(key_secret_info, dict) and "value" in key_secret_info:
                    self._key = key_secret_info["value"].encode('utf-8')
                elif isinstance(key_secret_info, str): 
                     self._key = key_secret_info.encode('utf-8')
                else: 
                    self._key = os.getenv('FALLBACK_MASKING_KEY', 'default_fallback_key_for_masking').encode('utf-8')
                    if key_secret_info is None: 
                        self.logger.warning("Chave de mascaramento 'data_masking_key' não encontrada no Vault. Usando chave de fallback.")
                        if self.audit: self.audit.log("Chave de mascaramento de fallback usada (não encontrada).", level="WARNING", action="MASKING_KEY_FALLBACK")
            return self._key
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao obter chave de mascaramento do Vault: {e}", level="ERROR", action="KEY_VAULT_ERROR")
            if self._key is None: # Garantir que _key tenha um valor de fallback
                 self._key = os.getenv('FALLBACK_MASKING_KEY', 'default_fallback_key_for_masking').encode('utf-8')
                 self.logger.error(f"Erro crítico ao obter chave do Vault: {e}. Usando chave de fallback.")
                 if self.audit: self.audit.log("Chave de mascaramento de fallback usada (erro no vault).", level="ERROR", action="MASKING_KEY_FALLBACK_VAULT_ERROR")
            return self._key

    def mask_data(self, data_series: pd.Series, masking_method: str, column_name: str = "N/A", **kwargs) -> pd.Series:
        masked_data = data_series.copy()
        operation_details = {"column": column_name, "method": masking_method}
        log_column_name = column_name if column_name else "N/A"

        try:
            if masking_method == 'hash':
                masked_data = data_series.apply(lambda x: hashlib.sha256(str(x).encode('utf-8')).hexdigest())
                if self.audit: self.audit.log(f"Dados mascarados por hash na coluna '{log_column_name}'.", action="DATA_MASKED_HASH", resource=log_column_name, details=operation_details)
            elif masking_method == 'fake':
                if column_name == 'email':
                    masked_data = data_series.apply(lambda x: self.faker.email())
                elif column_name == 'name':
                    masked_data = data_series.apply(lambda x: self.faker.name())
                elif column_name == 'address':
                    masked_data = data_series.apply(lambda x: self.faker.address())
                else:
                    masked_data = data_series.apply(lambda x: self.faker.word())
                if self.audit: self.audit.log(f"Dados mascarados por dados fake na coluna '{log_column_name}'.", action="DATA_MASKED_FAKE", resource=log_column_name, details=operation_details)
            elif masking_method == 'static':
                static_value = kwargs.get('static_value', '[MASCARADO]')
                masked_data = data_series.apply(lambda x: static_value)
                if self.audit: self.audit.log(f"Dados mascarados por valor estático ('{static_value}') na coluna '{log_column_name}'.", action="DATA_MASKED_STATIC", resource=log_column_name, details=operation_details)
            elif masking_method == 'partial':
                start_len = kwargs.get('start_len', 0)
                end_len = kwargs.get('end_len', 0)
                mask_char = kwargs.get('mask_char', '*')
                
                def partial_mask_func(value):
                    s_value = str(value)
                    core_len = len(s_value) - start_len - end_len
                    if core_len > 0:
                        return s_value[:start_len] + (mask_char * core_len) + s_value[len(s_value)-end_len:]
                    elif start_len + end_len >= len(s_value): 
                        return mask_char * len(s_value) 
                    return s_value
                
                masked_data = data_series.apply(partial_mask_func)
                if self.audit: self.audit.log(f"Dados mascarados parcialmente na coluna '{log_column_name}'.", action="DATA_MASKED_PARTIAL", resource=log_column_name, details=operation_details)
            else:
                raise SecuritySystemBaseError(f"Método de mascaramento '{masking_method}' não suportado.")
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao mascarar dados na coluna '{log_column_name}': {e}", level="ERROR", action="DATA_MASK_FAIL", resource=log_column_name, details={"error": str(e), **operation_details})
            self.logger.error(f"Erro ao mascarar dados na coluna '{log_column_name}': {e}", exc_info=True)
            raise SecuritySystemBaseError(f"Falha ao mascarar dados: {e}")
        return masked_data

    def add_differential_privacy(self, df: pd.DataFrame, column_name: str, epsilon: float, sensitivity: float) -> pd.DataFrame:
        if column_name not in df.columns:
            self.logger.error(f"Coluna '{column_name}' não encontrada no DataFrame para privacidade diferencial.")
            raise ValueError(f"Coluna '{column_name}' não encontrada.")
        if not pd.api.types.is_numeric_dtype(df[column_name]):
            self.logger.error(f"Coluna '{column_name}' não é numérica para privacidade diferencial.")
            raise ValueError(f"Coluna '{column_name}' deve ser numérica.")

        scale = sensitivity / epsilon
        noise = np.random.laplace(0, scale, len(df))
        df_dp = df.copy()
        df_dp[column_name] = df_dp[column_name] + noise
        
        if self.audit: self.audit.log(f"Privacidade diferencial aplicada à coluna '{column_name}'.",
                       action="DIFFERENTIAL_PRIVACY_APPLIED", resource=column_name,
                       details={"epsilon": epsilon, "sensitivity": sensitivity, "scale": scale})
        self.logger.info(f"Privacidade diferencial aplicada à coluna '{column_name}'.")
        return df_dp
