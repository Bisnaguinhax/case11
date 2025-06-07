import os
import sqlite3
import base64
from cryptography.fernet import Fernet
from datetime import datetime, timedelta
import logging

from security_system.vault import AirflowSecurityManager
from security_system.audit import AuditLogger
from security_system.exceptions import KeyManagementError, ConfigurationError, SecuritySystemBaseError

class KeyRotator:
    def __init__(self, security_manager=None, audit_logger=None, db_path=None):
        # Tenta instanciar AuditLogger aqui também se não for passado, para consistência
        if audit_logger:
            self.audit = audit_logger
        else:
            try:
                self.audit = AuditLogger()
            except ValueError as e: # Se AuditLogger falhar por causa de .env não carregado
                print(f"KeyRotator: Falha ao instanciar AuditLogger no __init__ (ValueError): {e}. O logging de auditoria pode estar desabilitado.")
                self.audit = None 
            except Exception as e_gen:
                print(f"KeyRotator: Falha inesperada ao instanciar AuditLogger no __init__: {e_gen}. O logging de auditoria pode estar desabilitado.")
                self.audit = None
        
        if security_manager:
            self.security_manager = security_manager
        else:
            self.security_manager = AirflowSecurityManager(audit_logger=self.audit)


        self.db_path = db_path or os.getenv('KEY_ROTATION_DB_PATH', '/tmp/airflow_key_rotation.db')
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        # Adicionar um handler básico para self.logger se não houver nenhum, para ver os logs.
        if not self.logger.handlers:
            ch = logging.StreamHandler() # Loga no console
            ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(ch)
            
        self._initialize_db()
        if self.audit: self.audit.log("KeyRotator inicializado.", action="KEY_ROTATOR_INIT")


    def _initialize_db(self):
        try:
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS keys (
                    version TEXT PRIMARY KEY,
                    key_value BLOB NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT
                )
            """)
            conn.commit()
            conn.close()
            if self.audit: self.audit.log("Banco de dados de rotação de chaves inicializado/verificado.", action="KEY_DB_INIT")
            self.logger.info("Banco de dados de rotação de chaves inicializado.")
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao inicializar o banco de dados de rotação de chaves: {e}", level="CRITICAL", action="KEY_DB_INIT_FAIL")
            self.logger.error(f"Erro ao inicializar o banco de dados de rotação de chaves: {e}", exc_info=True)
            raise KeyManagementError(f"Falha ao inicializar o DB de rotação de chaves: {e}")

    def _generate_new_key(self):
        new_key = Fernet.generate_key()
        if self.audit: self.audit.log("Nova chave criptográfica gerada.", action="KEY_GENERATE")
        self.logger.info("Nova chave criptográfica gerada.")
        return new_key

    def _store_key(self, version, key_value, expires_at=None):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            created_at = datetime.now().isoformat()
            cursor.execute("INSERT INTO keys (version, key_value, created_at, expires_at) VALUES (?, ?, ?, ?)",
                           (version, key_value, created_at, expires_at.isoformat() if expires_at else None))
            conn.commit()
            conn.close()
            if self.audit: self.audit.log(f"Chave versão '{version}' armazenada no DB.", action="KEY_STORE", resource=version)
            self.logger.info(f"Chave versão '{version}' armazenada no DB.")
        except sqlite3.IntegrityError:
            if self.audit: self.audit.log(f"Tentativa de armazenar chave com versão duplicada: {version}", level="WARNING", action="KEY_STORE_DUPLICATE")
            self.logger.warning(f"Chave versão '{version}' já existe no DB. Pulando armazenamento.")
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao armazenar chave versão '{version}': {e}", level="CRITICAL", action="KEY_STORE_FAIL")
            self.logger.error(f"Erro ao armazenar chave '{version}': {e}", exc_info=True)
            raise KeyManagementError(f"Falha ao armazenar chave: {e}")

    def _get_key_from_db(self, version):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT key_value FROM keys WHERE version = ?", (version,))
            result = cursor.fetchone()
            conn.close()
            if result:
                if self.audit: self.audit.log(f"Chave versão '{version}' recuperada do DB.", action="KEY_RETRIEVE", resource=version)
                return result[0]
            return None
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao recuperar chave versão '{version}': {e}", level="CRITICAL", action="KEY_RETRIEVE_FAIL")
            self.logger.error(f"Erro ao recuperar chave '{version}': {e}", exc_info=True)
            raise KeyManagementError(f"Falha ao recuperar chave: {e}")

    def get_active_key(self):
        try:
            current_key_info = self.security_manager.get_secret("current_encryption_key")
            if not current_key_info:
                raise ConfigurationError("Chave de criptografia atual não definida no Vault.")
            key_value_b64 = current_key_info.get("value")
            if not key_value_b64:
                raise ConfigurationError("Valor da chave atual não encontrado no Vault.")
            if self.audit: self.audit.log(f"Chave ativa recuperada do Vault.", action="ACTIVE_KEY_GET")
            return key_value_b64
        except SecuritySystemBaseError:
            raise
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao obter a chave criptográfica ativa do Vault: {e}", level="CRITICAL", action="ACTIVE_KEY_FAIL")
            self.logger.error(f"Erro ao obter chave ativa: {e}", exc_info=True)
            raise KeyManagementError(f"Falha ao obter chave ativa: {e}")

    def rotate_key(self, key_lifetime_days=90):
        try:
            if self.audit: self.audit.log("Iniciando rotação de chave criptográfica.", action="KEY_ROTATION_START")
            self.logger.info("Iniciando rotação de chave criptográfica...")
            old_key_info = self.security_manager.get_secret("current_encryption_key")
            old_key_version = None
            old_key_value = None
            if old_key_info:
                old_key_version = old_key_info.get("version")
                old_key_value_b64 = old_key_info.get("value")
                if old_key_value_b64:
                    old_key_value = base64.urlsafe_b64decode(old_key_value_b64)
            new_key_value_raw = self._generate_new_key()
            new_key_value_b64 = base64.urlsafe_b64encode(new_key_value_raw).decode('utf-8')
            new_key_version = f"v{datetime.now().strftime('%Y%m%d%H%M%S')}"
            new_key_expires_at = datetime.now() + timedelta(days=key_lifetime_days)
            self._store_key(new_key_version, new_key_value_raw, new_key_expires_at)
            new_current_key_info = {
                "version": new_key_version, "value": new_key_value_b64,
                "created_at": datetime.now().isoformat(), "expires_at": new_key_expires_at.isoformat()
            }
            self.security_manager.add_secret("current_encryption_key", new_current_key_info)
            if old_key_value and old_key_version:
                self._store_key(old_key_version, old_key_value)
            if self.audit: self.audit.log(f"Chave rotacionada. Anterior: {old_key_version}, Nova: {new_key_version}.",
                           level="INFO", action="KEY_ROTATION_SUCCESS", resource="encryption_key",
                           details={"old_key_version": old_key_version, "new_key_version": new_key_version})
            self.logger.info(f"Chave rotacionada. Nova versão: {new_key_version}.")
            return new_key_version
        except SecuritySystemBaseError as e:
            if self.audit: self.audit.log(f"Falha na rotação da chave: {e}", level="CRITICAL", action="KEY_ROTATION_FAIL", details={"error": str(e)})
            self.logger.error(f"Falha na rotação da chave: {e}", exc_info=True)
            raise KeyManagementError(f"Falha na rotação da chave: {e}")
        except Exception as e:
            if self.audit: self.audit.log(f"Erro inesperado na rotação da chave: {e}", level="CRITICAL", action="KEY_ROTATION_UNEXPECTED_FAIL", details={"error": str(e)})
            self.logger.error(f"Erro inesperado na rotação da chave: {e}", exc_info=True)
            raise KeyManagementError(f"Erro inesperado na rotação da chave: {e}")

    def get_key_for_decryption(self, version):
        try:
            key_value_raw = self._get_key_from_db(version)
            if key_value_raw:
                return Fernet(key_value_raw)
            current_key_info = self.security_manager.get_secret("current_encryption_key")
            if current_key_info and current_key_info.get("version") == version:
                key_value_b64 = current_key_info.get("value")
                if key_value_b64:
                    return Fernet(base64.urlsafe_b64decode(key_value_b64))
            raise KeyManagementError(f"Chave versão '{version}' não encontrada.")
        except Exception as e:
            if self.audit: self.audit.log(f"Erro ao obter chave versão '{version}' para descriptografia: {e}", level="CRITICAL", action="KEY_DEC_FAIL")
            self.logger.error(f"Erro ao obter chave para descriptografia '{version}': {e}", exc_info=True)
            raise KeyManagementError(f"Falha ao obter chave para descriptografia: {e}")

    def cleanup_old_keys(self, retain_days: int):
        if self.audit: self.audit.log(f"Iniciando limpeza de chaves mais antigas que {retain_days} dias (simulação).", action="KEY_CLEANUP_START")
        self.logger.info(f"Simulação: Limpeza de chaves antigas (reter por {retain_days} dias) concluída.")
        return 0

    def __del__(self):
        try:
            if hasattr(self, 'audit') and self.audit is not None:
                 self.audit.log("KeyRotator sendo finalizado.", action="KEY_ROTATOR_SHUTDOWN")
        except Exception: pass
        try:
            if hasattr(self, 'logger') and self.logger:
                handlers = self.logger.handlers[:]
                for handler in handlers:
                    handler.close()
                    self.logger.removeHandler(handler)
        except Exception: pass
