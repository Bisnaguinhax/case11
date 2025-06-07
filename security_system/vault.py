import os
import json
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from security_system.audit import AuditLogger
from security_system.exceptions import KeyManagementError, ConfigurationError

class AirflowSecurityManager:
    def __init__(self, vault_db_path: str, secret_key: str, audit_logger: AuditLogger):
        if not all([vault_db_path, secret_key, audit_logger]):
            raise ConfigurationError("vault_db_path, secret_key, e audit_logger são obrigatórios.")
        
        self.audit = audit_logger
        self.vault_db_path = vault_db_path
        self._ensure_vault_path()

        salt = b'a_static_salt_for_key_derivation_airflow_sec'
        kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=100000, backend=default_backend())
        fernet_key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
        self.fernet = Fernet(fernet_key)
        self.vault_data = self._load_vault()
        self.audit.log("AirflowSecurityManager inicializado.", action="VAULT_INIT")

    def _ensure_vault_path(self):
        vault_dir = os.path.dirname(self.vault_db_path)
        if vault_dir and not os.path.exists(vault_dir):
            os.makedirs(vault_dir, exist_ok=True)

    def _load_vault(self):
        if os.path.exists(self.vault_db_path) and os.path.getsize(self.vault_db_path) > 0:
            with open(self.vault_db_path, 'rb') as f:
                encrypted_data = f.read()
            decrypted_data = self.fernet.decrypt(encrypted_data).decode('utf-8')
            return json.loads(decrypted_data)
        return {"secrets": {}, "service_endpoints": {}}

    def _save_vault(self):
        plain_data = json.dumps(self.vault_data, indent=4)
        encrypted_data = self.fernet.encrypt(plain_data.encode())
        with open(self.vault_db_path, 'wb') as f:
            f.write(encrypted_data)
        self.audit.log("Vault salvo e criptografado.", action="VAULT_SAVE")

    def add_secret(self, name, value):
        self.vault_data["secrets"][name] = value
        self._save_vault()
        self.audit.log(f"Segredo '{name}' adicionado/atualizado.", action="ADD_SECRET", resource=name)

    def get_secret(self, name):
        secret = self.vault_data["secrets"].get(name)
        if secret is not None:
            self.audit.log(f"Segredo '{name}' acessado.", action="GET_SECRET", resource=name)
        else:
            self.audit.log(f"Tentativa de acessar segredo inexistente: '{name}'.", level="WARNING", action="GET_SECRET_FAIL", resource=name)
        return secret

    # Manter o resto dos seus métodos (delete_secret, etc.)
    def delete_secret(self, name):
        if name in self.vault_data["secrets"]:
            del self.vault_data["secrets"][name]
            self._save_vault()
            self.audit.log(f"Segredo '{name}' removido.", action="DELETE_SECRET", resource=name)
            return True
        return False

    def add_service_endpoint(self, name, url):
        self.vault_data["service_endpoints"][name] = url
        self._save_vault()
        self.audit.log(f"Endpoint '{name}' adicionado/atualizado.", action="ADD_ENDPOINT", resource=name)

    def get_service_endpoint(self, name):
        endpoint = self.vault_data["service_endpoints"].get(name)
        if endpoint is not None:
            self.audit.log(f"Endpoint '{name}' acessado.", action="GET_ENDPOINT", resource=name)
        return endpoint

    def delete_service_endpoint(self, name):
        if name in self.vault_data["service_endpoints"]:
            del self.vault_data["service_endpoints"][name]
            self._save_vault()
            self.audit.log(f"Endpoint '{name}' removido.", action="DELETE_ENDPOINT", resource=name)
            return True
        return False
