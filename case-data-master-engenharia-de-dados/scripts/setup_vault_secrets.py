import os
import sys
from dotenv import load_dotenv

plugins_path = os.path.join(os.path.dirname(__file__), '..', 'plugins')
sys.path.insert(0, os.path.abspath(plugins_path))

try:
    from security_system.audit import AuditLogger 
    from security_system.vault import AirflowSecurityManager 
except ImportError as e:
    print(f"Erro de importação: {e}")
    print(f"Verifique se o pacote 'security_system' está acessível a partir de: {os.path.abspath(plugins_path)}")
    print("Ou ajuste o sys.path acima para apontar para o diretório que contém 'security_system'.")
    sys.exit(1)

dotenv_file_path = '/Users/felps/airflow/config/security.env'
print(f"Tentando carregar .env de: {dotenv_file_path}")
loaded = load_dotenv(dotenv_file_path, override=True)
if not loaded:
    print(f"AVISO: Não foi possível carregar o arquivo .env de {dotenv_file_path}")

print("Configurando segredos no Vault...")
try:
    audit = AuditLogger() 
    sec_manager = AirflowSecurityManager(audit_logger=audit) 

    API_KEY_OPENWEATHER = "005a8da0a976ff926b5edca0d80f363f"
    NOME_DO_SEGREDO_NO_VAULT = "openweathermap_api_key"

    sec_manager.add_secret(NOME_DO_SEGREDO_NO_VAULT, API_KEY_OPENWEATHER)
    print(f"Segredo '{NOME_DO_SEGREDO_NO_VAULT}' adicionado/atualizado no vault.")
    print("Script de configuração de segredos concluído!")

except Exception as e_main:
    print(f"Ocorreu um erro geral ao executar o script de setup: {e_main}")
    import traceback
    traceback.print_exc()
