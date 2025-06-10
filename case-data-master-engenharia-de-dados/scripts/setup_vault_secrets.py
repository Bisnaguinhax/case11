#!/usr/bin/env python3
"""
Script de configuração do Security Vault - Demonstração Técnica
================================================================

Este script popula o cofre de segurança com credenciais de exemplo.
Para a banca avaliadora: substitua os valores placeholder pelas 
suas configurações reais antes da execução.

Uso: python3 scripts/setup_vault_secrets.py
"""

import os
import sys
import traceback
from pathlib import Path

# --- CONFIGURAÇÕES DO SISTEMA ---
# Placeholders que serão substituídos pelo configure.py
SECRET_KEY = 'demo_key_replace_in_production_with_openssl_rand_base64_32'
PLUGINS_PATH = '{{AIRFLOW_HOME}}/plugins'
AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'

def ensure_directories():
    """Cria os diretórios necessários se não existirem."""
    dirs_to_create = [
        os.path.dirname(AUDIT_LOG_PATH),
        os.path.dirname(VAULT_DB_PATH)
    ]
    
    for dir_path in dirs_to_create:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"   -> Diretório garantido: {dir_path}")

def setup_python_path():
    """Adiciona o diretório de plugins ao Python path."""
    plugins_abs_path = os.path.abspath(PLUGINS_PATH)
    if plugins_abs_path not in sys.path:
        sys.path.insert(0, plugins_abs_path)
        print(f"   -> Plugins path adicionado: {plugins_abs_path}")

def import_security_modules():
    """Importa os módulos de segurança necessários."""
    try:
        from security_system.audit import AuditLogger
        from security_system.vault import AirflowSecurityManager
        return AuditLogger, AirflowSecurityManager
    except ImportError as e:
        print(f"❌ ERRO: Não foi possível importar módulos de segurança")
        print(f"   -> Verifique se os arquivos estão em: {PLUGINS_PATH}")
        print(f"   -> Erro específico: {e}")
        sys.exit(1)

def setup_credentials(sec_manager):
    """Configura as credenciais no vault."""
    print("\n📝 Adicionando credenciais de exemplo...")
    
    # --- CREDENCIAIS DE EXEMPLO PARA DEMONSTRAÇÃO ---
    # Em produção, estes valores viriam de variáveis de ambiente
    
    credentials = {
        "openweathermap_api_key": {
            "value": "demo_api_key_substitua_pela_real",
            "description": "API key para OpenWeatherMap"
        },
        "minio_local_credentials": {
            "value": {
                "endpoint_url": "http://localhost:9000",
                "access_key": "demo_access_key",
                "secret_key": "demo_secret_key"
            },
            "description": "Credenciais do MinIO local"
        },
        "postgres_indicativos_credentials": {
            "value": {
                "user": "demo_user",
                "password": "demo_password",
                "host": "localhost",
                "port": "5432",
                "dbname": "indicativos"
            },
            "description": "Credenciais PostgreSQL - Base Indicativos"
        },
        "postgres_datamart_credentials": {
            "value": {
                "dbname": "dm_st_gestao_indicadores",
                "user": "demo_user_dm",
                "password": "demo_password_dm",
                "host": "localhost",
                "port": "5432"
            },
            "description": "Credenciais PostgreSQL - Data Mart"
        }
    }
    
    for key, config in credentials.items():
        sec_manager.add_secret(key, config["value"])
        print(f"   -> ✅ '{key}' configurado ({config['description']})")

def main():
    """Função principal do script."""
    print("🔐 === CONFIGURAÇÃO DO SECURITY VAULT ===")
    print("   Demonstração técnica - Sistema de segurança customizado\n")
    
    try:
        print("🔧 [1/4] Preparando ambiente...")
        ensure_directories()
        setup_python_path()
        
        print("\n📦 [2/4] Importando módulos...")
        AuditLogger, AirflowSecurityManager = import_security_modules()
        
        print("\n🏗️  [3/4] Inicializando componentes...")
        audit = AuditLogger(
            audit_file_path=AUDIT_LOG_PATH,
            system_log_file_path=SYSTEM_LOG_PATH
        )
        
        sec_manager = AirflowSecurityManager(
            vault_db_path=VAULT_DB_PATH,
            secret_key=SECRET_KEY,
            audit_logger=audit
        )
        print("   -> Security Manager inicializado")
        
        print("\n🔑 [4/4] Configurando credenciais...")
        setup_credentials(sec_manager)
        
        print("\n🎉 === CONFIGURAÇÃO CONCLUÍDA COM SUCESSO ===")
        print("   -> Vault populado com credenciais de demonstração")
        print("   -> Logs de auditoria configurados")
        print("   -> Sistema pronto para uso!")
        
    except Exception as e:
        print(f"\n❌ === ERRO DURANTE A CONFIGURAÇÃO ===")
        print(f"   -> {e}")
        print(f"\n📋 Stack trace completo:")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
