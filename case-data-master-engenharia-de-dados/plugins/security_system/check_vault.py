#!/usr/bin/env python3
"""
Script de Verificação do Sistema de Vault de Segurança
=====================================================

Este script verifica se os segredos foram populados corretamente no vault
e testa a integridade do sistema de segurança customizado.

Instruções:
- Antes de executar, certifique-se de que 'setup_vault_secrets.py' foi executado
- Use a mesma SECRET_KEY utilizada no setup inicial
- Execute a partir da raiz do projeto: python3 plugins/security_system/check_vault.py
"""

import os
import sys
from security_system.vault import AirflowSecurityManager
from security_system.audit import AuditLogger

# --- CONFIGURAÇÕES DO AMBIENTE ---
VAULT_DB_PATH = "{{AIRFLOW_HOME}}/data/security_vault.db"
SECRET_KEY = "A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT"
AUDIT_LOG_PATH_FOR_TEST = "{{AIRFLOW_HOME}}/logs/security_audit/audit_test.csv"
SYSTEM_LOG_PATH_FOR_TEST = "{{AIRFLOW_HOME}}/logs/security_audit/system_test.log"

def create_directories():
    """Cria os diretórios necessários para logs de auditoria."""
    try:
        os.makedirs(os.path.dirname(AUDIT_LOG_PATH_FOR_TEST), exist_ok=True)
        os.makedirs(os.path.dirname(SYSTEM_LOG_PATH_FOR_TEST), exist_ok=True)
        return True
    except Exception as e:
        print(f"❌ Erro ao criar diretórios: {e}")
        return False

def verify_vault_secrets():
    """Verifica se todos os segredos críticos estão presentes no vault."""
    print("🔍 --- Verificação de Integridade do Vault de Segurança ---\n")
    
    if not create_directories():
        return False
    
    try:
        # Inicializa componentes de segurança
        test_audit_logger = AuditLogger(
            audit_file_path=AUDIT_LOG_PATH_FOR_TEST,
            system_log_file_path=SYSTEM_LOG_PATH_FOR_TEST
        )

        security_manager = AirflowSecurityManager(
            vault_db_path=VAULT_DB_PATH,
            secret_key=SECRET_KEY,
            audit_logger=test_audit_logger 
        )
        
        # Lista de segredos críticos para verificar
        secrets_to_check = [
            ("minio_local_credentials", "Credenciais MinIO (Object Storage)"),
            ("postgres_indicativos_credentials", "Credenciais PostgreSQL (Indicativos)"),
            ("postgres_datamart_credentials", "Credenciais PostgreSQL (Data Mart)"),
            ("openweathermap_api_key", "Chave API OpenWeatherMap")
        ]
        
        all_secrets_found = True
        
        for secret_key, description in secrets_to_check:
            secret_value = security_manager.get_secret(secret_key)
            
            if secret_value:
                if isinstance(secret_value, dict):
                    status = f"✅ ENCONTRADO ({len(secret_value)} chaves)"
                else:
                    status = "✅ ENCONTRADO"
                print(f"{description:<40} {status}")
            else:
                print(f"{description:<40} ❌ NÃO ENCONTRADO")
                all_secrets_found = False
        
        print("\n" + "="*60)
        
        if all_secrets_found:
            print("🎉 SUCESSO: Todos os segredos foram encontrados no vault!")
            print("   O sistema de segurança está funcionando corretamente.")
        else:
            print("⚠️  ATENÇÃO: Alguns segredos não foram encontrados.")
            print("   Execute 'scripts/setup_vault_secrets.py' primeiro.")
            
        return all_secrets_found
        
    except Exception as e:
        print(f"❌ ERRO CRÍTICO ao acessar o Vault: {e}")
        print("   Verifique se a SECRET_KEY e os caminhos estão corretos.")
        return False

if __name__ == "__main__":
    print("Sistema de Verificação do Vault de Segurança\n")
    
    # Verifica se está sendo executado do diretório correto
    if not os.path.exists("airflow.cfg"):
        print("❌ ERRO: Execute este script a partir da raiz do projeto.")
        sys.exit(1)
    
    success = verify_vault_secrets()
    sys.exit(0 if success else 1)
