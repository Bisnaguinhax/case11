# ===================================================================================
# DAG DE VALIDAÇÃO DE DADOS COM GREAT EXPECTATIONS - DEMONSTRAÇÃO SEGURA
# ===================================================================================
# Esta DAG executa uma suíte de validação do Great Expectations sobre os
# dados consolidados, atuando como um "Quality Gate" no pipeline.
#
# 🔐 SEGURANÇA E GOVERNANÇA:
# - Integração completa com o sistema de auditoria para rastrear os resultados.
# - Uso de exceções customizadas para falhar a task de forma informativa.
#
# 📌 INSTRUÇÕES:
# 1. Garanta que o ficheiro `dados_consolidados.csv` existe.
# 2. Garanta que a suíte de expectativas `vendas.json` está na pasta `dags/expectations`.
# 3. Execute a DAG para verificar a qualidade dos dados.
# ===================================================================================

from __future__ import annotations
import pendulum
import great_expectations as ge
import json
import os

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def _valida_vendas_ge(**kwargs):
    """Executa uma suíte de validação do Great Expectations com auditoria completa."""
    from plugins.security_system.audit import AuditLogger
    from plugins.security_system.exceptions import ValidationError

    # Configuração da auditoria
    AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
    SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
    audit = AuditLogger(AUDIT_LOG_PATH, SYSTEM_LOG_PATH)
    
    dag_id = kwargs.get('dag_run').dag_id
    audit.log("Iniciando task de validação de dados.", action="GE_VALIDATION_START", dag_id=dag_id)
    
    try:
        caminho_dados = "{{AIRFLOW_HOME}}/data/olist/dados_consolidados.csv"
        caminho_expectations = "{{AIRFLOW_HOME}}/dags/expectations/vendas.json"

        print(f"📄 Carregando dados de: {caminho_dados}")
        df = ge.read_csv(caminho_dados)

        print(f"📦 Lendo suíte de expectativas de: {caminho_expectations}")
        with open(caminho_expectations, "r") as f:
            expectation_suite = json.load(f)

        print("⚙️ Executando validação...")
        validation_result = df.validate(expectation_suite=expectation_suite)
        
        audit.log_validation(results=validation_result, metadata={"fonte_dados": caminho_dados})

        if not validation_result["success"]:
            raise ValidationError("Validação de dados com Great Expectations falhou!")

        print("✅ Validação concluída com sucesso.")

    except FileNotFoundError as e:
        error_msg = f"Erro de ficheiro não encontrado durante a validação: {e}"
        audit.log(error_msg, level="CRITICAL", action="GE_VALIDATION_FILE_NOT_FOUND", dag_id=dag_id)
        raise
    except Exception as e:
        error_msg = f"Erro inesperado durante a validação: {e}"
        audit.log(error_msg, level="CRITICAL", action="GE_VALIDATION_FAIL", dag_id=dag_id)
        raise

with DAG(
    dag_id="dag_validacao_segura_v1",
    start_date=pendulum.datetime(2025, 6, 10, tz="UTC"),
    schedule="0 2 * * *", # Diariamente às 2h
    catchup=False,
    doc_md="### Validação de Dados com Great Expectations\nExecuta testes de qualidade de dados no dataset consolidado.",
    tags=['validation', 'quality', 'great_expectations'],
) as dag:
    
    tarefa_validar = PythonOperator(
        task_id="validar_dados_consolidados_task",
        python_callable=_valida_vendas_ge,
    )
