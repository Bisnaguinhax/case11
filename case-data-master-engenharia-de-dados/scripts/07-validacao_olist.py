#!/usr/bin/env python3
# ================================================================================
# VALIDAÇÃO DE DADOS - Great Expectations
# ================================================================================
# Este script realiza validações de qualidade no dataset consolidado `dados_consolidados.csv`.
# Ele é executado independentemente da DAG e simula um check pré-carga no Data Mart.
#
# 📌 Instruções:
# - O caminho do arquivo é configurável e será setado automaticamente pelo `configure.py`.
# - As regras são simples, mas demonstram como pode definir expectativas formais.
# ================================================================================

import great_expectations as ge
import sys

# Caminho para os dados consolidados (será substituído pelo configure.py)
caminho_dados = "{{AIRFLOW_HOME}}/data/olist/dados_consolidados.csv"

print(f"📂 Carregando dados de: {caminho_dados}...")
try:
    df = ge.read_csv(caminho_dados)
except FileNotFoundError:
    print(f"❌ ERRO: Arquivo não encontrado em '{caminho_dados}'.")
    print("   -> Certifique-se de que a DAG 'dag_consolida_dados_olist_v1' foi executada primeiro.")
    sys.exit(1)


# --- Definição das expectativas ---
print("\n📏 Definindo expectativas de qualidade dos dados...")

# Expectativa 1: order_id não deve conter valores nulos (chave primária)
df.expect_column_values_to_not_be_null(
    "order_id",
    result_format="SUMMARY"
)
print("  ✅ Regra 1: `order_id` não pode ser nulo.")

# Expectativa 2: order_status deve estar entre valores válidos
df.expect_column_values_to_be_in_set(
    "order_status",
    ["delivered", "shipped", "canceled", "invoiced", "processing", "approved", "unavailable"],
    result_format="SUMMARY"
)
print("  ✅ Regra 2: `order_status` deve conter apenas valores reconhecidos.")

# --- Execução das validações ---
print("\n⚙️  Executando validação com base nas regras definidas...")
validation_result = df.validate()

# --- Exibição do resultado ---
print("\n📊 Validação finalizada. Resultado resumido:")
# O Great Expectations já tem uma ótima representação em string
print(validation_result)

# --- Checagem final: falhas são consideradas erro de execução (exit code != 0) ---
if not validation_result["success"]:
    print("\n❌ VALIDAÇÃO FALHOU: Uma ou mais regras foram violadas. Reveja os dados.")
    sys.exit(1)
else:
    print("\n✅ VALIDAÇÃO APROVADA: Todos os testes passaram com sucesso.")
    sys.exit(0)
