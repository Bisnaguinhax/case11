import great_expectations as ge
import json

# 📄 Carregando os dados de vendas
print("📄 Carregando o arquivo CSV com os dados de vendas...")
df = ge.read_csv("~/airflow_data/olist/dados_consolidados.csv")  

# 📦 Carregando o arquivo de expectativas JSON
print("📦 Lendo as expectativas de '/Users/felps/airflow/dags/vendas.json'...")
with open("/Users/felps/airflow/dags/vendas.json", "r") as f:
    expectations = json.load(f)

# 🧪 Aplicando cada expectativa manualmente
print("🧪 Iniciando a aplicação das expectativas...")
for exp in expectations["expectations"]:
    expectation_type = exp["expectation_type"]
    kwargs = exp["kwargs"]
    print(f"➡️  Aplicando: {expectation_type} com {kwargs}")
    getattr(df, expectation_type)(**kwargs)

# ✅ Rodando a validação
print("✅ Executando a validação final...")
results = df.validate()
print("📊 Resultados da validação:")
print(results)
