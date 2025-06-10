#!/usr/bin/env python3
# ================================================================================
# CONVERSÃO PARA AVRO - Escrita
# ================================================================================
# Este script converte um CSV local contendo dados do IPCA para o formato Avro,
# demonstrando como persistir dados em formatos otimizados para Data Lake.
# ================================================================================

import fastavro
import pandas as pd
import os
import sys

caminho_base = "{{AIRFLOW_HOME}}/data/indicadores"
caminho_csv = os.path.join(caminho_base, "ipca_coletado.csv")
caminho_avro = os.path.join(caminho_base, "ipca.avro")

print(f"📄 Lendo CSV de: {caminho_csv}")
if not os.path.exists(caminho_csv):
    print("❌ Arquivo CSV não encontrado. Verifique a execução da DAG de coleta primeiro.")
    sys.exit(1)

df_ipca = pd.read_csv(caminho_csv)

# Definição de schema exigida pelo Avro
schema = {
    "type": "record",
    "name": "IPCA",
    "fields": [
        {"name": "data", "type": "string"},
        {"name": "valor", "type": "string"}  # Tipo string para garantir compatibilidade
    ]
}

print("🛠️  Convertendo registros para Avro...")
records = df_ipca.to_dict("records")
with open(caminho_avro, "wb") as avro_file:
    fastavro.writer(avro_file, schema, records)

print(f"✅ Conversão concluída. Arquivo salvo em: {caminho_avro}")
