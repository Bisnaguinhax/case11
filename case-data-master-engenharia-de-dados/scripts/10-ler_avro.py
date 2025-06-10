#!/usr/bin/env python3
# ================================================================================
# LEITURA DE AVRO
# ================================================================================
# Este script demonstra a leitura de dados gravados no formato Avro,
# utilizado em Data Lakes por sua performance e definição de schema.
# ================================================================================

import fastavro
import os
import sys

caminho_avro = "{{AIRFLOW_HOME}}/data/indicadores/ipca.avro"

print(f"📂 Lendo ficheiro Avro de: {caminho_avro}\n")
if not os.path.exists(caminho_avro):
    print("❌ Arquivo Avro não encontrado. Execute o script '09-escrever_avro.py' primeiro.")
    sys.exit(1)

# Leitura dos registros Avro com tratamento de erro
try:
    with open(caminho_avro, "rb") as avro_file:
        reader = fastavro.reader(avro_file)
        registros = list(reader)

    print(f"📊 Total de registros lidos: {len(registros)}\n")
    print("🔍 Amostra de registros (os 3 primeiros):")
    for r in registros[:3]:
        print(f"  - {r}")

    print("\n✅ Leitura do ficheiro Avro concluída.")
    sys.exit(0)

except Exception as e:
    print(f"❌ Erro ao ler o ficheiro Avro: {e}")
    sys.exit(1)
