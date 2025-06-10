# ===============================================================================
# SCRIPT STANDALONE DE COLETA - IPCA (BANCO CENTRAL)
# ===============================================================================
# Este script simula uma execução independente de coleta de dados do IPCA.

# Fonte: Banco Central do Brasil (Série Histórica 433)
# ===============================================================================

import requests
import pandas as pd
import os

# 📁 Diretório de saída (dinâmico, substituído via `configure.py`)
save_path = "{{AIRFLOW_HOME}}/data/indicadores"
os.makedirs(save_path, exist_ok=True)

# 🌐 URL da API pública do Banco Central
url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
print(f"🌍 Coletando dados do IPCA de: {url}")

try:
    # Requisição GET para obter os dados
    response = requests.get(url, timeout=10)
    response.raise_for_status()  # Levanta erro se a resposta for inválida
    dados_ipca = response.json()

    # Transforma os dados em DataFrame
    df_ipca = pd.DataFrame(dados_ipca)

    # Define caminho de saída
    output_file = os.path.join(save_path, "ipca_standalone.csv")

    # Salva como CSV
    df_ipca.to_csv(output_file, index=False)

    print(f"✅ Dados do IPCA salvos com sucesso em: {output_file}")

except Exception as e:
    print(f"❌ Erro ao coletar dados do IPCA: {e}")
