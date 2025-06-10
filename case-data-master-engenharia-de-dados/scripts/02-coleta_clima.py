# ===============================================================================
# SCRIPT STANDALONE DE COLETA - DADOS CLIMÁTICOS (OpenWeatherMap)
# ===============================================================================
# Este script simula a coleta independente de dados climáticos para múltiplas
# cidades brasileiras, utilizando a API pública da OpenWeatherMap.
#
# 💡 Instrução:
# 1. Substitua o valor de `API_KEY` abaixo por uma chave válida.
# 2. O caminho `{{AIRFLOW_HOME}}` será configurado via `configure.py`.
#
# Segurança: Nenhuma chave real é exposta em produção — uso exclusivo para demo.
# ===============================================================================

import requests
import pandas as pd
import os

# === 🔐 CHAVE DA API (preenchida apenas para demonstração local) ===
API_KEY = "SUA_CHAVE_API_OPENWEATHERMAP_AQUI"  # Substituir manualmente

# === 🌆 Cidades-alvo da coleta com seus respectivos códigos da OpenWeatherMap ===
cidades = {
    "São Paulo": 3448439,
    "Rio de Janeiro": 3451190,
    "Belo Horizonte": 3470127,
    "Porto Alegre": 3452925,
    "Recife": 3390760
}

dados_clima = []
print("🌦️ Iniciando coleta de dados climáticos...")

# === 🔄 Loop principal de coleta ===
for cidade, codigo in cidades.items():
    try:
        print(f"   -> Coletando dados para {cidade}...")
        url = f"https://api.openweathermap.org/data/2.5/weather?id={codigo}&appid={API_KEY}&lang=pt_br&units=metric"
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Garante que o status HTTP foi 200
        clima = response.json()

        dados_clima.append({
            "cidade": cidade,
            "temperatura": clima["main"]["temp"],
            "condicao": clima["weather"][0]["description"]
        })

    except Exception as e:
        print(f"   ❌ Erro ao coletar dados para {cidade}: {e}")

# === 🧾 Convertendo os dados para DataFrame ===
if dados_clima:
    df_clima = pd.DataFrame(dados_clima)

    # === 💾 Salvando em disco ===
    save_path = "{{AIRFLOW_HOME}}/data/clima"  # Substituído via configure.py
    os.makedirs(save_path, exist_ok=True)

    output_file = os.path.join(save_path, "clima_standalone.csv")
    df_clima.to_csv(output_file, index=False)

    print(f"\n✅ Dados climáticos salvos com sucesso em: {output_file}")
else:
    print("\n⚠️ Nenhum dado climático foi coletado devido a erros. O arquivo CSV não foi gerado.")

