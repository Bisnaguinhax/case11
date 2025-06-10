#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Simulador de Stream de Vendas - Demonstração Técnica
====================================================

Este módulo simula um stream de dados de vendas em tempo real,
lendo de um arquivo CSV e enviando eventos para uma fila.

Desenvolvido para demonstração de processamento de dados em streaming
com integração ao Apache Airflow.
"""

import csv
import time
import sys
import os
from queue import Queue
from datetime import datetime

# Fila compartilhada para eventos de vendas
fila_eventos = Queue()

# Configuração do simulador
CAMINHO_CSV = "{{AIRFLOW_HOME}}/data/olist/dados_consolidados.csv"
LIMITE_EVENTOS = 30
DELAY_STREAMING = 0.5  # segundos entre eventos

def validar_arquivo_csv():
    """Valida se o arquivo CSV existe e é acessível."""
    if not os.path.exists(CAMINHO_CSV):
        print(f"❌ ERRO: Arquivo não encontrado: {CAMINHO_CSV}")
        print("   -> Verifique se o script configure.py foi executado.")
        return False
    
    if not os.access(CAMINHO_CSV, os.R_OK):
        print(f"❌ ERRO: Sem permissão de leitura: {CAMINHO_CSV}")
        return False
    
    return True

def simular_stream_vendas():
    """
    Simula um stream de vendas lendo dados do CSV e enviando para fila.
    
    Returns:
        int: Número de eventos processados
    """
    print("🚀 Iniciando simulador de stream de vendas...")
    print(f"📂 Arquivo fonte: {CAMINHO_CSV}")
    print(f"📊 Limite de eventos: {LIMITE_EVENTOS}")
    print("-" * 50)
    
    contador = 0
    eventos_processados = 0
    
    try:
        with open(CAMINHO_CSV, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Valida se as colunas necessárias existem
            required_columns = ['customer_state', 'price']
            if not all(col in reader.fieldnames for col in required_columns):
                print(f"❌ ERRO: Colunas obrigatórias não encontradas: {required_columns}")
                print(f"   -> Colunas disponíveis: {reader.fieldnames}")
                return 0
            
            for row in reader:
                if contador >= LIMITE_EVENTOS:
                    break
                
                try:
                    # Validação e conversão dos dados
                    price = float(row["price"]) if row["price"] else 0.0
                    state = row["customer_state"].strip() if row["customer_state"] else "UNKNOWN"
                    
                    evento = {
                        "timestamp": datetime.now().isoformat(),
                        "customer_state": state,
                        "price": price,
                        "event_id": contador + 1
                    }
                    
                    fila_eventos.put(evento)
                    print(f"📤 Evento {contador + 1:02d}: Estado={state:<2} | Valor=R$ {price:>8.2f}")
                    
                    contador += 1
                    eventos_processados += 1
                    
                    # Simula delay de streaming
                    time.sleep(DELAY_STREAMING)
                    
                except (ValueError, KeyError) as e:
                    print(f"⚠️  Linha {contador + 1} ignorada (dados inválidos): {e}")
                    contador += 1
                    continue
                    
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo não encontrado: {CAMINHO_CSV}")
        return 0
    except Exception as e:
        print(f"❌ ERRO INESPERADO: {e}")
        return 0
    
    print("-" * 50)
    print(f"✅ Simulação concluída!")
    print(f"📊 Total de eventos enviados: {eventos_processados}")
    print(f"📥 Eventos na fila: {fila_eventos.qsize()}")
    
    return eventos_processados

if __name__ == "__main__":
    print("=" * 60)
    print("    SIMULADOR DE STREAM DE VENDAS - DEMO TÉCNICA")
    print("=" * 60)
    
    # Validação inicial
    if not validar_arquivo_csv():
        sys.exit(1)
    
    # Executa a simulação
    eventos_enviados = simular_stream_vendas()
    
    if eventos_enviados > 0:
        print(f"\n🎉 Simulação executada com sucesso!")
        print(f"   -> Use a fila 'fila_eventos' para processar os dados.")
    else:
        print(f"\n❌ Falha na simulação.")
        sys.exit(1)
