import csv
import time
from queue import Queue

# Fila compartilhada
fila_eventos = Queue()

# Caminho do CSV
caminho_csv = "/Users/felps/airflow_data/olist/dados_consolidados.csv"

# Número máximo de eventos simulados
LIMITE_EVENTOS = 30
contador = 0

# Abrindo o arquivo CSV
with open(caminho_csv, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    
    for row in reader:
        if contador >= LIMITE_EVENTOS:
            break

        evento = {
            "customer_state": row["customer_state"],
            "price": float(row["price"])
        }

        fila_eventos.put(evento)
        print(f"📤 Evento enviado: {evento}")
        contador += 1
        time.sleep(0.5)  # Delay para simular streaming
