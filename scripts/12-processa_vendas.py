from pyspark.sql import SparkSession
import boto3
from pathlib import Path
import shutil
import os
import sys

def upload_para_minio(caminho_local, bucket_name, caminho_minio):
    minio_endpoint = os.getenv("MINIO_ENDPOINT_URL")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([minio_endpoint, access_key, secret_key]):
        print("ERRO: Variáveis de ambiente do MinIO são obrigatórias.")
        sys.exit(1)

    try:
        # Adicionado verify=False para lidar com certificados autoassinados em HTTPS
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=False 
        )
        s3.upload_file(caminho_local, bucket_name, caminho_minio)
        print(f"Enviado via script Spark: {caminho_local} -> s3://{bucket_name}/{caminho_minio}")
    except Exception as e:
        print(f"ERRO no upload para o MinIO: {e}")
        raise

print("Iniciando a sessão Spark...")
spark = SparkSession.builder.appName("ProcessaConsolidados").getOrCreate()
print("Sessão Spark criada com sucesso.")

# O script lê o arquivo consolidado para processá-lo e enviá-lo
print("Lendo o arquivo de dados consolidados...")
df_vendas = spark.read.csv("/Users/felps/airflow_data/olist/dados_consolidados.csv", header=True, inferSchema=True)
print("Arquivo lido com sucesso.")

# Exemplo de processamento simples 
df_processado = df_vendas.select("order_id", "price", "customer_state", "product_category_name")

caminho_local_dir = "/Users/felps/airflow_data/olist/processa_final_spark"
final_path = Path(caminho_local_dir) / "consolidado_vendas_processado.csv"

print(f"Salvando resultado processado em: {caminho_local_dir}...")
if os.path.exists(caminho_local_dir):
    shutil.rmtree(caminho_local_dir)
os.makedirs(caminho_local_dir)

df_processado.coalesce(1).write.mode("overwrite").option("header", "true").csv(caminho_local_dir)

part_files = list(Path(caminho_local_dir).glob("part-*.csv"))
if part_files:
    shutil.move(str(part_files[0]), str(final_path))
    print(f"Arquivo renomeado para: {final_path}")
else:
    print("ERRO: Nenhum arquivo 'part-' encontrado para renomear.")
    spark.stop()
    sys.exit(1)


bucket_destino = "s-prd.sand-ux-indc-brasil"
caminho_minio_destino = "consolidado_vendas" 

print(f"Enviando para o MinIO em: s3://{bucket_destino}/{caminho_minio_destino}...")
upload_para_minio(str(final_path), bucket_destino, caminho_minio_destino)

shutil.rmtree(caminho_local_dir)
print("Pasta temporária removida.")

spark.stop()
print("Sessão Spark encerrada com sucesso.")
