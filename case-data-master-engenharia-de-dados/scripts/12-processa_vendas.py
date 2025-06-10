"""
Script de processamento de vendas com PySpark e integração MinIO
Desenvolvido para demonstração de pipeline de dados escalável
"""

from pyspark.sql import SparkSession
import boto3
from pathlib import Path
import shutil
import os
import sys
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def upload_para_minio(caminho_local, bucket_name, caminho_minio):
    """
    Faz upload de arquivo para MinIO usando protocolo S3.
    
    Instrução:
    Este script espera que as credenciais do MinIO estejam definidas como
    variáveis de ambiente para maior segurança.
    Ex: export MINIO_ENDPOINT_URL="http://localhost:9000"
        export MINIO_ACCESS_KEY="minioadmin"  
        export MINIO_SECRET_KEY="minioadmin"
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT_URL")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    if not all([minio_endpoint, access_key, secret_key]):
        logger.error("ERRO: Variáveis de ambiente MINIO_ENDPOINT_URL, MINIO_ACCESS_KEY, e MINIO_SECRET_KEY são obrigatórias.")
        sys.exit(1)

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=False 
        )
        s3.upload_file(caminho_local, bucket_name, caminho_minio)
        logger.info(f"Upload realizado com sucesso: {caminho_local} -> s3://{bucket_name}/{caminho_minio}")
    except Exception as e:
        logger.error(f"ERRO no upload para o MinIO: {e}")
        raise

def main():
    """Função principal do processamento de dados."""
    logger.info("=== Iniciando Pipeline de Processamento de Vendas ===")
    
    # Inicialização do Spark
    logger.info("Iniciando a sessão Spark...")
    spark = SparkSession.builder \
        .appName("ProcessaConsolidados") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    logger.info("Sessão Spark criada com sucesso.")

    try:
        # Caminhos configuráveis via placeholder
        caminho_dados_consolidados = "{{AIRFLOW_HOME}}/data/olist/dados_consolidados.csv"
        caminho_local_dir_saida = "{{AIRFLOW_HOME}}/data/olist/processa_final_spark"

        logger.info(f"Lendo dados de: {caminho_dados_consolidados}")
        df_vendas = spark.read.csv(caminho_dados_consolidados, header=True, inferSchema=True)
        
        # Validação dos dados
        total_records = df_vendas.count()
        logger.info(f"Total de registros carregados: {total_records}")
        
        if total_records == 0:
            raise ValueError("Dataset vazio - processamento abortado")

        # Processamento dos dados
        logger.info("Aplicando transformações nos dados...")
        df_processado = df_vendas.select(
            "order_id", 
            "price", 
            "customer_state", 
            "product_category_name"
        ).filter(df_vendas.price.isNotNull())

        # Preparação do diretório de saída
        final_path = Path(caminho_local_dir_saida) / "consolidado_vendas_processado.csv"
        
        if os.path.exists(caminho_local_dir_saida):
            shutil.rmtree(caminho_local_dir_saida)
        os.makedirs(caminho_local_dir_saida)

        # Escrita dos dados processados
        logger.info(f"Salvando dados processados em: {caminho_local_dir_saida}")
        df_processado.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(caminho_local_dir_saida)

        # Renomeação do arquivo final
        part_files = list(Path(caminho_local_dir_saida).glob("part-*.csv"))
        if not part_files:
            raise FileNotFoundError("Nenhum arquivo 'part-' encontrado após processamento")
            
        shutil.move(str(part_files[0]), str(final_path))
        logger.info(f"Arquivo final gerado: {final_path}")

        # Upload para MinIO
        bucket_destino = "analytics-bucket"
        caminho_minio_destino = "processed/consolidado_vendas.csv"
        
        logger.info(f"Enviando para MinIO: s3://{bucket_destino}/{caminho_minio_destino}")
        upload_para_minio(str(final_path), bucket_destino, caminho_minio_destino)

        # Limpeza
        shutil.rmtree(caminho_local_dir_saida)
        logger.info("Limpeza de arquivos temporários concluída.")
        
        logger.info("=== Pipeline executado com sucesso! ===")

    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Sessão Spark encerrada.")

if __name__ == "__main__":
    main()
