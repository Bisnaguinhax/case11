import csv
import os
import sys
from threading import Thread
from datetime import datetime
import boto3
import traceback

# --- CONFIGURAÇÕES DEFINIDAS DIRETAMENTE NO SCRIPT ---
SECRET_KEY = 'Nggk-vXHT7kr3M4VLLGeYixWcOrjMu505Q90fjzO7A0='
AUDIT_LOG_PATH = '/Users/felps/airflow/logs/security_audit/audit.csv'
SYSTEM_LOG_PATH = '/Users/felps/airflow/logs/security_audit/system.log'
VAULT_DB_PATH = '/Users/felps/airflow/data/security_vault.db'

# --- LÓGICA DE IMPORTAÇÃO ---
plugins_path = '/Users/felps/airflow/plugins'
if plugins_path not in sys.path:
    sys.path.insert(0, os.path.abspath(plugins_path))

try:
    from security_system.audit import AuditLogger
    from security_system.vault import AirflowSecurityManager
    from security_system.exceptions import ConfigurationError
except ImportError as e:
    print(f"ERRO DE IMPORTAÇÃO: {e}. Verifique o sys.path e a localização do pacote 'security_system'.")
    sys.exit(1)

try:
    from simulador_stream_vendas import fila_eventos
except ImportError:
    print("ERRO: Execute o 'simulador_stream_vendas.py' primeiro ou garanta que ele esteja no mesmo diretório.")
    sys.exit(1)


# --- CLASSE DO PROCESSADOR SEGURO ---

class SecureStreamProcessor:
    def __init__(self):
        print("Inicializando o Processador de Stream Seguro...")
        # Instanciar componentes de segurança passando as configurações diretamente
        self.audit = AuditLogger(
            audit_file_path=AUDIT_LOG_PATH,
            system_log_file_path=SYSTEM_LOG_PATH
        )
        self.sec_manager = AirflowSecurityManager(
            vault_db_path=VAULT_DB_PATH,
            secret_key=SECRET_KEY,
            audit_logger=self.audit
        )

        self.s3_client = self._get_secure_minio_client()

        # Bucket de destino para os dados de stream
        self.bucket_name = "g-prd.sand-ux-indcs-brasil" 
        self.pasta_saida_local = "/Users/felps/airflow_data/olist/stream_processado"
        os.makedirs(self.pasta_saida_local, exist_ok=True)
        self.nome_arquivo_local = "vendas_stream.csv"
        self.caminho_local = os.path.join(self.pasta_saida_local, self.nome_arquivo_local)
        self.caminho_minio = f"stream_processado/{self.nome_arquivo_local}"

        self._init_local_csv()
        self.audit.log("Processador de Stream Seguro inicializado com sucesso.", action="STREAM_PROCESSOR_INIT")

    def _get_secure_minio_client(self):
        self.audit.log("Buscando credenciais do MinIO no Vault para o processador de stream...", action="GET_MINIO_CREDS_STREAM")
        minio_creds = self.sec_manager.get_secret("minio_local_credentials")
        if not minio_creds or not isinstance(minio_creds, dict):
            raise ConfigurationError("Credenciais do MinIO não encontradas no Vault.")

        endpoint = minio_creds.get("endpoint_url")
        access_key = minio_creds.get("access_key")
        secret_key = minio_creds.get("secret_key")

        if not all([endpoint, access_key, secret_key]):
             raise ConfigurationError("Credenciais do MinIO no Vault estão incompletas.")

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=False # Para certificados autoassinados em ambiente local
        )
        self.audit.log("Cliente MinIO seguro criado com sucesso para o processador de stream.", action="MINIO_CLIENT_CREATED_STREAM")
        return s3

    def _init_local_csv(self):
        # Inicializa (ou limpa) o arquivo local no início de cada execução
        with open(self.caminho_local, "w", newline="", encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["customer_state", "price", "timestamp"])
            writer.writeheader()
        self.audit.log(f"Arquivo CSV local para o stream inicializado/limpo: {self.caminho_local}", action="STREAM_CSV_INIT")

    def processar_evento(self, evento):
        # Lógica de negócio: processar apenas vendas acima de 100
        if evento.get("price", 0) > 100:
            evento["timestamp"] = datetime.now().isoformat()

            # Escrever no CSV local
            with open(self.caminho_local, "a", newline="", encoding='utf-8') as f:
                # Garantir que o DictWriter tenha as mesmas chaves que o evento
                # O evento pode ter mais chaves, então é melhor definir as chaves explicitamente
                fieldnames = ["customer_state", "price", "timestamp"]
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # Filtrar o evento para ter apenas as chaves esperadas pelo writer
                filtered_evento = {key: evento[key] for key in fieldnames if key in evento}
                writer.writerow(filtered_evento)

            print(f"✅ Evento processado e salvo localmente: {evento}")
            self.audit.log("Evento de venda caro processado e salvo localmente.", action="STREAM_EVENT_PROCESSED", details=str(evento))

            # Fazer upload do arquivo atualizado para o MinIO
            try:
                self.s3_client.upload_file(self.caminho_local, self.bucket_name, self.caminho_minio)
                print(f"📡 CSV enviado ao MinIO: s3://{self.bucket_name}/{self.caminho_minio}")
                if hasattr(self.audit, 'log_upload'):
                     self.audit.log_upload(local_path=self.caminho_local, minio_path=f"s3://{self.bucket_name}/{self.caminho_minio}")
                else:
                     self.audit.log(f"Upload para MinIO: {self.caminho_local}", action="STREAM_UPLOAD_SUCCESS")
            except Exception as e:
                print(f"❌ Erro ao enviar para MinIO: {e}")
                self.audit.log(f"Falha no upload para o MinIO: {e}", level="ERROR", action="STREAM_UPLOAD_FAIL")

    def start(self):
        print("🚀 Processador de Stream iniciado. Aguardando eventos... (Pressione Ctrl+C para parar)")
        while True:
            try:
                evento = fila_eventos.get() # Bloqueia até um item estar disponível
                if evento is None: # Sinal de parada (não usado pelo simulador atual, mas boa prática)
                    break

                self.processar_evento(evento)
            except KeyboardInterrupt: # Permite parar com Ctrl+C
                break

        print("\nSinal de parada recebido. Encerrando o processador.")
        self.audit.log("Processador de Stream encerrado.", action="STREAM_PROCESSOR_STOP")


if __name__ == "__main__":
    try:
        processor = SecureStreamProcessor()
        processor.start()
    except Exception as e_main:
        print(f"ERRO FATAL AO INICIAR O PROCESSADOR: {e_main}")
        traceback.print_exc()
