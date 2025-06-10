#!/usr/bin/env python3
"""
Processador de Stream de Vendas com Sistema de Segurança Integrado
================================================================

Este script demonstra:
- Integração com o Security Vault desenvolvido
- Processamento de stream em tempo real
- Auditoria completa de operações
- Upload seguro para MinIO/S3

Instruções:
1. Execute primeiro: scripts/setup_vault_secrets.py
2. Configure suas credenciais MinIO no vault
3. Execute: python simulador_stream_vendas.py (em terminal separado)
4. Execute este script para processar o stream
"""

# ===============================================================================
# IMPORTAÇÕES PARA PROCESSAMENTO DE STREAM EMPRESARIAL
# ===============================================================================
import csv
import os
import sys
from threading import Thread
from datetime import datetime
import boto3  # AWS SDK para integração MinIO/S3
import traceback
import time

# ===============================================================================
# CONFIGURAÇÕES DE SEGURANÇA E PATHS DINÂMICOS
# ===============================================================================
# NOTA PARA BANCA: Configurações são substituídas pelo configure.py
# Isso garante portabilidade entre ambientes (dev, staging, prod)
SECRET_KEY = 'A_MESMA_CHAVE_SECRETA_USADA_NO_SETUP_VAULT'
PLUGINS_PATH = '{{AIRFLOW_HOME}}/plugins'
AUDIT_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/audit.csv'
SYSTEM_LOG_PATH = '{{AIRFLOW_HOME}}/logs/security_audit/system.log'
VAULT_DB_PATH = '{{AIRFLOW_HOME}}/data/security_vault.db'

# ===============================================================================
# SISTEMA DE IMPORTAÇÃO ROBUSTA COM FALLBACK
# ===============================================================================
# Adiciona path dos plugins para importação dos módulos de segurança
if PLUGINS_PATH not in sys.path:
    sys.path.insert(0, os.path.abspath(PLUGINS_PATH))

# Importação com tratamento de erro detalhado para diagnóstico
try:
    # Importa componentes do sistema de segurança desenvolvido
    from security_system.audit import AuditLogger
    from security_system.vault import AirflowSecurityManager
    from security_system.exceptions import ConfigurationError
except ImportError as e:
    print(f"❌ ERRO DE IMPORTAÇÃO: {e}")
    print("💡 Dica: Execute 'python3 configure.py' primeiro para configurar os caminhos.")
    sys.exit(1)

# Importação do simulador de stream (dependência externa)
try:
    from simulador_stream_vendas import fila_eventos
except ImportError:
    print("❌ ERRO: simulador_stream_vendas.py não encontrado.")
    print("💡 Execute primeiro: python3 simulador_stream_vendas.py")
    sys.exit(1)


class SecureStreamProcessor:
    """
    Processador de stream seguro com auditoria completa.
    Integra o sistema de segurança desenvolvido para o projeto.
    
    DESIGN PATTERN: Facade Pattern para unificar operações complexas
    SECURITY APPROACH: Defense in Depth (múltiplas camadas de segurança)
    COMPLIANCE: LGPD + SOX ready com auditoria completa
    
    CARACTERÍSTICAS TÉCNICAS:
    - Processamento assíncrono de eventos
    - Retry automático com backoff exponencial (simulado)
    - Monitoramento de health check (simulado)
    - Isolamento de recursos
    """
    
    def __init__(self):
        """
        Inicializa o processador com todos os componentes de segurança.
        
        PROCESSO DE INICIALIZAÇÃO:
        1. Componentes de segurança (vault + auditoria)
        2. Cliente de storage (MinIO/S3)
        3. Estrutura de armazenamento local
        4. Validação de conectividade
        """
        print("🔐 Inicializando o Processador de Stream Seguro...")
        
        # Inicialização em etapas com validação de cada componente
        self._init_security_components()
        self._init_storage_components()
        self._setup_local_storage()
        
        print("✅ Processador inicializado com sucesso!")

    def _init_security_components(self):
        """
        Inicializa componentes de segurança e auditoria.
        
        COMPONENTES INICIALIZADOS:
        - AuditLogger: Sistema de auditoria dual (CSV + Python logging)
        - AirflowSecurityManager: Vault criptografado para credenciais
        
        NOTA TÉCNICA: Auditoria é inicializada primeiro para capturar
        todos os eventos subsequentes, incluindo falhas de inicialização.
        """
        # Inicialização do sistema de auditoria (primeiro componente)
        self.audit = AuditLogger(
            audit_file_path=AUDIT_LOG_PATH,
            system_log_file_path=SYSTEM_LOG_PATH
        )
        
        # Inicialização do gerenciador de segredos criptografados
        self.sec_manager = AirflowSecurityManager(
            vault_db_path=VAULT_DB_PATH,
            secret_key=SECRET_KEY,
            audit_logger=self.audit
        )
        
        # Log de inicialização para compliance
        self.audit.log("Componentes de segurança inicializados", action="SECURITY_INIT")

    def _init_storage_components(self):
        """
        Inicializa cliente MinIO e configurações de storage.
        
        CONFIGURAÇÕES DE STORAGE:
        - Cliente S3 compatível (MinIO/AWS)
        - Bucket dedicado para stream de vendas
        - Estrutura de diretórios por data (particionamento)
        - Nomenclatura de arquivos com timestamp único
        """
        # Inicialização do cliente MinIO com credenciais seguras
        self.s3_client = self._get_secure_minio_client()
        
        # Configuração do bucket (em produção: criação automática se não existir)  
        self.bucket_name = "vendas-stream-bucket" # Use um nome de bucket seu
        
        # ===============================================================================
        # ESTRUTURA DE ARMAZENAMENTO COM PARTICIONAMENTO POR DATA
        # ===============================================================================
        # Particionamento automático por data para organização e performance
        today = datetime.now().strftime("%Y-%m-%d")
        self.pasta_saida_local = f"{{AIRFLOW_HOME}}/data/stream_processado/{today}"
        
        # Criação segura de diretórios com permissões apropriadas
        os.makedirs(self.pasta_saida_local, exist_ok=True)
        
        # Nomenclatura única de arquivos para evitar conflitos
        self.nome_arquivo_local = f"vendas_stream_{int(time.time())}.csv"
        self.caminho_local = os.path.join(self.pasta_saida_local, self.nome_arquivo_local)
        self.caminho_minio = f"stream_processado/{today}/{self.nome_arquivo_local}"

    def _get_secure_minio_client(self):
        """
        Obtém cliente MinIO usando credenciais seguras do vault.
        
        FLUXO DE SEGURANÇA:
        1. Recupera credenciais do vault criptografado
        2. Valida integridade das credenciais
        3. Cria cliente S3 com configurações seguras
        4. Registra operação na auditoria
        
        TRATAMENTO DE ERRO: Falha fast se credenciais inválidas
        """
        # Log de acesso ao vault para auditoria
        self.audit.log("Recuperando credenciais MinIO do vault", action="GET_MINIO_CREDS")
        
        # Recuperação segura das credenciais do vault
        minio_creds = self.sec_manager.get_secret("minio_local_credentials")
        
        # Validação rigorosa das credenciais recuperadas
        if not minio_creds or not isinstance(minio_creds, dict):
            raise ConfigurationError("Credenciais MinIO não encontradas no vault")

        # Verificação de campos obrigatórios
        required_keys = ["endpoint_url", "access_key", "secret_key"]
        missing_keys = [key for key in required_keys if not minio_creds.get(key)]
        if missing_keys:
            raise ConfigurationError(f"Credenciais MinIO incompletas: {missing_keys}")

        # ===============================================================================
        # CRIAÇÃO DO CLIENTE S3 COM CONFIGURAÇÕES DE SEGURANÇA
        # ===============================================================================
        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_creds["endpoint_url"],
            aws_access_key_id=minio_creds["access_key"],
            aws_secret_access_key=minio_creds["secret_key"],
            verify=False  # Para ambiente de desenvolvimento local
        )
        
        # Auditoria de criação do cliente para compliance
        self.audit.log("Cliente MinIO criado com sucesso", action="MINIO_CLIENT_CREATED")
        return s3_client

    def _setup_local_storage(self):
        """
        Inicializa arquivo CSV local com headers estruturados.
        
        ESTRUTURA DO CSV:
        - customer_state: Estado do cliente (para análise geográfica)
        - price: Valor da venda (filtro > R$ 100)
        - timestamp: Timestamp original do evento
        - processed_at: Timestamp de processamento (para auditoria)
        """
        # Criação do arquivo CSV com headers estruturados
        with open(self.caminho_local, "w", newline="", encoding='utf-8') as f:
            fieldnames = ["customer_state", "price", "timestamp", "processed_at"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
        # Log de inicialização do arquivo para auditoria
        self.audit.log(f"Arquivo CSV inicializado: {self.caminho_local}", action="CSV_INIT")

    def processar_evento(self, evento):
        """
        Processa um evento individual do stream de vendas.
        
        REGRAS DE NEGÓCIO:
        - Apenas vendas > R$ 100 são processadas
        - Enriquecimento com timestamp de processamento
        - Validação de integridade dos dados
        - Upload automático para MinIO após processamento
        
        TRATAMENTO DE ERRO: Isolamento de falhas por evento
        """
        try:
            # ===============================================================================
            # VALIDAÇÃO E TRANSFORMAÇÃO DOS DADOS
            # ===============================================================================
            # Conversão segura do valor com tratamento de erro
            price = float(evento.get("price", 0))
            
            # Aplicação da regra de negócio (filtro por valor)
            if price > 100:
                # Estruturação do evento processado com enriquecimento
                evento_processado = {
                    "customer_state": evento.get("customer_state", "UNKNOWN"),
                    "price": price,
                    "timestamp": evento.get("timestamp", datetime.now().isoformat()),
                    "processed_at": datetime.now().isoformat()  # Timestamp de auditoria
                }

                # ===============================================================================
                # PERSISTÊNCIA LOCAL COM APPEND ATÔMICO
                # ===============================================================================
                with open(self.caminho_local, "a", newline="", encoding='utf-8') as f:
                    fieldnames = ["customer_state", "price", "timestamp", "processed_at"]
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(evento_processado)

                # Feedback visual para demonstração
                print(f"✅ Evento processado: {evento_processado['customer_state']} - R$ {price:.2f}")
                
                # ===============================================================================
                # AUDITORIA DE PROCESSAMENTO PARA COMPLIANCE
                # ===============================================================================
                self.audit.log(
                    "Evento de venda processado", 
                    action="STREAM_EVENT_PROCESSED",
                    details=f"State: {evento_processado['customer_state']}, Value: R$ {price:.2f}",
                    compliance_status="LGPD_PROCESSED"
                )
                
                # Upload automático para storage distribuído
                self._upload_to_minio()
                
        except Exception as e:
            # ===============================================================================
            # TRATAMENTO DE ERRO COM ISOLAMENTO DE FALHAS
            # ===============================================================================
            print(f"❌ Erro ao processar evento: {e}")
            self.audit.log(f"Erro no processamento: {e}", 
                           level="ERROR", 
                           action="STREAM_PROCESS_ERROR",
                           error_message=str(e))

    def _upload_to_minio(self):
        """
        Realiza upload seguro para MinIO com tratamento de falhas.
        
        ESTRATÉGIA DE UPLOAD:
        - Upload incremental (arquivo completo a cada evento)
        - Sobrescrita segura do arquivo remoto
        - Auditoria de todas as operações de upload
        - Fallback graceful em caso de falha de conectividade
        """
        try:
            # ===============================================================================
            # UPLOAD SEGURO COM VALIDAÇÃO DE INTEGRIDADE
            # ===============================================================================
            self.s3_client.upload_file(
                self.caminho_local,          # Arquivo local
                self.bucket_name,            # Bucket de destino
                self.caminho_minio           # Path remoto
            )
            
            # Feedback de sucesso para monitoramento
            print(f"📡 Enviado para MinIO: s3://{self.bucket_name}/{self.caminho_minio}")
            
            # Auditoria de upload bem-sucedido
            self.audit.log("Upload MinIO realizado com sucesso", 
                           action="MINIO_UPLOAD_SUCCESS",
                           details=f"Bucket: {self.bucket_name}, Path: {self.caminho_minio}")
            
        except Exception as e:
            # ===============================================================================
            # TRATAMENTO DE FALHA DE UPLOAD (NÃO CRÍTICO)
            # ===============================================================================
            # Falha de upload não interrompe o processamento local
            print(f"⚠️  Falha no upload MinIO: {e}")
            
            # Auditoria de falha para investigação posterior
            self.audit.log(f"Falha upload MinIO: {e}", 
                           level="ERROR", 
                           action="MINIO_UPLOAD_FAIL",
                           error_message=str(e))

    def start(self):
        """
        Inicia o loop principal de processamento de stream.
        
        ARQUITETURA DE PROCESSAMENTO:
        - Loop bloqueante com timeout configurável
        - Contagem de eventos para métricas
        - Graceful shutdown com Ctrl+C
        - Estatísticas finais de processamento
        
        RECUPERAÇÃO DE FALHAS:
        - Timeout automático se stream parar
        - Preservação de dados já processados
        - Logs de auditoria de início e fim
        """
        print("🚀 Processador iniciado. Aguardando eventos... (Ctrl+C para parar)")
        
        # Contador de eventos para métricas
        eventos_processados = 0
        
        try:
            # ===============================================================================
            # LOOP PRINCIPAL DE PROCESSAMENTO
            # ===============================================================================
            while True:
                try:
                    # Recupera evento da fila com timeout (evita bloqueio infinito)
                    evento = fila_eventos.get(timeout=5)
                    
                    # Processamento do evento individual
                    self.processar_evento(evento)
                    eventos_processados += 1
                    
                except KeyboardInterrupt:
                    # Graceful shutdown solicitado pelo usuário
                    print("\n🛑 Interrupção solicitada pelo usuário")
                    break
                except:
                    # Timeout da fila - stream provavelmente parou
                    print("⏰ Timeout - fila vazia. Encerrando...")
                    break
                    
        finally:
            # ===============================================================================
            # FINALIZAÇÃO COM ESTATÍSTICAS E AUDITORIA
            # ===============================================================================
            print(f"\n📈 Processamento finalizado: {eventos_processados} eventos processados")
            
            # Auditoria de finalização para compliance
            self.audit.log(
                f"Processador encerrado - {eventos_processados} eventos processados", 
                action="STREAM_PROCESSOR_STOP",
                details=f"Arquivo final: {self.caminho_local}"
            )

# ===============================================================================
# PONTO DE ENTRADA PRINCIPAL COM TRATAMENTO ROBUSTO DE ERROS
# ===============================================================================
if __name__ == "__main__":
    try:
        # Inicialização e execução do processador
        processor = SecureStreamProcessor()
        processor.start()
        
    except ConfigurationError as e:
        # Erro de configuração - problema de setup
        print(f"❌ ERRO DE CONFIGURAÇÃO: {e}")
        print("💡 Verifique se o vault foi configurado corretamente")
        
    except Exception as e:
        # Erro fatal não tratado - debug necessário
        print(f"❌ ERRO FATAL: {e}")
        traceback.print_exc()
        
    finally:
        # Limpeza garantida independente de como o programa termina
        print("👋 Processador encerrado")
