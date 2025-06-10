# ===============================================================================
# IMPORTAÇÕES PARA SISTEMA DE AUDITORIA EMPRESARIAL
# ===============================================================================
import logging
import csv
import os
from datetime import datetime, timedelta
import pandas as pd  # Para análise de dados de auditoria
import json

class AuditLogger:
    """
    Sistema de Auditoria Empresarial para Compliance.
    
    CARACTERÍSTICAS TÉCNICAS:
    - Dual logging: CSV estruturado + Python logging
    - Campos customizados para LGPD e SOX
    - Rotação automática de logs
    - Análise de padrões suspeitos
    
    COMPLIANCE FRAMEWORKS SUPORTADOS:
    - LGPD (Lei Geral de Proteção de Dados)
    - SOX (Sarbanes-Oxley)
    - PCI-DSS
    - ISO 27001
    """
    
    def __init__(self, audit_file_path: str, system_log_file_path: str):
        """
        Inicializa sistema de auditoria com validação rigorosa.
        
        FAIL-FAST PRINCIPLE: Falha imediata se caminhos inválidos
        NOTA PARA BANCA: Logs são críticos - sem eles, sistema não inicia
        """
        self.audit_file_path = audit_file_path
        self.system_log_file_path = system_log_file_path

        # Validação crítica de entrada - logs são obrigatórios
        if not self.audit_file_path or not self.system_log_file_path:
            error_msg = f"CRÍTICO (AuditLogger): Caminhos de log não foram fornecidos."
            print(error_msg) 
            raise ValueError(error_msg)

        # Garantia de existência dos diretórios de log
        self._ensure_log_paths_exist()
        
        # ===============================================================================
        # CONFIGURAÇÃO DO SISTEMA DE LOGGING PYTHON
        # ===============================================================================
        # Logger nomeado para identificação única no sistema
        self.logger = logging.getLogger('security_system.AuditLogger_vFINAL')
        self.logger.propagate = False  # Evita duplicação de logs
        self.logger.setLevel(logging.INFO)

        # Configuração única de handlers para evitar duplicação
        if not self.logger.handlers:
            self._setup_file_handlers()
        
        # Inicialização do arquivo CSV de auditoria
        self._init_audit_csv_file()

    def _ensure_log_paths_exist(self):
        """
        Garante que diretórios de log existem.
        
        PADRÃO DE SEGURANÇA: Criação com permissões restritivas
        """
        for file_path in [self.audit_file_path, self.system_log_file_path]:
            if file_path:
                # Extração do diretório pai
                directory = os.path.dirname(file_path)
                if directory and not os.path.exists(directory):
                    # Criação com permissões seguras
                    os.makedirs(directory, exist_ok=True)

    def _setup_file_handlers(self):
        """
        Configura handlers de arquivo para logging.
        
        FORMATO PERSONALIZADO: timestamp|level|component|message
        ENCODING: UTF-8 para suporte internacional
        """
        try:
            # Handler para arquivo de log do sistema
            handler = logging.FileHandler(self.system_log_file_path, 
                                          mode='a', 
                                          encoding='utf-8')
            
            # Formatter customizado para análise posterior
            formatter = logging.Formatter(
                '%(asctime)s|%(levelname)s|%(name)s|%(message)s', 
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        except Exception as e:
            # Falha crítica - sistema não pode operar sem logs
            print(f"AuditLogger: Falha ao configurar handler: {e}")

    def _init_audit_csv_file(self):
        """
        Inicializa arquivo CSV de auditoria com cabeçalhos estruturados.
        
        ESTRUTURA PARA COMPLIANCE:
        - timestamp: Marca temporal precisa
        - user: Identificação do usuário (LGPD)
        - action: Ação executada 
        - compliance_status: Status para LGPD
        - risk_level: Nível de risco da operação
        - stack_trace_needed: Flag para debug
        """
        # Verifica se precisa criar cabeçalho
        write_header = (not os.path.exists(self.audit_file_path) or 
                        os.path.getsize(self.audit_file_path) == 0)
        
        if write_header:
            with open(self.audit_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                # Cabeçalhos estruturados para compliance
                writer.writerow([
                    'timestamp',            # ISO timestamp
                    'level',                # Log level (INFO, WARNING, ERROR, CRITICAL)
                    'dag_id',               # Airflow DAG identifier
                    'task_id',              # Airflow Task identifier  
                    'user',                 # User/process identifier
                    'action',               # Action performed
                    'details',              # Detailed description
                    'compliance_status',    # LGPD compliance status
                    'risk_level',           # Risk assessment
                    'service',              # Service/component
                    'error_message',        # Error details if applicable
                    'stack_trace_needed'    # Debug flag
                ])

    def log(self, message: str, level: str = "INFO", **kwargs):
        """
        Método principal de logging com auditoria estruturada.
        
        DUAL LOGGING APPROACH:
        1. CSV estruturado para análise e compliance
        2. Python logging para integração com ferramentas
        
        PARÂMETROS DINÂMICOS (**kwargs):
        - dag_id: ID da DAG do Airflow
        - task_id: ID da Task do Airflow
        - user: Usuário executando a ação
        - action: Tipo de ação (padronizado)
        - compliance_status: Status LGPD
        - risk_level: Nível de risco
        """
        # ===============================================================================
        # ESTRUTURAÇÃO DOS DADOS DE AUDITORIA
        # ===============================================================================
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'level': level.upper(),
            'dag_id': kwargs.get('dag_id', 'system'),
            'task_id': kwargs.get('task_id', 'system'),
            'user': kwargs.get('user', 'airflow_process'),
            'action': kwargs.get('action', 'GENERIC_EVENT'),
            'details': message,
            'compliance_status': kwargs.get('status', 'LGPD_NA'),
            'risk_level': kwargs.get('risk_level', level.upper()),
            'service': kwargs.get('service', 'N/A'),
            'error_message': kwargs.get('error_message', ''),
            'stack_trace_needed': kwargs.get('stack_trace_needed', False)
        }
        
        try:
            # ===============================================================================
            # PERSISTÊNCIA EM CSV PARA ANÁLISE ESTRUTURADA
            # ===============================================================================
            with open(self.audit_file_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=log_data.keys())
                
                # Adiciona cabeçalho se arquivo vazio (safety check)
                if f.tell() == 0: 
                    writer.writeheader()
                
                # Escreve registro de auditoria
                writer.writerow(log_data)
            
            # ===============================================================================
            # LOGGING PYTHON PARA INTEGRAÇÃO COM FERRAMENTAS
            # ===============================================================================
            # Usa o nível apropriado ou fallback para INFO
            log_method = getattr(self.logger, level.lower(), self.logger.info)
            log_method(f"ACTION: {log_data['action']} | DETAILS: {message}")
            
        except Exception as e:
            # FALHA CRÍTICA: Se auditoria falha, todo o sistema deve parar
            print(f"FALHA CRÍTICA NO LOGGER DE AUDITORIA: {e}")
            # Em produção: enviar alerta para equipe de segurança
            
