import logging
import os
from datetime import datetime, timedelta

class SecurityMonitor:
    def __init__(self, monitor_log_path=None):
        self.monitor_log_path = monitor_log_path or os.getenv('SECURITY_MONITOR_LOG_PATH')
        if not self.monitor_log_path:
            self.monitor_log_path = '/tmp/airflow_monitor_logs' # Default explícito se getenv falhar
            print(f"SecurityMonitor: SECURITY_MONITOR_LOG_PATH não definido, usando default: {self.monitor_log_path}")

        log_dir = os.path.dirname(self.monitor_log_path)

        if log_dir and not os.path.exists(log_dir): 
            try:
                os.makedirs(log_dir, exist_ok=True)
            except OSError as e:
                print(f"SecurityMonitor: Erro crítico ao criar diretório de log {log_dir}: {e}")
        

        if os.path.isdir(self.monitor_log_path): # Se o path fornecido for um diretório
            self.log_file = os.path.join(self.monitor_log_path, f"security_activity_{datetime.now().strftime('%Y-%m-%d')}.log")
        else: # Se for um caminho de arquivo completo
            self.log_file = self.monitor_log_path
            # Garante que o diretório para este arquivo exista, caso monitor_log_path seja um arquivo em um subdiretório
            log_file_dir = os.path.dirname(self.log_file)
            if log_file_dir and not os.path.exists(log_file_dir):
                try:
                    os.makedirs(log_file_dir, exist_ok=True)
                except OSError as e:
                    print(f"SecurityMonitor: Erro crítico ao criar diretório para log_file {log_file_dir}: {e}")

        self._initialize_logger()

    def _initialize_logger(self):
        self.logger = logging.getLogger('security_system.SecurityMonitor') 
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            try:
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                file_handler = logging.FileHandler(self.log_file, encoding='utf-8', mode='a')
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)
            except Exception as e:
                 print(f"SecurityMonitor: Falha ao inicializar file_handler para {self.log_file}: {e}")


    def log_event(self, event_type, message, details=None):
        log_message = f"EVENT: {event_type} - {message}"
        if details:
            log_message += f" - Details: {json.dumps(details)}" # Melhor serializar dicts
        if hasattr(self, 'logger') and self.logger.handlers: 
            self.logger.info(log_message)
        else: 
            print(f"LOG_EVENT_FALLBACK (SecurityMonitor): {log_message}")


    def check_unusual_activity(self, threshold_minutes=5, activity_type="login_failure"):
        now = datetime.now()
        recent_logs = []
        try:
            if not os.path.exists(self.log_file): 
                if hasattr(self, 'logger') and self.logger.handlers:
                    self.logger.warning(f"Arquivo de log {self.log_file} não encontrado para check_unusual_activity.")
                return False

            with open(self.log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if activity_type in line: # Checagem simples
                        try:
                            # Tentar parsear timestamp de forma mais robusta se o formato do log for conhecido
                            log_time_str = line.split(' - ')[0] # Exemplo, se o log começa com 'YYYY-MM-DD HH:MM:SS,ms - LEVEL - MSG'
                            log_time = datetime.strptime(log_time_str.split(',')[0], '%Y-%m-%d %H:%M:%S')
                            if now - log_time < timedelta(minutes=threshold_minutes):
                                recent_logs.append(line)
                        except ValueError: # Ignorar linhas que não têm timestamp no formato esperado
                            continue 
            if len(recent_logs) >= 3:
                details_unusual={"count": len(recent_logs), "threshold": 3, "time_frame": f"{threshold_minutes} minutes"}
                log_message = f"EVENT: UNUSUAL_ACTIVITY_DETECTED - Múltiplas ocorrências de '{activity_type}' detectadas. - Details: {json.dumps(details_unusual)}"
                if hasattr(self, 'logger') and self.logger.handlers:
                    self.logger.warning(log_message)
                else:
                    print(f"WARNING_FALLBACK (SecurityMonitor): {log_message}")
                return True
        except FileNotFoundError: 
            pass
        except Exception as e:
            log_target = self.logger if hasattr(self, 'logger') and self.logger.handlers else logging
            log_target.error(f"Erro ao verificar atividade incomum: {e}", exc_info=True)
        return False

    def get_cpu_usage(self):
        self.log_event("SYSTEM_MONITOR_CPU", "CPU usage requested (placeholder).")
        return 10.5

    def get_memory_usage(self):
        self.log_event("SYSTEM_MONITOR_MEMORY", "Memory usage requested (placeholder).")
        return 256.0

    def check_memory_threshold(self, threshold_mb: float, alert_percentage: float = 80.0):
        used_mb_simulated = self.get_memory_usage()
        details_mem={"used_mb": used_mb_simulated, "threshold_mb": threshold_mb}
        log_target = self.logger if hasattr(self, 'logger') and self.logger.handlers else logging
        
        if used_mb_simulated > threshold_mb:
            log_target.warning(f"EVENT: MEMORY_ALERT - Uso de memória simulado {used_mb_simulated:.2f}MB excede o limiar de {threshold_mb:.2f}MB. - Details: {json.dumps(details_mem)}")
            return True
        log_target.info(f"EVENT: MEMORY_CHECK - Uso de memória simulado {used_mb_simulated:.2f}MB está dentro do limiar de {threshold_mb:.2f}MB. - Details: {json.dumps(details_mem)}")
        return False
