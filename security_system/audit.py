import logging
import csv
import os
from datetime import datetime, timedelta
import pandas as pd
import json

class AuditLogger:
    def __init__(self, audit_file_path: str, system_log_file_path: str):
        self.audit_file_path = audit_file_path
        self.system_log_file_path = system_log_file_path

        if not self.audit_file_path or not self.system_log_file_path:
            error_msg = f"CRÍTICO (AuditLogger): Caminhos de log não foram fornecidos."
            print(error_msg) 
            raise ValueError(error_msg)

        self._ensure_log_paths_exist()
        self.logger = logging.getLogger('security_system.AuditLogger_vFINAL')
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            self._setup_file_handlers()
        
        self._init_audit_csv_file()

    def _ensure_log_paths_exist(self):
        for file_path in [self.audit_file_path, self.system_log_file_path]:
            if file_path:
                directory = os.path.dirname(file_path)
                if directory and not os.path.exists(directory):
                    os.makedirs(directory, exist_ok=True)

    def _setup_file_handlers(self):
        try:
            handler = logging.FileHandler(self.system_log_file_path, mode='a', encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s|%(levelname)s|%(name)s|%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        except Exception as e:
            print(f"AuditLogger: Falha ao configurar handler: {e}")

    def _init_audit_csv_file(self):
        write_header = not os.path.exists(self.audit_file_path) or os.path.getsize(self.audit_file_path) == 0
        if write_header:
            with open(self.audit_file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp', 'level', 'dag_id', 'task_id', 'user', 'action', 'details', 'compliance_status', 'risk_level', 'service', 'error_message', 'stack_trace_needed'])

    def log(self, message: str, level: str = "INFO", **kwargs):
        log_data = {'timestamp': datetime.now().isoformat(), 'level': level.upper(), 'dag_id': kwargs.get('dag_id', 'system'), 'task_id': kwargs.get('task_id', 'system'), 'user': kwargs.get('user', 'airflow_process'), 'action': kwargs.get('action', 'GENERIC_EVENT'), 'details': message, 'compliance_status': kwargs.get('status', 'LGPD_NA'), 'risk_level': kwargs.get('risk_level', level.upper()), 'service': kwargs.get('service', 'N/A'), 'error_message': kwargs.get('error_message', ''), 'stack_trace_needed': kwargs.get('stack_trace_needed', False)}
        try:
            with open(self.audit_file_path, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=log_data.keys())
                if f.tell() == 0: writer.writeheader()
                writer.writerow(log_data)
            getattr(self.logger, level.lower(), self.logger.info)(f"ACTION: {log_data['action']} | DETAILS: {message}")
        except Exception as e:
            print(f"FALHA CRÍTICA NO LOGGER DE AUDITORIA: {e}")
    
    def generate_report(self, start_date: str, end_date: str) -> dict:
        try:
            start_dt = datetime.fromisoformat(start_date)
            end_dt = datetime.fromisoformat(end_date)
            if not os.path.exists(self.audit_file_path):
                 return {"error": "Arquivo de auditoria não encontrado.", "periodo": f"{start_date} a {end_date}"}
            df = pd.read_csv(self.audit_file_path, parse_dates=['timestamp'], infer_datetime_format=True)
            if df.empty:
                return {"message": "Arquivo de auditoria vazio", "periodo": f"{start_date} a {end_date}"}
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
            mask = (df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)
            period_df = df.loc[mask].copy()
            if period_df.empty:
                return {"message": "Nenhum evento encontrado para o período", "periodo": f"{start_date} a {end_date}"}
            total_events = len(period_df)
            compliance_ok_count = period_df[period_df['compliance_status'] == 'LGPD_OK'].shape[0]
            report = {'periodo': f"{start_date} a {end_date}", 'total_eventos': total_events, 'distribuicao_acoes': period_df['action'].value_counts().to_dict(), 'taxa_conformidade_lgpd': round((compliance_ok_count / total_events * 100) if total_events > 0 else 0, 2), 'eventos_risco_alto': period_df[period_df['risk_level'] == 'HIGH'].shape[0], 'principais_violacoes': period_df[period_df['compliance_status'] != 'LGPD_OK']['details'].value_counts().head(5).to_dict(), 'usuarios_ativos': period_df['user'].nunique(), 'detalhes_eventos_criticos': period_df[period_df['level'].isin(['ERROR', 'CRITICAL'])].to_dict(orient='records')}
            self._generate_authority_report(period_df)
            self.log(f"Relatório de auditoria gerado para o período {start_date} a {end_date}", action="AUDIT_REPORT_GEN", details=json.dumps({'total_eventos': report['total_eventos'],'taxa_conformidade': report['taxa_conformidade_lgpd']}))
            return report
        except Exception as e:
            self.logger.error(f"Falha ao gerar relatório de auditoria: {str(e)}", exc_info=True)
            return {"error": str(e)}

    def _generate_authority_report(self, df: pd.DataFrame):
        columns_to_drop = ['user', 'risk_level', 'error_message', 'stack_trace_needed', 'service']
        authority_df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore').copy()
        report_dir = os.path.dirname(self.audit_file_path)
        if not report_dir: report_dir = "." 
        report_path = os.path.join(report_dir, f"lgpd_authority_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        try:
            authority_df.to_csv(report_path, index=False, encoding='utf-8')
            os.chmod(report_path, 0o440)
            self.log(message=f"Relatório para autoridades gerado: {report_path}", action='AUTHORITY_REPORT_GEN', sensitive_action=True)
        except Exception as e:
            self.logger.error(f"Falha ao gerar relatório para autoridades: {e}", exc_info=True)

    def get_audit_data(self, days: int = 7) -> pd.DataFrame:
        try:
            if not os.path.exists(self.audit_file_path):
                 return pd.DataFrame()
            df = pd.read_csv(self.audit_file_path, parse_dates=['timestamp'], infer_datetime_format=True)
            if df.empty: return pd.DataFrame()
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None) 
            cutoff = datetime.now() - timedelta(days=days)
            return df[df['timestamp'] >= cutoff].copy()
        except Exception as e:
            self.logger.error(f"Falha ao recuperar dados de auditoria: {str(e)}", exc_info=True)
            return pd.DataFrame()

    def log_operation(self, dag_id: str, task_id: str, operation: str, metadata: dict = None):
        details_msg = f"Operation: {operation}"
        if metadata:
            details_msg += f" | Metadata: {json.dumps(metadata)}"
        self.log(details_msg, action=f"OP_{operation.upper()}", dag_id=dag_id, task_id=task_id)

    def log_incident(self, severity: str, dag_id: str, task_id: str, error: str, stack_trace: bool = False):
        self.log(f"INCIDENT DETECTED: {error}", level=severity.upper(), action="SECURITY_INCIDENT", dag_id=dag_id, task_id=task_id, risk_level=severity.upper(), compliance_status="LGPD_BREACH" if severity.upper() in ["CRITICAL", "URGENT", "HIGH"] else "LGPD_WARNING", error_message=error, stack_trace_needed=stack_trace)

    def log_upload(self, local_path: str, minio_path: str):
        self.log(f"File uploaded from {local_path} to {minio_path}", action="FILE_UPLOAD", details=f"Source: {local_path}, Destination: {minio_path}")

    def log_transfer(self, object_key: str, source_bucket: str = 'N/A', dest_bucket: str = 'N/A'):
        self.log(f"Object '{object_key}' transferred from '{source_bucket}' to '{dest_bucket}'", action="OBJECT_TRANSFER", details=f"Key: {object_key}, Source: {source_bucket}, Dest: {dest_bucket}")

    def log_validation(self, results: dict = None, success: bool = None, stats: dict = None, failed_expectations: list = None, metadata: dict = None):
        is_success = success
        if is_success is None and results:
            if isinstance(results, dict): 
                is_success = results.get('success', False) 
            elif hasattr(results, 'success'): 
                 is_success = results.success
        elif is_success is None: 
            is_success = False 

        validation_status_action = "VALIDATION_SUCCESS" if is_success else "VALIDATION_FAILURE"
        log_level = "INFO" if is_success else "ERROR"
        compliance = "LGPD_OK" if is_success else "LGPD_VIOLATION"
        
        details_dict_for_csv = {}
        if results:
            if isinstance(results, dict): 
                details_dict_for_csv['ge_success'] = results.get('success')
                details_dict_for_csv['ge_stats'] = results.get('statistics')
                if results.get('results'):
                    details_dict_for_csv['ge_failed_expectations'] = [r.get('expectation_config', {}).get('expectation_type', 'N/A') for r in results.get('results', []) if not r.get('success')]
            elif hasattr(results, 'success'): 
                details_dict_for_csv['ge_success'] = results.success
                if hasattr(results, 'statistics'): details_dict_for_csv['ge_stats'] = results.statistics
                if hasattr(results, 'results'): details_dict_for_csv['ge_failed_expectations'] = [r.expectation_config.expectation_type for r in results.results if not r.success]
        else: 
            if success is not None: details_dict_for_csv['manual_success'] = success
            if stats: details_dict_for_csv['manual_stats'] = stats
            if failed_expectations: details_dict_for_csv['manual_failed_expectations'] = failed_expectations
        if metadata: details_dict_for_csv['custom_metadata'] = metadata
        
        details_message = json.dumps(details_dict_for_csv)

        self.log(
            message=f"Validation Status: {validation_status_action}",
            action=validation_status_action,
            level=log_level,
            compliance_status=compliance, 
            details=details_message
        )
