import os
import sys
import shutil
import time
import logging
from pathlib import Path
from typing import List, Tuple, Optional

# --- Script de Configuração e Validação Automática ---
# Este script adapta um projeto desenvolvido localmente para execução
# em qualquer máquina, substituindo placeholders de caminho e validando
# a integridade da estrutura resultante com backup e rollback automático.

# Configurações
PLACEHOLDER_PATH = "{{AIRFLOW_HOME}}"
BACKUP_DIR = ""  # Variável global para guardar o nome da pasta de backup
LOG_FILE = "setup_configuration.log"

# Configuração de logging
def setup_logging():
    """Configura sistema de logging para auditoria completa do processo."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

class ConfigurationError(Exception):
    """Exceção customizada para erros de configuração."""
    pass

class BackupError(Exception):
    """Exceção customizada para erros de backup."""
    pass

def create_backup(target_path: str) -> bool:
    """
    Cria um backup completo do diretório do projeto antes de qualquer modificação.
    
    Args:
        target_path: Caminho do diretório a ser feito backup
        
    Returns:
        bool: True se backup foi criado com sucesso, False caso contrário
        
    Raises:
        BackupError: Se falhar ao criar o backup
    """
    global BACKUP_DIR
    BACKUP_DIR = f"project_backup_{int(time.time())}"
    
    print(f"\n[PASSO 1 de 5] 🗄️  Criando backup de segurança em: '{BACKUP_DIR}'...")
    logger.info(f"Iniciando backup do projeto em: {BACKUP_DIR}")
    
    try:
        # Padrões de arquivos/pastas a serem ignorados no backup
        ignore_patterns = shutil.ignore_patterns(
            'venv', '__pycache__', 'project_backup_*', '.git', 
            '*.pyc', '*.pyo', '.DS_Store', 'Thumbs.db'
        )
        
        shutil.copytree(target_path, BACKUP_DIR, ignore=ignore_patterns)
        
        # Verifica se o backup foi criado corretamente
        if not os.path.exists(BACKUP_DIR) or len(os.listdir(BACKUP_DIR)) == 0:
            raise BackupError("Backup foi criado mas está vazio ou inacessível")
            
        print("   -> ✅ Backup criado com sucesso.")
        logger.info(f"Backup criado com sucesso: {len(os.listdir(BACKUP_DIR))} itens copiados")
        return True
        
    except Exception as e:
        error_msg = f"Erro crítico ao criar backup: {str(e)}"
        print(f"❌ {error_msg}")
        logger.error(error_msg)
        raise BackupError(error_msg) from e

def get_files_to_process(base_path: str) -> List[str]:
    """
    Coleta todos os arquivos que precisam ter placeholders substituídos.
    
    Args:
        base_path: Diretório base para busca
        
    Returns:
        List[str]: Lista de caminhos de arquivos para processar
    """
    files_to_process = []
    ignored_dirs = {'venv', 'project_backup_', '.git', '__pycache__', '.pytest_cache'}
    valid_extensions = {'.py', '.cfg', '.json', '.md', '.yml', '.yaml', '.txt'}
    
    for root, dirs, files in os.walk(base_path):
        # Remove diretórios ignorados da lista de busca
        dirs[:] = [d for d in dirs if not any(ignored in d for ignored in ignored_dirs)]
        
        for file in files:
            file_path = Path(root) / file
            if file_path.suffix.lower() in valid_extensions:
                files_to_process.append(str(file_path))
                
    logger.info(f"Encontrados {len(files_to_process)} arquivos para processamento")
    return files_to_process

def configure_paths(target_path: str) -> int:
    """
    Encontra e substitui os placeholders de caminho nos arquivos do projeto.
    
    Args:
        target_path: Novo caminho base para substituição
        
    Returns:
        int: Número de arquivos modificados, -1 em caso de erro
        
    Raises:
        ConfigurationError: Se houver erro na configuração dos caminhos
    """
    print("\n[PASSO 2 de 5] 🔄  Substituindo placeholders de caminho...")
    logger.info("Iniciando substituição de placeholders")
    
    # Normaliza o caminho para usar barras '/' (compatibilidade cross-platform)
    target_path_normalized = str(Path(target_path)).replace('\\', '/')
    
    print(f"   -> Placeholder a ser substituído: '{PLACEHOLDER_PATH}'")
    print(f"   -> Novo caminho de destino:       '{target_path_normalized}'")
    
    try:
        files_to_process = get_files_to_process('.')
        files_changed = 0
        
        for file_path in files_to_process:
            try:
                # Lê o arquivo com encoding explícito
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                # Só modifica se o placeholder estiver presente
                if PLACEHOLDER_PATH in content:
                    new_content = content.replace(PLACEHOLDER_PATH, target_path_normalized)
                    
                    # Escreve o arquivo modificado
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    
                    files_changed += 1
                    logger.info(f"Placeholder substituído em: {file_path}")
                    
            except (UnicodeDecodeError, PermissionError) as e:
                logger.warning(f"Não foi possível processar {file_path}: {str(e)}")
                continue
            except Exception as e:
                error_msg = f"Erro inesperado ao processar {file_path}: {str(e)}"
                logger.error(error_msg)
                raise ConfigurationError(error_msg) from e

        print(f"   -> ✅ {files_changed} arquivo(s) tiveram seus caminhos atualizados.")
        logger.info(f"Configuração concluída: {files_changed} arquivos modificados")
        return files_changed
        
    except Exception as e:
        error_msg = f"Falha na configuração de caminhos: {str(e)}"
        logger.error(error_msg)
        raise ConfigurationError(error_msg) from e

def validate_setup(target_path: str) -> Tuple[bool, List[str]]:
    """
    Verifica se a estrutura do projeto e arquivos críticos estão corretos.
    
    Args:
        target_path: Caminho base do projeto
        
    Returns:
        Tuple[bool, List[str]]: (validação_passou, lista_de_problemas)
    """
    print("\n[PASSO 3 de 5] ✅  Validando a nova configuração...")
    logger.info("Iniciando validação da configuração")
    
    # Arquivos e pastas críticos que devem existir
    critical_paths = [
        "airflow.cfg",
        "requirements.txt", 
        "dags",
        "plugins/security_system/vault.py",
        "scripts/setup_vault_secrets.py",
        "configure.py"
    ]
    
    problems = []
    all_valid = True
    
    for path in critical_paths:
        expected_path = Path(target_path) / path
        
        if expected_path.exists():
            print(f"   -> [✅ OK] Componente crítico encontrado: {path}")
            logger.info(f"Validação OK: {path}")
        else:
            problem = f"Componente crítico AUSENTE: {expected_path}"
            print(f"   -> [❌ FALHA] {problem}")
            logger.error(f"Validação FALHA: {path}")
            problems.append(problem)
            all_valid = False
    
    # Validação adicional: verifica se placeholders foram realmente substituídos
    print("   -> Verificando se placeholders foram completamente substituídos...")
    placeholder_check = check_remaining_placeholders()
    
    if placeholder_check:
        problems.extend(placeholder_check)
        all_valid = False
    
    validation_status = "SUCESSO" if all_valid else "FALHA"
    logger.info(f"Validação concluída com status: {validation_status}")
    
    return all_valid, problems

def check_remaining_placeholders() -> List[str]:
    """
    Verifica se ainda existem placeholders não substituídos no projeto.
    
    Returns:
        List[str]: Lista de arquivos que ainda contêm placeholders
    """
    files_with_placeholders = []
    
    try:
        files_to_check = get_files_to_process('.')
        
        for file_path in files_to_check:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    if PLACEHOLDER_PATH in f.read():
                        files_with_placeholders.append(file_path)
            except:
                continue
    except Exception as e:
        logger.warning(f"Erro na verificação de placeholders: {str(e)}")
    
    return files_with_placeholders

def rollback_changes() -> bool:
    """
    Restaura o projeto a partir do backup em caso de falha.
    
    Returns:
        bool: True se rollback foi bem-sucedido, False caso contrário
    """
    print("\n[AÇÃO DE EMERGÊNCIA] ⏪  Iniciando rollback...")
    logger.warning("Iniciando processo de rollback")
    
    if not BACKUP_DIR or not os.path.exists(BACKUP_DIR):
        error_msg = "Impossível fazer rollback: pasta de backup não encontrada"
        print(f"   -> ❌ {error_msg}")
        logger.error(error_msg)
        return False

    try:
        print(f"   -> Restaurando a partir de '{BACKUP_DIR}'...")
        
        # Remove todos os itens exceto o backup e o log
        for item in os.listdir('.'):
            if item != BACKUP_DIR and item != LOG_FILE:
                item_path = Path(item)
                if item_path.is_dir():
                    shutil.rmtree(item_path)
                else:
                    item_path.unlink()
        
        # Restaura arquivos do backup
        for item in os.listdir(BACKUP_DIR):
            source = Path(BACKUP_DIR) / item
            destination = Path('.') / item
            shutil.move(str(source), str(destination))
        
        # Remove a pasta de backup
        shutil.rmtree(BACKUP_DIR)
        
        print("   -> ✅ Rollback concluído. Projeto restaurado ao estado original.")
        logger.info("Rollback executado com sucesso")
        return True
        
    except Exception as e:
        error_msg = f"ERRO CRÍTICO durante rollback: {str(e)}"
        print(f"   -> ❌ {error_msg}")
        logger.critical(error_msg)
        return False

def cleanup_successful_setup():
    """Remove arquivos temporários após configuração bem-sucedida."""
    print("\n[PASSO 4 de 5] 🧹  Limpeza pós-configuração...")
    
    try:
        if BACKUP_DIR and os.path.exists(BACKUP_DIR):
            print(f"   -> Backup mantido em '{BACKUP_DIR}' (remova manualmente se desejar)")
            
        logger.info("Limpeza concluída")
        
    except Exception as e:
        logger.warning(f"Erro na limpeza: {str(e)}")

def generate_setup_report(files_changed: int, validation_passed: bool, problems: List[str]):
    """Gera relatório final da configuração."""
    print(f"\n[PASSO 5 de 5] 📋  Relatório de Configuração")
    print("=" * 50)
    print(f"📁 Projeto:             {os.path.basename(os.getcwd())}")
    print(f"🔄 Arquivos alterados:    {files_changed}")
    print(f"✅ Validação:           {'PASSOU' if validation_passed else 'FALHOU'}")
    
    if problems:
        print("\n⚠️  Problemas encontrados:")
        for problem in problems:
            print(f"   - {problem}")
    
    print(f"📝 Log detalhado:       {LOG_FILE}")
    print("=" * 50)
    
    logger.info(f"Relatório final - Arquivos: {files_changed}, Validação: {validation_passed}")

def main():
    """Função principal do script de configuração."""
    print("🚀 --- Script de Configuração e Validação Automática ---")
    print("    Versão Enterprise com Backup e Rollback Automático")
    
    start_time = time.time()
    current_path = os.getcwd()
    
    # Validação inicial
    if not os.path.exists("configure.py"):
        error_msg = "❌ ERRO: Execute este script a partir da pasta raiz do projeto"
        print(error_msg)
        logger.error("Script executado fora do diretório raiz do projeto")
        sys.exit(1)
    
    logger.info("=== INÍCIO DA CONFIGURAÇÃO AUTOMÁTICA ===")
    logger.info(f"Diretório de trabalho: {current_path}")
    
    try:
        # Passo 1: Backup
        if not create_backup(current_path):
            raise BackupError("Falha na criação do backup")
        
        # Passo 2: Configuração
        files_changed = configure_paths(current_path)
        if files_changed == -1:
            raise ConfigurationError("Falha na substituição de caminhos")
        
        # Passo 3: Validação
        validation_passed, problems = validate_setup(current_path)
        if not validation_passed:
            raise ConfigurationError(f"Validação falhou: {'; '.join(problems)}")
        
        # Passo 4: Limpeza
        cleanup_successful_setup()
        
        # Passo 5: Relatório
        generate_setup_report(files_changed, validation_passed, problems)
        
        # Sucesso!
        elapsed_time = time.time() - start_time
        success_msg = f"\n🎉 CONFIGURAÇÃO CONCLUÍDA COM SUCESSO em {elapsed_time:.1f}s!"
        print(success_msg)
        print("\n📋 Próximos passos:")
        print("   1. Criar ambiente virtual: python -m venv venv")
        print("   2. Ativar ambiente: source venv/bin/activate  (Linux/Mac) ou venv\\Scripts\\activate  (Windows)")
        print("   3. Instalar dependências: pip install -r requirements.txt")
        
        logger.info(f"=== CONFIGURAÇÃO CONCLUÍDA COM SUCESSO em {elapsed_time:.1f}s ===")
        
    except (BackupError, ConfigurationError) as e:
        error_msg = f"\n❌ Erro durante configuração: {str(e)}"
        print(error_msg)
        logger.error(f"Erro na configuração: {str(e)}")
        
        # Tentativa de rollback automático
        if rollback_changes():
            print("\n✅ Sistema restaurado ao estado original via rollback automático.")
            logger.info("Rollback automático executado com sucesso")
        else:
            print(f"\n⚠️  ATENÇÃO: Rollback falhou. Restaure manualmente a partir de '{BACKUP_DIR}'")
            logger.critical("Rollback falhou - intervenção manual necessária")
        
        sys.exit(1)
        
    except Exception as e:
        error_msg = f"\n💥 Erro inesperado: {str(e)}"
        print(error_msg)
        logger.critical(f"Erro inesperado: {str(e)}")
        
        # Rollback em caso de erro inesperado
        rollback_changes()
        sys.exit(1)

if __name__ == "__main__":
    main()
