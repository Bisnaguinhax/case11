#!/usr/bin/env python3
"""
Script de teste de conectividade com PostgreSQL - Demonstração Técnica
Facilita a configuração do ambiente para avaliação.
"""

import sys
from sqlalchemy import create_engine, text

def test_database_connection():
    """Testa a conexão com o PostgreSQL e valida estrutura básica."""
    
    print("=" * 60)
    print("🔍 TESTE DE CONECTIVIDADE - POSTGRESQL DATA MART")
    print("=" * 60)
    
    # Instrução clara 
    print("\n📋 INSTRUÇÕES PARA A BANCA:")
    print("   1. Preencha as credenciais do seu PostgreSQL abaixo")
    print("   2. Execute: python3 scripts/teste_conexao_pg.py")
    print("   3. Se der erro, ajuste as credenciais e tente novamente")
    print("-" * 60)
    
    # --- CONFIGURAÇÕES - PREENCHA SUAS CREDENCIAIS ---
    DB_CONFIG = {
        "user": "SEU_USUARIO_DATAMART",
        "password": "SUA_SENHA_DATAMART", 
        "host": "localhost",  # ou o IP do seu servidor
        "port": "5432",
        "database": "dm_st_gestao_indicadores"
    }
    
    # Constrói a URL de conexão
    db_url = (f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
              f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    # Mostra URL mascarada para debug
    masked_url = db_url.replace(DB_CONFIG['password'], '********')
    print(f"\n🔗 Tentando conectar: {masked_url}")
    
    try:
        # Teste de conexão básica
        print("\n⏳ Testando conectividade...")
        engine = create_engine(db_url)
        
        with engine.connect() as connection:
            print("   ✅ Conexão estabelecida com sucesso!")
            
            # Teste adicional: verifica se consegue executar query
            print("\n⏳ Testando execução de queries...")
            result = connection.execute(text("SELECT version();"))
            db_version = result.fetchone()[0]
            print(f"   ✅ PostgreSQL Version: {db_version}")
            
            # Teste de criação de tabela (para validar permissões)
            print("\n⏳ Testando permissões de escrita...")
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS airflow_test_table (
                    id SERIAL PRIMARY KEY,
                    test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            print("   ✅ Permissões de escrita OK!")
            
            # Limpeza
            connection.execute(text("DROP TABLE IF EXISTS airflow_test_table;"))
            connection.commit()
            
        print("\n" + "=" * 60)
        print("🎉 TESTE CONCLUÍDO COM SUCESSO!")
        print("   O PostgreSQL está pronto para o Airflow.")
        print("   Próximo passo: execute 'python3 configure.py'")
        print("=" * 60)
        
        return True
        
    except ImportError:
        print("\n❌ ERRO: Biblioteca psycopg2 não encontrada!")
        print("   Instale com: pip install psycopg2-binary")
        return False
        
    except Exception as e:
        print(f"\n❌ FALHA NA CONEXÃO!")
        print(f"   Erro: {str(e)}")
        print("\n🔧 POSSÍVEIS SOLUÇÕES:")
        print("   1. Verifique se o PostgreSQL está rodando")
        print("   2. Confirme as credenciais (usuário/senha)")
        print("   3. Verifique se o banco 'dm_st_gestao_indicadores' existe")
        print("   4. Teste a conectividade: telnet localhost 5432")
        return False

if __name__ == "__main__":
    success = test_database_connection()
    sys.exit(0 if success else 1)
