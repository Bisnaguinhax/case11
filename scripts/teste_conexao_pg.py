from sqlalchemy import create_engine

print("--- Iniciando teste de conexão direta com o PostgreSQL ---")

# Suas credenciais exatas do Data Mart
DB_USER = "felps"
DB_PASSWORD = "senha123@"
DB_HOST = "127.0.0.1" # Forçando o uso de IP
DB_PORT = "5432"
DB_NAME = "dm_st_gestao_indicadores"

# Construindo a URL de conexão explícita
db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Tentando conectar com a URL: {db_url.replace(DB_PASSWORD, '********')}") # Imprime a URL sem a senha

try:
    # Criar o engine e tentar conectar
    engine = create_engine(db_url)
    with engine.connect() as connection:
        print("\n✅ SUCESSO! A conexão com o PostgreSQL foi estabelecida com sucesso!")
        print("Isso significa que as credenciais e o servidor estão corretos.")

except Exception as e:
    print(f"\n❌ FALHA! Ocorreu um erro ao tentar conectar:")
    print(e)

