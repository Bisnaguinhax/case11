#!/usr/bin/env python3
"""
Script de Validação de Dados de Vendas
======================================
Utiliza Great Expectations para validar a qualidade dos dados consolidados.
Parte do pipeline de dados para demonstração técnica.

Instrução para a banca:
O script `configure.py` irá substituir os placeholders {{AIRFLOW_HOME}}.
"""

import great_expectations as ge
import json
import sys
import os
from pathlib import Path

# Configuração de arquivos - serão substituídos pelo configure.py
ARQUIVO_DE_DADOS = "{{AIRFLOW_HOME}}/data/olist/dados_consolidados.csv"
ARQUIVO_DE_EXPECTATIVAS = "{{AIRFLOW_HOME}}/dags/expectations/vendas_expectations.json"

def validar_arquivos_existem():
    """Verifica se os arquivos necessários existem antes da execução."""
    arquivos = [ARQUIVO_DE_DADOS, ARQUIVO_DE_EXPECTATIVAS]
    
    for arquivo in arquivos:
        if not os.path.exists(arquivo):
            print(f"❌ ERRO: Arquivo não encontrado: {arquivo}")
            return False
    return True

def carregar_dados():
    """Carrega e valida o arquivo CSV de dados."""
    print(f"📄 Carregando dados de: {ARQUIVO_DE_DADOS}")
    
    try:
        df = ge.read_csv(ARQUIVO_DE_DADOS)
        print(f"   -> Sucesso! {len(df)} registros carregados.")
        return df
    except Exception as e:
        print(f"❌ Erro ao carregar dados: {e}")
        return None

def carregar_expectativas():
    """Carrega as expectativas de qualidade do arquivo JSON."""
    print(f"📦 Carregando expectativas de: {ARQUIVO_DE_EXPECTATIVAS}")
    
    try:
        with open(ARQUIVO_DE_EXPECTATIVAS, "r", encoding='utf-8') as f:
            expectations = json.load(f)
        
        print(f"   -> Sucesso! {len(expectations.get('expectations', []))} expectativas carregadas.")
        return expectations
    except Exception as e:
        print(f"❌ Erro ao carregar expectativas: {e}")
        return None

def aplicar_expectativas(df, expectations):
    """Aplica todas as expectativas de qualidade no DataFrame."""
    print("🧪 Aplicando expectativas de qualidade...")
    
    expectativas_aplicadas = 0
    
    for exp in expectations.get("expectations", []):
        try:
            expectation_type = exp["expectation_type"]
            kwargs = exp.get("kwargs", {})
            
            print(f"   -> Aplicando: {expectation_type}")
            
            # Aplica a expectativa
            getattr(df, expectation_type)(**kwargs)
            expectativas_aplicadas += 1
            
        except Exception as e:
            print(f"   -> ⚠️  Falha em {expectation_type}: {e}")
    
    print(f"✅ {expectativas_aplicadas} expectativas aplicadas com sucesso.")
    return expectativas_aplicadas > 0

def executar_validacao(df):
    """Executa a validação final e exibe os resultados."""
    print("🔍 Executando validação final...")
    
    try:
        results = df.validate()
        
        # Análise dos resultados
        total_expectations = results.statistics.get("evaluated_expectations", 0)
        successful = results.statistics.get("successful_expectations", 0)
        success_rate = (successful / total_expectations * 100) if total_expectations > 0 else 0
        
        print("📊 RESULTADOS DA VALIDAÇÃO:")
        print(f"   -> Total de expectativas: {total_expectations}")
        print(f"   -> Expectativas atendidas: {successful}")
        print(f"   -> Taxa de sucesso: {success_rate:.1f}%")
        print(f"   -> Status geral: {'✅ APROVADO' if results.success else '❌ REPROVADO'}")
        
        # Detalhes de falhas (se houver)
        if not results.success:
            print("\n⚠️  DETALHES DAS FALHAS:")
            for result in results.results:
                if not result.success:
                    print(f"   -> {result.expectation_config.expectation_type}: {result.result}")
        
        return results.success
        
    except Exception as e:
        print(f"❌ Erro durante validação: {e}")
        return False

def main():
    """Função principal do script de validação."""
    print("=" * 60)
    print("🚀 INICIANDO VALIDAÇÃO DE DADOS DE VENDAS")
    print("=" * 60)
    
    # 1. Verificar se arquivos existem
    if not validar_arquivos_existem():
        sys.exit(1)
    
    # 2. Carregar dados
    df = carregar_dados()
    if df is None:
        sys.exit(1)
    
    # 3. Carregar expectativas
    expectations = carregar_expectativas()
    if expectations is None:
        sys.exit(1)
    
    # 4. Aplicar expectativas
    if not aplicar_expectativas(df, expectations):
        print("❌ Falha ao aplicar expectativas.")
        sys.exit(1)
    
    # 5. Executar validação
    sucesso = executar_validacao(df)
    
    print("=" * 60)
    if sucesso:
        print("🎉 VALIDAÇÃO CONCLUÍDA COM SUCESSO!")
        print("Os dados estão em conformidade com as expectativas definidas.")
    else:
        print("⚠️  VALIDAÇÃO FALHOU!")
        print("Os dados NÃO atendem a todas as expectativas de qualidade.")
    print("=" * 60)
    
    # Exit code para integração com Airflow
    sys.exit(0 if sucesso else 1)

if __name__ == "__main__":
    main()
