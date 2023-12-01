import pandas as pd
import psycopg2

# Configuração da conexão
conexao = psycopg2.connect(
    dbname="ProjetoMuseu",
    user="postgres",
    password="daniel12345",
    host="localhost",
    port="5432"
)

query = "select * from analise_anual_registros;"
query2 = "select * from analise_anual_registros where year = 2017;"
consulta_erros = "select * from view_erros_registros;"
query_taxon_match = "select * from taxon_match;"
query_estados = "SELECT DISTINCT stateprovince FROM distribuicao_estado_cidade;"
query_top_contributors = "select * from top_contributors;"
df_estados = pd.read_sql_query(query_estados, conexao)
df_top_contributors = pd.read_sql_query(query_top_contributors, conexao)
df = pd.read_sql_query(query, conexao)
df_erros = pd.read_sql_query(consulta_erros, conexao)
df_taxon_match = pd.read_sql_query(query_taxon_match, conexao)
soma_total_matches = df_taxon_match['total_matches'].sum()
df = df.sort_values(by=['year'])
df_erros = df_erros.sort_values(by=['quantidade_registros'], ascending=True)

# Fechar a conexão
conexao.close()
