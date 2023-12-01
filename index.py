import pandas as pd
import psycopg2

from connectioninfo import *

# Configuração da conexão
conexao = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
)

query = "select * from analise_anual_registros;"
query2 = "select * from analise_anual_registros where year = 2017;"
consulta_erros = "select * from view_erros_registros;"
query_estados = "SELECT DISTINCT stateprovince FROM distribuicao_estado_cidade;"
query_top_contributors = "select * from top_contributors;"
df_estados = pd.read_sql_query(query_estados, conexao)
df_top_contributors = pd.read_sql_query(query_top_contributors, conexao)
df = pd.read_sql_query(query, conexao)
df_erros = pd.read_sql_query(consulta_erros, conexao)
df = df.sort_values(by=['year'])
df_erros = df_erros.sort_values(by=['quantidade_registros'], ascending=True)

# Fechar a conexão
conexao.close()
