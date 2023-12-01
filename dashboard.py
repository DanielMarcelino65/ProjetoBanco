from app import * 
from dash import html, dcc, Input, Output
from index import df, df_erros, df_estados, df_top_contributors
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash_bootstrap_templates import ThemeSwitchAIO
import psycopg2

from connectioninfo import *


opcoes_estados = [{'label': estado, 'value': estado} for estado in df_estados['stateprovince'].unique() if estado is not None]


app.layout = html.Div([
    ThemeSwitchAIO(aio_id='theme-switch'),
    html.Br(),  
    html.H1('My Dashboard'),
    html.Br(),
    html.Div([  
        html.Div([  
            html.H3('Registros por ano'),
            dcc.Graph(id='graph1'),
        ], className='six columns'),
        html.Div([  
            html.H3('Top 10 Contribuidores'),
            dcc.Graph(id='graph_top_contributors'),
        ], className='six columns'),
        html.Div([  
            html.H3('Gráfico de Erros'),
            dcc.Graph(id='graph_error'),
        ], className='six columns'),
    ], className='row'),
    html.Div([  
        html.Div([
            html.H3('Registros com e sem ano'),
            dcc.Graph(id='graph_circle'),
        ], className='twelve columns'),
    ], className='row'),
    html.Div([
        dcc.Dropdown(id='estado_picker', options=opcoes_estados, value='Maranhão', placeholder="Selecione um Estado"),
        dcc.Graph(id='graph_estado_cidade'),
    ]),
])


@app.callback(
    Output('graph1', 'figure'),
    Output('graph_circle', 'figure'),
    Output('graph_error', 'figure'),
    Output('graph_top_contributors', 'figure'),
    Input(ThemeSwitchAIO.ids.switch('theme-switch'), 'value')
)
def update_graph(theme):
    if theme == 'dark':
        template = 'plotly_dark'
    else:
        template = 'plotly_white'

    df_bar = df.dropna(subset=['year'])

    df_bar['year'] = df_bar['year'].astype(int).astype(str)

    fig1 = px.line(df_bar, x='year', y='total_registros', template=template, 
                  category_orders={'year': sorted(df_bar['year'].unique())})
    
    registros_com_ano = df[df['year'].notna()]['total_registros'].sum()
    registros_sem_ano = df[df['year'].isna()]['total_registros'].sum()
    total_registros = df['total_registros'].sum()
    porcentagem_com_ano = (registros_com_ano / total_registros) * 100
    porcentagem_sem_ano = (registros_sem_ano / total_registros) * 100

    fig_circle = go.Figure(go.Pie(
        labels=['Registros com Ano', 'Registros sem Ano'],
        values=[registros_com_ano, registros_sem_ano],
        hole=.7,
        textinfo='label+percent',
        showlegend=False,
        marker=dict(colors=['blue', 'lightgray'])
    ))
    fig_circle.update_layout(
        title_text='Porcentagem de Registros com e sem Ano',
        annotations=[dict(text=f'{porcentagem_com_ano:.2f}%', x=0.5, y=0.5, font_size=20, showarrow=False)]
    )

    fig_error = px.bar(df_erros, x='quantidade_registros', y='issue', template=template)
    fig_top_contributors = px.bar(df_top_contributors, x='recordedbyperson', y='num_registros', template=template)


    return fig1, fig_circle, fig_error, fig_top_contributors

@app.callback(
    Output('graph_estado_cidade', 'figure'),
    Input('estado_picker', 'value') 
)
def update_graph_estado_cidade(estado_selecionado):
    conexao = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host, 
        port=port
    )

    if estado_selecionado:
        df_filtrado = pd.read_sql_query(f"SELECT locality, total_registros FROM distribuicao_estado_cidade WHERE stateProvince = '{estado_selecionado}'", conexao)
    else:
        df_filtrado = pd.DataFrame(columns=['locality', 'total_registros'])

    conexao.close()
 
    fig_estado_cidade = px.bar(df_filtrado, x='locality', y='total_registros', title=f'Distribuição por Cidades em {estado_selecionado}')

    return fig_estado_cidade


if __name__ == '__main__':
    app.run_server(debug=True, port = 8051)