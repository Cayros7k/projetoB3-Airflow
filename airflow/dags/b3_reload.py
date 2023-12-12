import zipfile
import pendulum
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# Define a DAG com agendamento e configurações específicas.
@dag(
    schedule='0 22 * * *',
    start_date=pendulum.datetime(2023, 12, 1),
    catchup=False,
    tags=["b3_reload"],
)

# DAG para o processamento retroativo dos dados da B3.
def b3_reload():

    # Verifica o dia da semana atual e retorna a data correspondente baseada em um formato específico.
    def check_day_week(type_format: str):
        # Obtém o dia da semana atual.
        day_week = str(datetime.now().strftime("%A"))

        # Mapeamento dos dias da semana para intervalos de dias.
        day_mapping = {
            "Tuesday": 1,
            "Wednesday": 1,
            "Thursday": 1,
            "Friday": 1,
            "Saturday": 1,
            "Sunday": 2,
            "Monday": 3,
        }

        # Obtém o intervalo de dias correspondente ao dia da semana atual.
        interval = day_mapping.get(day_week)

        # Formatos disponíveis para a data.
        check_format = {"date": "%Y-%m-%d", "date_br": "%d%m%Y"}
        
        # Obtém o formato especificado para a data.
        date_format = check_format.get(type_format)

        # Calcula e retorna a data correspondente ao dia da semana passada no formato desejado.
        return (datetime.now().date() - timedelta(days=interval)).strftime(
            format=date_format)
    
    # Task que faz o download, processamento e carga dos dados da B3.
    @task()
    def extract_process_day():
        date = check_day_week(type_format="date_br")  
        
        print("Iniciando procedimento no dia "+str(date)+"...")  
        
        # URL do arquivo ZIP para baixar.
        url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D" + str(date) + ".ZIP"

        # Download do arquivo ZIP
        print("Baixando arquivo do dia " + str(date) +"...")

        response = requests.get(url)

        # Se o response retornar o status 200, ele começa a etapa de processamento dos dados.
        if response.status_code == 200:

            zip_file = zipfile.ZipFile(BytesIO(response.content))

            file_list = zip_file.namelist()

            chosen_file = 'COTAHIST_D' + str(date) + '.TXT'

            extracted_file_content = zip_file.read(chosen_file)

            # Define o tamanho correto dos campos (específicado na documentação da B3).
            tamanho_campos=[2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]
            
            # Adiciona o arquivo TXT em um dataframe chamado dados_acoes.
            dados_acoes = pd.read_fwf(BytesIO(extracted_file_content), widths=tamanho_campos, header=0)
            
            # Define as colunas no dataframe.
            dados_acoes.columns = [    
            "tipo_registro",
            "data_pregao",
            "cod_bdi",
            "cod_negociacao",
            "tipo_mercado",
            "nome_empresa",
            "especificacao_papel",
            "prazo_dias_merc_termo",
            "moeda_referencia",
            "preco_abertura",
            "preco_maximo",
            "preco_minimo",
            "preco_medio",
            "preco_ultimo_negocio",
            "preco_melhor_oferta_compra",
            "preco_melhor_oferta_venda",
            "numero_negocios",
            "quantidade_papeis_negociados",
            "volume_total_negociado",
            "preco_exercicio",
            "indicador_correcao_precos",
            "data_vencimento",
            "fator_cotacao",
            "preco_exercicio_pontos",
            "cod_isin",
            "num_distribuicao_papel"
            ]
            
            linha=len(dados_acoes["data_pregao"])
            dados_acoes=dados_acoes.drop(linha-1)

            # Ajustando valores com vírgula
            listaVirgula=[
            "preco_abertura",
            "preco_maximo",
            "preco_minimo",
            "preco_medio",
            "preco_ultimo_negocio",
            "preco_melhor_oferta_compra",
            "preco_melhor_oferta_venda",
            "volume_total_negociado",
            "preco_exercicio",
            "preco_exercicio_pontos"
            ]

            for coluna in listaVirgula:
                dados_acoes[coluna]=[i/100. for i in dados_acoes[coluna]]

            # Ajustando a coluna 'data_pregao' para o tipo Date.
            dados_acoes['data_pregao'] = pd.to_datetime(dados_acoes.data_pregao)
            dados_acoes['data_pregao'] = dados_acoes['data_pregao'].dt.strftime('%Y-%m-%d')

            # Ajustando algumas colunas para o tipo Int.
            dados_acoes[['cod_bdi','fator_cotacao', 'numero_negocios', 'quantidade_papeis_negociados', 'volume_total_negociado', 'preco_exercicio_pontos', 'num_distribuicao_papel']] \
                = dados_acoes[['cod_bdi', 'fator_cotacao', 'numero_negocios', 'quantidade_papeis_negociados', 'volume_total_negociado', 'preco_exercicio_pontos', 'num_distribuicao_papel']].astype(int)
            
            zip_file.close()
            print(f"Dia {date} Concluído")
            print(dados_acoes)
        else:
            print(f"Falha ao baixar o arquivo ")

        # Define variáveis para realizar a conexão com o banco.
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            # Estabelece conexão com o banco de dados PostgreSQL e carrega os dados na tabela 'stage'
            engine = create_engine("postgresql+psycopg2://airflow:airflow@host.docker.internal/airflow")
            dados_acoes.to_sql(name='stage', con=engine, if_exists='append', index=False)
            conn.commit()
            cur.close()
        except Exception as e:
            print(e)
    
    # Tarefa que recria as estruturas das tabelas dimensões e tabela fato.
    @task()
    def recreateTables():
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()
        # Se existir, deleta as tabelas especificadas em cascata (excluindo as relações).
        cur.execute("""
                    DROP TABLE IF EXISTS dim_tipo_mercado CASCADE;
                    DROP TABLE IF EXISTS dim_empresas CASCADE;
                    DROP TABLE IF EXISTS dim_papeis CASCADE;
                    DROP TABLE IF EXISTS dim_cod_bdi CASCADE;
                    DROP TABLE IF EXISTS fato_pregao CASCADE;
                    """)
        conn.commit()
        # Cria a estrutura da tabela dimensão dim_tipo_mercado.
        cur.execute("""
            CREATE TABLE dim_tipo_mercado (
                tipo_mercado bigint PRIMARY KEY, 
                desc_tipo_mercado varchar(255)
            )
        """)    
        conn.commit()
        # Cria a estrutura da tabela dimensão dim_empresas.
        cur.execute("""
            CREATE TABLE dim_empresas (
                cod_negociacao varchar(255) primary key, 
                nome_empresa varchar(255) 
            ) 
        """)
        conn.commit()
        # Cria a estrutura da tabela dimensão dim_papeis.
        cur.execute("""
            CREATE TABLE dim_papeis (
                especificacao_papel varchar(255) primary key, 
                num_distribuicao_papel bigint, 
                cod_isin varchar(255) 
            )
        """)
        conn.commit()
        # Cria a estrutura da tabela dimensão dim_cod_bdi.
        cur.execute("""
            CREATE TABLE dim_cod_bdi (
                cod_bdi bigint primary key, 
                desc_cod_bdi varchar(255)
            )
        """)
        conn.commit()
        # Cria a estrutura da tabela fato fato_pregao.
        cur.execute("""
            CREATE TABLE fato_pregao (
                id_pregao bigint primary key,
                cod_bdi bigint REFERENCES dim_cod_bdi(cod_bdi),
                tipo_mercado bigint REFERENCES dim_tipo_mercado(tipo_mercado),
                cod_negociacao varchar(255) REFERENCES dim_empresas(cod_negociacao), 
                especificacao_papel varchar(255) REFERENCES dim_papeis(especificacao_papel), 
                data_pregao date, 
                preco_melhor_oferta_compra decimal, 
                preco_melhor_oferta_venda decimal, 
                moeda_referencia varchar(255), 
                numero_negocios bigint, 
                preco_abertura decimal,  
                preco_maximo decimal, 
                preco_medio decimal, 
                preco_minimo decimal, 
                preco_ultimo_negocio decimal,
                tipo_registro bigint, 
                volume_total_negociado bigint
            )
        """)
        conn.commit()
        cur.close()

    # Tarefa que irá inserir os dados nas tabelas criadas de acordo com a tabela 'stage'.
    @task()
    def reLoad():
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()
        # Faz inserção na tabela dimensão dim_tipo_mercado utilizando o 'DISTINCT' para pegar remover duplicatas.    
        cur.execute("""
            INSERT INTO dim_tipo_mercado (
                SELECT DISTINCT tipo_mercado,
                    CASE
                        WHEN tipo_mercado = '10' THEN 'VISTA'
                        WHEN tipo_mercado = '12' THEN 'EXERCICIO DE OPCOESES DE COMPRA'
                        WHEN tipo_mercado = '13' THEN 'EXERCICIO DE OPCOES DE VENDA'
                        WHEN tipo_mercado = '17' THEN 'LEILAO'
                        WHEN tipo_mercado = '20' THEN 'FRACIONARIO'
                        WHEN tipo_mercado = '30' THEN 'TERMO'
                        WHEN tipo_mercado = '50' THEN 'FUTURO COM RETENCAO DE GANHO'
                        WHEN tipo_mercado = '60' THEN 'FUTURO COM MOVIMENTACAO CONTINUA'
                        WHEN tipo_mercado = '70' THEN 'OPCOES DE COMPRA'
                        WHEN tipo_mercado = '80' THEN 'OPCOES DE VENDA'
                        ELSE NULL
                    END AS desc_tipo_mercado
                FROM stage
            )
        """)
        conn.commit()
        
        # Faz inserção na tabela dimensão dim_empresas utilizando o 'DISTINCT' para pegar remover duplicatas.
        cur.execute("""
            INSERT INTO dim_empresas (cod_negociacao, nome_empresa)
            SELECT DISTINCT cod_negociacao, nome_empresa
            FROM stage
            ON CONFLICT DO NOTHING
        """)
        conn.commit()

        # Faz inserção na tabela dimensão dim_papeis utilizando o 'DISTINCT' para pegar remover duplicatas.
        cur.execute("""
            INSERT INTO dim_papeis(especificacao_papel, num_distribuicao_papel, cod_isin)
            SELECT DISTINCT especificacao_papel, num_distribuicao_papel, cod_isin 
            FROM stage
            ON CONFLICT DO NOTHING
        """)
        conn.commit()

        # Faz inserção na tabela dimensão dim_cod_bdi utilizando o 'DISTINCT' para pegar remover duplicatas.
        cur.execute("""
            INSERT INTO dim_cod_bdi (cod_bdi, desc_cod_bdi)
            SELECT DISTINCT cod_bdi,
                CASE
                    WHEN cod_bdi = '2' THEN 'LOTE PADRAO'
                    WHEN cod_bdi = '5' THEN 'SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA'
                    WHEN cod_bdi = '6' THEN 'CONCORDATARIAS'
                    WHEN cod_bdi = '7' THEN 'RECUPERACAO EXTRAJUDICIAL'
                    WHEN cod_bdi = '8' THEN 'RECUPERAÇÃO JUDICIAL'
                    WHEN cod_bdi = '9' THEN 'RAET - REGIME DE ADMINISTRACAO ESPECIAL TEMPORARIA'
                    WHEN cod_bdi = '10' THEN 'DIREITOS E RECIBOS'
                    WHEN cod_bdi = '11' THEN 'INTERVENCAO'
                    WHEN cod_bdi = '12' THEN 'FUNDOS IMOBILIARIOS'
                    WHEN cod_bdi = '14' THEN 'CERT.INVEST/TIT.DIV.PUBLICA'
                    WHEN cod_bdi = '18' THEN 'OBRIGACÕES'
                    WHEN cod_bdi = '22' THEN 'BÔNUS (PRIVADOS)'
                    WHEN cod_bdi = '26' THEN 'APOLICES/BÔNUS/TITULOS PUBLICOS'
                    WHEN cod_bdi = '32' THEN 'EXERCICIO DE OPCOES DE COMPRA DE INDICES'
                    WHEN cod_bdi = '33' THEN 'EXERCICIO DE OPCOES DE VENDA DE INDICES'
                    WHEN cod_bdi = '38' THEN 'EXERCICIO DE OPCOES DE COMPRA'
                    WHEN cod_bdi = '42' THEN 'EXERCICIO DE OPCOES DE VENDA'
                    WHEN cod_bdi = '46' THEN 'LEILAO DE NAO COTADOS'
                    WHEN cod_bdi = '48' THEN 'LEILAO DE PRIVATIZACAO'
                    WHEN cod_bdi = '49' THEN 'LEILAO DO FUNDO RECUPERACAO ECONOMICA ESPIRITO SANTO'
                    WHEN cod_bdi = '50' THEN 'LEILAO'
                    WHEN cod_bdi = '51' THEN 'LEILAO FINOR'
                    WHEN cod_bdi = '52' THEN 'LEILAO FINAM'
                    WHEN cod_bdi = '53' THEN 'LEILAO FISET'
                    WHEN cod_bdi = '54' THEN 'LEILAO DE ACÕES EM MORA'
                    WHEN cod_bdi = '56' THEN 'VENDAS POR ALVARA JUDICIAL'
                    WHEN cod_bdi = '58' THEN 'OUTROS'
                    WHEN cod_bdi = '60' THEN 'PERMUTA POR ACÕES'
                    WHEN cod_bdi = '61' THEN 'META'
                    WHEN cod_bdi = '62' THEN 'MERCADO A TERMO'
                    WHEN cod_bdi = '66' THEN 'DEBENTURES COM DATA DE VENCIMENTO ATE 3 ANOS'
                    WHEN cod_bdi = '68' THEN 'DEBENTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS'
                    WHEN cod_bdi = '70' THEN 'FUTURO COM RETENCAO DE GANHOS'
                    WHEN cod_bdi = '71' THEN 'MERCADO DE FUTURO'
                    WHEN cod_bdi = '74' THEN 'OPCOES DE COMPRA DE INDICES'
                    WHEN cod_bdi = '75' THEN 'OPCOES DE VENDA DE INDICES'
                    WHEN cod_bdi = '78' THEN 'OPCOES DE COMPRA'
                    WHEN cod_bdi = '82' THEN 'OPCOES DE VENDA'
                    WHEN cod_bdi = '83' THEN 'BOVESPAFIX'
                    WHEN cod_bdi = '84' THEN 'SOMA FIX'
                    WHEN cod_bdi = '90' THEN 'TERMO VISTA REGISTRADO'
                    WHEN cod_bdi = '96' THEN 'MERCADO FRACIONARIO'
                    WHEN cod_bdi = '99' THEN 'TOTAL GERAL'
                    ELSE NULL
                END AS desc_cod_bdi
            FROM stage
        """)
        conn.commit()

        # Faz inserção na tabela fato fato_pregao, selecionando as tabelas para compôr a tabela.
        cur.execute("""
            INSERT INTO fato_pregao(
                id_pregao, cod_bdi, tipo_mercado, cod_negociacao, especificacao_papel, data_pregao, preco_melhor_oferta_compra, 
                preco_melhor_oferta_venda, moeda_referencia, numero_negocios, preco_abertura, preco_maximo, preco_medio, preco_minimo, 
                preco_ultimo_negocio, tipo_registro, volume_total_negociado
            )
            SELECT
                s.id_pregao,
                dc.cod_bdi, 
                dtm.tipo_mercado,
                de.cod_negociacao, 
                dp.especificacao_papel,  
                s.data_pregao, 
                s.preco_melhor_oferta_compra, 
                s.preco_melhor_oferta_venda,
                s.moeda_referencia, 
                s.numero_negocios, 
                s.preco_abertura,  
                s.preco_maximo, 
                s.preco_medio, 
                s.preco_minimo, 
                s.preco_ultimo_negocio, 
                s.tipo_registro, 
                s.volume_total_negociado 
            FROM stage s
            JOIN dim_cod_bdi dc ON s.cod_bdi = dc.cod_bdi
            JOIN dim_tipo_mercado dtm ON s.tipo_mercado = dtm.tipo_mercado
            JOIN dim_empresas de ON s.cod_negociacao = de.cod_negociacao
            JOIN dim_papeis dp ON s.especificacao_papel = dp.especificacao_papel
        """)
        conn.commit()
        cur.close()  

    # Define variáveis para a chamada das funções criadas.
    recreateTables1 = recreateTables()
    extract_process_day1 = extract_process_day()
    reLoad1 = reLoad()

    # Define a ordem em que as Tasks serão executadas.
    recreateTables1 >> extract_process_day1 >> reLoad1

b3_reload()