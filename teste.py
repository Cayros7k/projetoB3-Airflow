import zipfile
import pendulum
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def check_day_week(type_format: str):
        day_week = str(datetime.now().strftime("%A"))

        day_mapping = {
            "Tuesday": 1,
            "Wednesday": 1,
            "Thursday": 1,
            "Friday": 1,
            "Saturday": 1,
            "Sunday": 2,
            "Monday": 3,
        }

        interval = day_mapping.get(day_week)

        check_format = {"date": "%Y-%m-%d", "date_br": "%d%m%Y"}

        date_format = check_format.get(type_format)

        return (datetime.now().date() - timedelta(days=interval)).strftime(
            format=date_format
        )

def extract_process():
        date = check_day_week(type_format="date_br") 
        
        print("Iniciando procedimento no ano "+str(date)+"...")  
        
        #URL do arquivo ZIP para baixar baixar
        url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D08122023.ZIP" + str(date) + ".ZIP"
        print(url)
        #Download do arquivo ZIP
        print("Baixando arquivo do dia " + str(date) +"...")

        response = requests.get(url)

        #Verificando se o download deu certo
        if response.status_code == 200:

            zip_file = zipfile.ZipFile(BytesIO(response.content))

            file_list = zip_file.namelist()

            chosen_file = 'COTAHIST_D' + str(date) + '.TXT'

            extracted_file_content = zip_file.read(chosen_file)

            tamanho_campos=[2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]
            
            dados_acoes = pd.read_fwf(BytesIO(extracted_file_content), widths=tamanho_campos, header=0)
            
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

            #Ajustando valores com vírgula
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

            dados_acoes['data_pregao'] = pd.to_datetime(dados_acoes.data_pregao)
            dados_acoes['data_pregao'] = dados_acoes['data_pregao'].dt.strftime('%Y-%m-%d')
            dados_acoes[['cod_bdi','fator_cotacao', 'numero_negocios', 'quantidade_papeis_negociados', 'volume_total_negociado', 'preco_exercicio_pontos', 'num_distribuicao_papel']] \
                = dados_acoes[['cod_bdi', 'fator_cotacao', 'numero_negocios', 'quantidade_papeis_negociados', 'volume_total_negociado', 'preco_exercicio_pontos', 'num_distribuicao_papel']].astype(int)
            
            zip_file.close()
            print(f"Dia {date} Concluído")
            print(dados_acoes)
        else:
            print(f"Falha ao baixar o arquivo ")

        # hook = PostgresHook(postgres_conn_id='postgres-airflow')
        # conn = hook.get_conn()
        # cur = conn.cursor()
        # try:
        #     engine = create_engine("postgresql+psycopg2://airflow:airflow@host.docker.internal/airflow")
        #     dados_acoes.to_sql(name='stage', con=engine, if_exists='append', index=False)
        #     conn.commit()
        #     cur.close()
        # except Exception as e:
        #     print(e)
extract_process()