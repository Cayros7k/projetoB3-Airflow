import zipfile
import pendulum
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

@dag(
    schedule='*/5 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=[""],
)

def b3():
    @task()
    def extract_and_process_data():
        Ano = 2018
        while Ano <= 2023:

            print("Iniciando procedimento no ano "+str(Ano)+"...")  
            #URL do arquivo ZIP para baixar baixar
            url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A" + str(Ano) + ".ZIP"

            #Download do arquivo ZIP
            print("Baixando arquivo do ano " + str(Ano) +"...")
            response = requests.get(url)

            #Verificando se o download deu certo
            if response.status_code == 200:

                zip_file = zipfile.ZipFile(BytesIO(response.content))

                file_list = zip_file.namelist()

                chosen_file = 'COTAHIST_A' + str(Ano) + '.TXT'

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
                "codigo_isin",
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
                dados_acoes['data_pregao'] = dados_acoes['data_pregao'].dt.strftime('%d/%m/%Y')
                print("Arquivo do ano "+str(Ano)+" tratado")
                print("Filtrando arquivo...")

                print (dados_acoes.head())

                zip_file.close()

            else:
                print(f"Falha ao baixar o arquivo ")
            
            Ano += 1
    
    @task()
    def createTables():
        query = """
            
        """
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    
    @task()
    def load_raw(dados_acoes):
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()

        for item in dados_acoes:
            query = f"""
            """
            print(query)
            cur.execute(query)
        conn.commit()

    extract_and_process_data1 = extract_and_process_data()
    createTables1 = createTables()
    load_raw1 = load_raw(extract_and_process_data1)    

    createTables1 >> extract_and_process_data1 >> load_raw1

b3()