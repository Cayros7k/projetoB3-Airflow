import zipfile
import pendulum
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

# Define a DAG com agendamento e configurações específicas.
# Projetada para uma única execução, porém foi definida para executar às 3h (UTC) do dia 2 de Janeiro de cada ano seguinte.
@dag(
    schedule='1 3 02 1 *',
    start_date=pendulum.datetime(2018, 2, 1),
    catchup=False,
    tags=["b3_retro"],
)

# DAG para o processamento retroativo dos dados da B3.
def b3_retro():

    # Task que faz o download, processamento e carga dos dados da B3.
    @task()
    def process_data():
        Ano = 2018
        # Enquanto a variável Ano for menor ou igual a 2023:
        while Ano <= 2023:
            print("Iniciando procedimento no ano "+str(Ano)+"...")  
            
            # URL do arquivo ZIP para utilizá-lo.
            url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A" + str(Ano) + ".ZIP"

            # Adquirindo o arquivo ZIP.
            print("Baixando arquivo do ano " + str(Ano) +"...")
            response = requests.get(url)

            # Se a variável response retornar o status 200, começará a etapa de processamento dos dados.
            if response.status_code == 200:
                
                # Processo de extração e utilização do arquivo TXT.
                zip_file = zipfile.ZipFile(BytesIO(response.content))

                file_list = zip_file.namelist()

                chosen_file = 'COTAHIST_A' + str(Ano) + '.TXT'

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
                
                # Remove o rodapé presente no documento.
                linha=len(dados_acoes["data_pregao"])
                dados_acoes=dados_acoes.drop(linha-1)

                # Ajustando valores com vírgula.
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
                print(f"Ano {Ano} Concluído")
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

            # Acrescenta mais um ano para realizar o loop. 
            Ano += 1

            # Garante que, ao iniciar o loop, o dataframe dados_acoes esteja vazio.
            dados_acoes = None
            
    # Task que cria a estrutura da tabela 'stage' no banco de dados.
    @task()
    def createStage():
        # 
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()
        # Se existir, exclui a tabela stage. Em seguir, cria a tabela stage. 
        query = """
            DROP TABLE IF EXISTS stage;

            CREATE TABLE stage (
                id_pregao bigserial primary key
                , tipo_registro bigint
                , data_pregao date
                , cod_bdi bigint
                , cod_negociacao varchar(255)
                , tipo_mercado bigint
                , nome_empresa varchar(255)
                , especificacao_papel varchar(255)
                , prazo_dias_merc_termo varchar(255)
                , moeda_referencia varchar(255)
                , preco_abertura decimal
                , preco_maximo decimal
                , preco_minimo decimal
                , preco_medio decimal
                , preco_ultimo_negocio decimal
                , preco_melhor_oferta_compra decimal
                , preco_melhor_oferta_venda decimal
                , numero_negocios bigint
                , quantidade_papeis_negociados bigint
                , volume_total_negociado bigint
                , preco_exercicio decimal
                , indicador_correcao_precos varchar(255)
                , data_vencimento varchar(255)
                , fator_cotacao bigint
                , preco_exercicio_pontos bigint
                , cod_isin varchar(255)
                , num_distribuicao_papel bigint
            ); 
            """
        cur.execute(query)
        conn.commit()
        cur.close()
   
    # Define variáveis para a chamada das funções criadas.
    process_data1 = process_data()
    createStage1 = createStage()

    # Define a ordem em que as Tasks serão executadas.
    createStage1>> process_data1
b3_retro()

