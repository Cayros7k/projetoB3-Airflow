import zipfile
import pendulum
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from airflow.hooks.postgres_hook import PostgresHook

@dag(
    schedule='*/5 * * * *',
    start_date=pendulum.datetime(2018, 1, 1, tz="UTC"),
    catchup=False,
    tags=["b3"],
)

def b3():
    @task()
    def extract_and_process_data():
        Ano = 2018
        id_counter = 1

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

                dados_acoes.columns = ["tipo_registro", "data_pregao", "cod_bdi", "cod_negociacao", "tipo_mercado", "nome_empresa", "especificacao_papel", "prazo_dias_merc_termo" \
                                       ,"moeda_referencia", "preco_abertura", "preco_maximo", "preco_minimo", "preco_medio", "preco_ultimo_negocio", "preco_melhor_oferta_compra" \
                                        ,"preco_melhor_oferta_venda", "numero_negocios", "quantidade_papeis_negociados", "volume_total_negociado", "preco_exercicio" \
                                            ,"indicador_correcao_precos", "data_vencimento", "fator_cotacao", "preco_exercicio_pontos", "codigo_isin", "num_distribuicao_papel"]

                #Adicionar a coluna ID Auto Increment
                #dados_acoes['id_pregao'] = range(id_counter, id_counter + len(dados_acoes))
                #id_counter += len(dados_acoes)
                
                linha=len(dados_acoes["data_pregao"])
                dados_acoes=dados_acoes.drop(linha-1)

                #Ajustando valores com vírgula
                listaVirgula=["preco_abertura", "preco_maximo", "preco_minimo", "preco_medio", "preco_ultimo_negocio", "preco_melhor_oferta_compra" \
                              ,"preco_melhor_oferta_venda", "volume_total_negociado", "preco_exercicio", "preco_exercicio_pontos"]

                for coluna in listaVirgula:
                    dados_acoes[coluna]=[i/100. for i in dados_acoes[coluna]]

                dados_acoes['data_pregao'] = pd.to_datetime(dados_acoes.data_pregao)
                dados_acoes['data_pregao'] = dados_acoes['data_pregao'].dt.strftime('%d/%m/%Y')

                mercado_descricoes = {
                    10: 'VISTA',
                    12: 'EXERCÍCIO DE OPÇÕES DE COMPRA',
                    13: 'EXERCÍCIO DE OPÇÕES DE VENDA',
                    17: 'LEILÃO',
                    20: 'FRACIONÁRIO',
                    30: 'TERMO',
                    50: 'FUTURO COM RETENÇÃO DE GANHO',
                    60: 'FUTURO COM MOVIMENTAÇÃO CONTÍNUA',
                    70: 'OPÇÕES DE COMPRA',
                    80: 'OPÇÕES DE VENDA',
                }

                bdi_descricoes = {
                    2: 'LOTE PADRAO',
                    5: 'SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA',
                    6: 'CONCORDATARIAS',
                    7: 'RECUPERACAO EXTRAJUDICIAL', 
                    8: 'RECUPERAÇÃO JUDICIAL',
                    9: 'RAET - REGIME DE ADMINISTRACAO ESPECIAL TEMPORARIA',
                    10: 'DIREITOS E RECIBOS',
                    11: 'INTERVENCAO',
                    12: 'FUNDOS IMOBILIARIOS',
                    14: 'CERT.INVEST/TIT.DIV.PUBLICA', 
                    18: 'OBRIGACÕES',
                    22: 'BÔNUS (PRIVADOS)',
                    26: 'APOLICES/BÔNUS/TITULOS PUBLICOS',
                    32: 'EXERCICIO DE OPCOES DE COMPRA DE INDICES',
                    33: 'EXERCICIO DE OPCOES DE VENDA DE INDICES',
                    38: 'EXERCICIO DE OPCOES DE COMPRA',
                    42: 'EXERCICIO DE OPCOES DE VENDA',
                    46: 'LEILAO DE NAO COTADOS', 
                    48: 'LEILAO DE PRIVATIZACAO',
                    49: 'LEILAO DO FUNDO RECUPERACAO ECONOMICA ESPIRITO SANTO',
                    50: 'LEILAO',
                    51: 'LEILAO FINOR',
                    52: 'LEILAO FINAM',
                    53: 'LEILAO FISET',
                    54: 'LEILAO DE ACÕES EM MORA',
                    56: 'VENDAS POR ALVARA JUDICIAL',
                    58: 'OUTROS',
                    60: 'PERMUTA POR ACÕES', 
                    61: 'META',
                    62: 'MERCADO A TERMO',
                    66: 'DEBENTURES COM DATA DE VENCIMENTO ATE 3 ANOS',
                    68: 'DEBENTURES COM DATA DE VENCIMENTO MAIOR QUE 3 ANOS',
                    70: 'FUTURO COM RETENCAO DE GANHOS',
                    71: 'MERCADO DE FUTURO',
                    74: 'OPCOES DE COMPRA DE INDICES',
                    75: 'OPCOES DE VENDA DE INDICES',
                    78: 'OPCOES DE COMPRA',
                    82: 'OPCOES DE VENDA',
                    83: 'BOVESPAFIX',
                    84: 'SOMA FIX',
                    90: 'TERMO VISTA REGISTRADO',
                    96: 'MERCADO FRACIONARIO',
                    99: 'TOTAL GERAL',
                }

                dados_pregao = dados_acoes[['cod_bdi', 'cod_negociacao', 'data_pregao', 'preco_melhor_oferta_compra', 'preco_melhor_oferta_venda', 
                                        'moeda_referencia', 'numero_negocios', 'preco_abertura', 'preco_maximo', 'preco_medio', 'preco_minimo', 
                                        'preco_ultimo_negocio' , 'tipo_mercado', 'tipo_registro', 'volume_total_negociado']]
        
                dados_mercado = dados_acoes[['tipo_mercado']].drop_duplicates()
                dados_mercado['desc_tipo_mercado'] = dados_mercado['tipo_mercado'].map(mercado_descricoes)

                dados_empresas = dados_acoes[['cod_negociacao', 'nome_empresa', 'tipo_mercado']].drop_duplicates() 

                dados_papeis = dados_acoes[['especificacao_papel', 'num_distribuicao_papel', 'cod_isin']].drop_duplicates()

                dados_cod_bdi = dados_acoes[['cod_bdi']].drop_duplicates()
                dados_cod_bdi['desc_cod_bdi'] = dados_cod_bdi['cod_bdi'].map(bdi_descricoes)

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
            DROP TABLE IF EXISTS fato_pregao;
            DROP TABLE IF EXISTS dim_empresas;
            DROP TABLE IF EXISTS dim_cod_bdi;
            DROP TABLE IF EXISTS dim_tipo_mercado;
            DROP TABLE IF EXISTS dim_papel;

            CREATE TABLE dim_empresas(
                cod_negociacao varchar primary key
                , nome_empresa varchar
                , tipo_mercado int
            );

            CREATE TABLE dim_cod_bdi(
                cod_bdi int primary key
                , desc_cod_bdi varchar
            );

            CREATE TABLE dim_tipo_mercado(
                tipo_mercado int primary key
                , desc_tipo_mercado varchar
            );

            CREATE TABLE dim_papel(
                especificacao_papel varchar primary key
                , num_distribuicao_papel int
                , cod_isin varchar
            );

            CREATE TABLE fato_pregao (
                cod_bdi int
                , cod_negociacao varchar
                , especificacao_papel varchar
                , tipo_mercado int
                , data_pregao date
                , preco_melhor_oferta_compra float
                , preco_melhor_oferta_venda float
                , moeda_referencia varchar
                , numero_negocios int
                , preco_abertura float
                , preco_fechamento float
                , preco_maximo float
                , preco_medio float
                , preco_minimo float
                , preco_ultimo_negocio float
                , tipo_registro int
                , volume_total_negociado int,

                CONSTRAINT fk_empresa FOREING KEY (cod_negociacao) REFERENCES dim_empresas(cod_negociacao) ON DELETE CASCADE
                , CONSTRAINT fk_bdi FOREING KEY (cod_bdi) REFERENCES dim_cod_bdi(cod_bdi) ON DELETE CASCADE
                , CONSTRAINT fk_mercado FOREING KEY (tipo_mercado) REFERENCES dim_tipo_mercado(tipo_mercado) ON DELETE CASCADE
                , CONSTRAINT fk_papel FOREING KEY (especificacao_papel) REFERENCES dim_papel(especificacao_papel) ON DELETE CASCADE;
            );            
        """
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    
    @task()
    def load_raw(dados_pregao, dados_mercado, dados_empresas, dados_papeis, dados_cod_bdi):
        hook = PostgresHook(postgres_conn_id='postgres-airflow')
        conn = hook.get_conn()
        cur = conn.cursor()

        for item in dados_mercado(orient="records"):
            query = f"""
            INSERT INTO public."dim_tipo_mercado"(tipo_mercado, desc_tipo_mercado)
            VALUES({item['tipo_mercado']}, {item['desc_tipo_mercado']});
            """
            print(query)
            cur.execute(query)
        conn.commit()

        for item in dados_empresas(orient="records"):
            query = f"""
            INSERT INTO public."dim_empresas"(cod_negociacao, nome_empresa, tipo_mercado)
            VALUES({item['cod_negociacao']}, {item['nome_empresa']}, {item['tipo_mercado']})
            """
            print(query)
            cur.execute(query)
        conn.commit()

        for item in dados_papeis(orient="records"):
            query = f"""
            INSERT INTO public."dim_papel"(especificacao_papel, num_distribuicao_papel, cod_isin)
            VALUES({item['especificacao_papel']}, {item['num_distribuicao_papel']}, {item['cod_isin']})
            """
            print(query)
            cur.execute(query)
        conn.commit()

        for item in dados_cod_bdi(orient="records"):
            query = f"""
            INSERT INTO public."dim_cod_bdi"(cod_bdi, desc_cod_bdi)
            VALUES({item['cod_bdi']}, {item['desc_cod_bdi']})
            """
            print(query)
            cur.execute(query)
        conn.commit()

        for item in dados_pregao(orient="records"):
            query = f"""
            INSERT INTO public."fato_pregao"
            (cod_bdi, cod_negociacao, especificacao_papel, tipo_mercado, data_pregao, preco_melhor_oferta_compra, preco_melhor_oferta_venda, 
            moeda_referencia, numero_negocios, preco_abertura, preco_fechamento, preco_maximo, preco_medio, preco_minimo, preco_ultimo_negocio, tipo_registro, volume_total_negociado)
            VALUES({item['cod_bdi']}, {item['cod_negociacao']}, {item['especificacao_papel']}, {item['tipo_mercado']}, {item['data_pregao']}, {item['preco_melhor_oferta_compra']},
            {item['preco_melhor_oferta_venda']}, {item['moeda_referencia']}, {item['numero_negocios']}, {item['preco_abertura']}, {item['preco_fechamento']}, {item['preco_maximo']},
            {item['preco_medio']}, {item['preco_minimo']}, {item['preco_ultimo_negocio']}, {item['tipo_registro']}, {item['volume_total_negociado']})
            """
            print(query)
            cur.execute(query)
        conn.commit()

    extract_and_process_data1 = extract_and_process_data()
    createTables1 = createTables()
    load_raw1 = load_raw(extract_and_process_data1)    

    createTables1 >> extract_and_process_data1 >> load_raw1
b3()