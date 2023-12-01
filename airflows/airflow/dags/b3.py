import requests
import zipfile
from io import BytesIO
import pandas as pd

#Criando loop para que todos os arquivos sejam baixados de forma automática

def process_data():
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
            
            dados_acoes['id_pregao'] = range(id_counter, id_counter + len(dados_acoes))
            id_counter += len(dados_acoes)

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
            dados_acoes['cod_bdi'] = dados_acoes['cod_bdi'].astype(int)

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

            dados_pregao = dados_acoes[['id_pregao', 'cod_bdi', 'cod_negociacao', 'data_pregao', 'preco_melhor_oferta_compra', 'preco_melhor_oferta_venda', 
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

            print (dados_cod_bdi.head())

            zip_file.close()

        else:
            print(f"Falha ao baixar o arquivo ")
        
        Ano += 1

process_data()
        

