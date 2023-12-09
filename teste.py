        # for _, row in dados_acoes.iterrows():
        #     query = f"""
        #     INSERT INTO stage.stg(tipo_registro, data_pregao, cod_bdi, cod_negociacao, tipo_mercado, nome_empresa, especificacao_papel,
        #     prazo_dias_merc_termo, moeda_referencia, preco_abertura, preco_maximo, preco_minimo, preco_medio, preco_ultimo_negocio,
        #     preco_melhor_oferta_compra, preco_melhor_oferta_venda, numero_negocios, quantidade_papeis_negociados, volume_total_negociado,
        #     preco_exercicio, indicador_correcao_precos, data_vencimento, fator_cotacao, preco_exercicio_pontos, cod_isin, num_distribuicao_papel) 
        #     VALUES ({row['tipo_registro']}, '{row['data_pregao']}', {row['cod_bdi']},'{row['cod_negociacao']}', {row['tipo_mercado']}, 
        #     '{row['nome_empresa']}', '{row['especificacao_papel']}', '{row['prazo_dias_merc_termo']}', '{row['moeda_referencia']}', {row['preco_abertura']},
        #     {row['preco_maximo']}, {row['preco_minimo']}, {row['preco_medio']}, {row['preco_ultimo_negocio']}, {row['preco_melhor_oferta_compra']}, 
        #     {row['preco_melhor_oferta_venda']}, {row['numero_negocios']}, {row['quantidade_papeis_negociados']}, {row['volume_total_negociado']}, 
        #     {row['preco_exercicio']}, {row['indicador_correcao_precos']}, {row['data_vencimento']}, {row['fator_cotacao']}, {row['preco_exercicio_pontos']}, 
        #     '{row['cod_isin']}', {row['num_distribuicao_papel']});
        #     """
        #     cur.execute(query)
        #     break