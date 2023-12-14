# 1. Listar empresas que tiveram o maior volume de negociações em diferentes anos.
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
SELECT 
    de.nome_empresa, 
    de.cod_negociacao, 
    CAST(ano AS varchar) AS ano_formatado,
    total_volume
FROM (
    SELECT 
        fp.cod_negociacao, 
        EXTRACT(YEAR FROM fp.data_pregao) AS ano, 
        SUM(fp.volume_total_negociado) AS total_volume,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM fp.data_pregao) ORDER BY SUM(fp.volume_total_negociado) DESC) AS ranking
    FROM fato_pregao fp
    GROUP BY fp.cod_negociacao, EXTRACT(YEAR FROM fp.data_pregao)
) AS ranked
JOIN dim_empresas de ON ranked.cod_negociacao = de.cod_negociacao
WHERE ranking = 1
ORDER BY ano, total_volume DESC;
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝

# 2. Número total de pregões por empresa.
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
WITH ranked_companies AS (
    SELECT 
        de.nome_empresa,
        COUNT(*) AS total_pregoes,
        ROW_NUMBER() OVER(PARTITION BY de.nome_empresa ORDER BY COUNT(*) DESC) AS company_rank
    FROM fato_pregao fp
    JOIN dim_empresas de ON fp.cod_negociacao = de.cod_negociacao
    GROUP BY de.nome_empresa, EXTRACT(YEAR FROM fp.data_pregao)
)
SELECT 
    nome_empresa,
    SUM(total_pregoes) AS total_pregoes
FROM ranked_companies
WHERE company_rank <= 5
GROUP BY nome_empresa
ORDER BY SUM(total_pregoes) DESC
LIMIT 5;
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝

# 3. Obtém o total de negociações por empresa.
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
SELECT de.nome_empresa, COUNT(*) AS total_negociacoes
FROM fato_pregao fp
JOIN dim_empresas de ON fp.cod_negociacao = de.cod_negociacao
GROUP BY de.nome_empresa
ORDER BY total_negociacoes DESC;
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝

# 4. Busca o papel com o maior preço máximo negociado.
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
SELECT dp.especificacao_papel, MAX(fp.preco_maximo) AS maior_preco_maximo
FROM fato_pregao fp
JOIN dim_papeis dp ON fp.especificacao_papel = dp.especificacao_papel
GROUP BY dp.especificacao_papel
ORDER BY maior_preco_maximo DESC
LIMIT 1;
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝

# 5. Busca os 10 dias com o maior número de negociações.
╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗
SELECT data_pregao, COUNT(*) AS total_negociacoes
FROM fato_pregao
GROUP BY data_pregao
ORDER BY total_negociacoes DESC
LIMIT 10;
╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝



