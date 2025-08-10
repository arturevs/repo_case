VW_PERFORMANCE_SQL = """
CREATE OR REPLACE VIEW vw_performance_relativa_mercado AS
WITH
-- PASSO 1: Mantém o mesmo, calcula o IDA do mês anterior para cada grupo.
calculo_variacao_individual AS (
    SELECT
        t.data_referencia,
        g.nome_grupo,
        f.valor AS ida_atual,
        LAG(f.valor, 1) OVER (PARTITION BY g.nome_grupo ORDER BY t.data_referencia) AS ida_anterior
    FROM
        fato_atendimento f
    JOIN dim_tempo t ON f.tempo_id = t.id_tempo
    JOIN dim_grupo_economico g ON f.grupo_economico_id = g.id_grupo
),

-- PASSO 2: Mantém o mesmo, calcula a taxa de variação individual e a média de mercado por mês.
calculo_taxa_individual_e_media_mercado AS (
    SELECT
        data_referencia,
        nome_grupo,
        (ida_atual - ida_anterior) / NULLIF(ida_anterior, 0) AS taxa_variacao_individual,
        AVG(ida_atual) OVER (PARTITION BY data_referencia) AS media_ida_mercado_atual
    FROM
        calculo_variacao_individual
),

-- PASSO 3 (REESTRUTURADO): Calcula a taxa de variação da média do mercado de forma separada e limpa.
variacao_media_mercado AS (
    SELECT
        data_referencia,
        (media_ida_mercado_atual - LAG(media_ida_mercado_atual, 1) OVER (ORDER BY data_referencia)) / NULLIF(LAG(media_ida_mercado_atual, 1) OVER (ORDER BY data_referencia), 0) AS taxa_variacao_media_mercado
    FROM
        (SELECT DISTINCT data_referencia, media_ida_mercado_atual FROM calculo_taxa_individual_e_media_mercado) AS medias_unicas_mensais
)

-- PASSO 4 (FINAL): Junta os dados de variação individual com os de variação de mercado e pivota.
SELECT
    TO_CHAR(ind.data_referencia, 'YYYY-MM') AS "Mes",
    MAX(merc.taxa_variacao_media_mercado * 100) AS "Taxa de Variação Média",
    MAX(CASE WHEN ind.nome_grupo = 'ALGAR' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "ALGAR",
    MAX(CASE WHEN ind.nome_grupo = 'CLARO' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "CLARO",
    MAX(CASE WHEN ind.nome_grupo = 'VIVO' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "VIVO",
    MAX(CASE WHEN ind.nome_grupo = 'OI' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "OI",
    MAX(CASE WHEN ind.nome_grupo = 'SKY' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "SKY",
    MAX(CASE WHEN ind.nome_grupo = 'TIM' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "TIM",
    MAX(CASE WHEN ind.nome_grupo = 'NEXTEL' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "NEXTEL",
    MAX(CASE WHEN ind.nome_grupo = 'SERCOMTEL' THEN (ind.taxa_variacao_individual - merc.taxa_variacao_media_mercado) * 100 ELSE NULL END) AS "SERCOMTEL"
FROM
    calculo_taxa_individual_e_media_mercado AS ind
JOIN
    variacao_media_mercado AS merc ON ind.data_referencia = merc.data_referencia
WHERE
    ind.taxa_variacao_individual IS NOT NULL AND merc.taxa_variacao_media_mercado IS NOT NULL
GROUP BY
    "Mes"
ORDER BY
    "Mes"
"""