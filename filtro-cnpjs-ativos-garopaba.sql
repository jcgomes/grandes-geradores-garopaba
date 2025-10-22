SELECT
    -- Seleção das colunas da tabela estabelecimento (e.*)
    e.cnpj_basico,
    e.cnpj_ordem,
    e.cnpj_dv,
    e.identificador_matriz_filial,
    -- Aplicação da lógica de preenchimento condicional para nome_fantasia
    COALESCE(e.nome_fantasia, emp.razao_social) AS nome_fantasia,
    e.situacao_cadastral,
    e.data_situacao_cadastral,
    e.motivo_situacao_cadastral,
    e.nome_cidade_exterior,
    e.pais,
    e.data_inicio_atividade,
    e.cnae_fiscal_principal,
    e.cnae_fiscal_secundaria,
    e.tipo_logradouro,
    e.logradouro,
    e.numero,
    e.complemento,
    e.bairro,
    e.cep,
    e.uf,
    e.municipio,
    e.ddd1,
    e.telefone1,
    e.ddd2,
    e.telefone2,
    e.ddd_fax,
    e.fax,
    e.email,
    e.situacao_especial,
    e.data_situacao_especial,
    e.data_atualizacao,

    -- Coluna da tabela empresa adicionada
    emp.porte_empresa,

    -- Lógica de Classificação do Tipo de Grande Gerador
    CASE
        -- Resíduos Sólidos Agrossilvipastoris (RSA) - CNAEs 01-03
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) IN ('01', '02', '03')
        THEN 'Resíduos Sólidos Agrossilvipastoris (RSA)'

        -- Resíduos Sólidos Industriais (RSI) - CNAEs 10-33
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) BETWEEN '10' AND '33'
        THEN 'Resíduos Sólidos Industriais (RSI)'

        -- Resíduos Sólidos da Construção Civil (RCC) - CNAEs 41-43
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) BETWEEN '41' AND '43'
        THEN 'Resíduos Sólidos da Construção Civil e Demolição (RCC)'

        -- Resíduos Sólidos dos Serviços de Saúde (RSS) - CNAE 86
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) = '86'
        THEN 'Resíduos Sólidos dos Serviços de Saúde (RSS)'

        -- Resíduos Sólidos dos Serviços de Água e Esgoto (RSAE) - CNAEs 36, 37
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) IN ('36', '37')
        THEN 'Resíduos Sólidos dos Serviços de Água e Esgoto (RSAE)'

        -- Resíduos Sólidos dos Serviços de Transporte (RST) - CNAEs 49-53
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) BETWEEN '49' AND '53'
        THEN 'Resíduos Sólidos dos Serviços de Transporte (RST)'

        -- Resíduos Sólidos dos Serviços de Mineração (RSM) - CNAEs 05-09
        WHEN SUBSTR(e.cnae_fiscal_principal, 1, 2) BETWEEN '05' AND '09'
        THEN 'Resíduos Sólidos dos Serviços de Mineração (RSM)'

        -- Resíduos Sólidos Urbanos (RSU) - Demais CNAEs (comércio, serviços, etc.)
        ELSE 'Resíduos Sólidos Urbanos (RSU)'
    END AS tipo_grande_gerador

FROM
    estabelecimento e
LEFT JOIN
    empresa emp ON e.cnpj_basico = emp.cnpj_basico
WHERE
    -- 8113 é o código do município de garopaba na tabela município
    -- 02 é a situação cadastral: ATIVA
    e.municipio = '8113'
    AND e.situacao_cadastral = '02'

    -- OU inclui os registros que NÃO possuem nome_fantasia (é NULL),
    -- MAS conseguem ser preenchidos com a razao_social da tabela empresa (que não é NULL)
    AND (
        e.nome_fantasia IS NOT NULL
        OR
        (e.nome_fantasia IS NULL AND emp.razao_social IS NOT NULL)
    )

ORDER BY
    e.cnae_fiscal_principal
