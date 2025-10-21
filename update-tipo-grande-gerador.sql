-- Atualização mais específica por faixas de CNAE
UPDATE estabelecimentos_tratados 
SET tipo_grande_gerador = CASE 
    -- Agricultura, pecuária, produção florestal, pesca e aquicultura (01-03)
    WHEN cnae_fiscal_principal BETWEEN 01113 AND 03221 THEN 'Resíduos Sólidos Agrossilvipastoris (RSA)'
    
    -- Extração de mineral (05-09)
    WHEN cnae_fiscal_principal BETWEEN 5003 AND 9904 THEN 'Resíduos Sólidos dos Serviços de Mineração (RSM)'
    
    -- Indústrias de transformação (10-33)
    WHEN cnae_fiscal_principal BETWEEN 10112 AND 32990 THEN 'Resíduos Sólidos Industriais (RSI)'
    
    -- Eletricidade e gás, água, esgoto (35-39)
    WHEN cnae_fiscal_principal BETWEEN 35115 AND 39005 THEN 'Resíduos Sólidos dos Serviços de Água e Esgoto (RSAE)'
    
    -- Construção (41-43)
    WHEN cnae_fiscal_principal BETWEEN 41107 AND 43991 THEN 'Resíduos Sólidos da Construção Civil e Demolição (RCC)'
    
    -- Transporte, armazenagem e correio (49-53)
    WHEN cnae_fiscal_principal BETWEEN 49116 AND 53202 THEN 'Resíduos Sólidos dos Serviços de Transporte (RST)'
    
    -- Atividades de saúde (86-88)
    WHEN cnae_fiscal_principal BETWEEN 86101 AND 88006 THEN 'Resíduos Sólidos dos Serviços de Saúde (RSS)'
    
    -- Comércio, serviços, administração pública, educação, etc. (RSU)
    ELSE 'Resíduos Sólidos Urbanos (RSU)'
END;
