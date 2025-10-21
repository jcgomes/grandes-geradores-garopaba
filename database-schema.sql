create table cnae
(
    codigo    TEXT,
    descricao TEXT
);

create table empresa
(
    cnpj_basico                 VARCHAR(8)
        primary key,
    razao_social                TEXT not null,
    natureza_juridica           VARCHAR(10),
    qualificacao_responsavel    VARCHAR(10),
    capital_social              DECIMAL(15, 2),
    porte_empresa               VARCHAR(2),
    ente_federativo_responsavel TEXT,
    data_atualizacao            DATE
);

create table estabelecimento
(
    cnpj_basico                 VARCHAR(8),
    cnpj_ordem                  VARCHAR(4),
    cnpj_dv                     VARCHAR(2),
    identificador_matriz_filial VARCHAR(1),
    nome_fantasia               TEXT,
    situacao_cadastral          VARCHAR(2),
    data_situacao_cadastral     DATE,
    motivo_situacao_cadastral   VARCHAR(2),
    nome_cidade_exterior        TEXT,
    pais                        VARCHAR(10),
    data_inicio_atividade       DATE,
    cnae_fiscal_principal       VARCHAR(10),
    cnae_fiscal_secundaria      TEXT,
    tipo_logradouro             TEXT,
    logradouro                  TEXT,
    numero                      TEXT,
    complemento                 TEXT,
    bairro                      TEXT,
    cep                         VARCHAR(8),
    uf                          VARCHAR(2),
    municipio                   VARCHAR(10),
    ddd1                        VARCHAR(2),
    telefone1                   VARCHAR(9),
    ddd2                        VARCHAR(2),
    telefone2                   VARCHAR(9),
    ddd_fax                     VARCHAR(2),
    fax                         VARCHAR(9),
    email                       TEXT,
    situacao_especial           TEXT,
    data_situacao_especial      DATE,
    data_atualizacao            DATE,
    primary key (cnpj_basico, cnpj_ordem, cnpj_dv)
);

create table estabelecimentos_tratados
(
    cnpj_basico                 INTEGER,
    cnpj_ordem                  INTEGER,
    cnpj_dv                     INTEGER,
    identificador_matriz_filial INTEGER,
    nome_fantasia               TEXT,
    porte_empresa               REAL,
    tipo_grande_gerador         TEXT,
    id_consolidado              INTEGER,
    situacao_cadastral          INTEGER,
    data_situacao_cadastral     INTEGER,
    motivo_situacao_cadastral   INTEGER,
    nome_cidade_exterior        REAL,
    pais                        REAL,
    data_inicio_atividade       INTEGER,
    cnae_fiscal_principal       INTEGER,
    cnae_fiscal_secundaria      TEXT,
    tipo_logradouro             TEXT,
    logradouro                  TEXT,
    numero                      TEXT,
    complemento                 TEXT,
    bairro                      TEXT,
    cep                         INTEGER,
    uf                          TEXT,
    municipio                   INTEGER,
    ddd1                        REAL,
    telefone1                   TEXT,
    ddd2                        REAL,
    telefone2                   REAL,
    ddd_fax                     REAL,
    fax                         TEXT,
    email                       TEXT,
    situacao_especial           TEXT,
    data_situacao_especial      REAL,
    data_atualizacao            TEXT,
    coordenada_wkt              TEXT
);

create table motivo_situacao
(
    codigo    TEXT,
    descricao TEXT
);

create table municipio
(
    codigo    TEXT,
    descricao TEXT
);

create table natureza_juridica
(
    codigo    TEXT,
    descricao TEXT
);

create table pais
(
    codigo    TEXT,
    descricao TEXT
);

create table qualificacao_socio
(
    codigo    TEXT,
    descricao TEXT
);

create table socio
(
    cnpj_basico                      VARCHAR(8),
    identificador_socio              VARCHAR(1),
    nome_socio_razao_social          TEXT not null,
    cpf_cnpj_socio                   VARCHAR(14),
    qualificacao_socio               VARCHAR(10),
    data_entrada_sociedade           DATE,
    pais                             VARCHAR(10),
    representante_legal              VARCHAR(11),
    nome_representante_legal         TEXT,
    qualificacao_representante_legal VARCHAR(10),
    faixa_etaria                     VARCHAR(1),
    data_atualizacao                 DATE
);

create table sqlite_master
(
    type     TEXT,
    name     TEXT,
    tbl_name TEXT,
    rootpage INT,
    sql      TEXT
);

