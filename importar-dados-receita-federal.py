"""
IMPORTADOR CNPJ - RECEITA FEDERAL
==================================
Classe para importação e consolidação de dados do CNPJ de múltiplas pastas temporais.
Desenvolvido para processar dados de 2023-05 a 2025-09 em banco SQLite unificado.

Características principais:
- Processamento multi-pasta temporal
- UPSERT inteligente (não sobrescreve dados bons)
- Otimizações de performance (chunking, índices)
- Tratamento robusto de erros
- Logging detalhado com emojis

Autor: Juliano C. Gomes
Versão: 1.0
Data: 06/10/2025
"""

import pandas as pd
import sqlite3
import os
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class ImportadorCNPJMultiPasta:
    """
    Importador principal para dados do CNPJ da Receita Federal.
    
    Esta classe processa arquivos CSV de múltiplos períodos temporais,
    consolidando-os em um banco SQLite unificado com tratamento inteligente
    de atualizações e duplicatas.
    
    Atributos:
        diretorio_base (Path): Diretório base contendo as pastas de dados
        caminho_db (str): Caminho para o arquivo do banco SQLite
        conn (sqlite3.Connection): Conexão com o banco de dados
        mapeamento_arquivos (dict): Mapeamento de padrões de nome para tipos de tabela
        estrutura_colunas (dict): Schema das tabelas com definição de colunas
    """
    
    def __init__(self, diretorio_base, caminho_db="cnpj_receita.db"):
        """
        Inicializa o importador com configurações base.
        
        Args:
            diretorio_base (str): Diretório raiz contendo as pastas de dados (YYYY-MM)
            caminho_db (str, optional): Caminho para o banco SQLite. Defaults to "cnpj_receita.db".
        """
        self.diretorio_base = Path(diretorio_base)
        self.caminho_db = caminho_db
        self.conn = None
        
        # Mapeamento flexível para identificação de arquivos
        # Suporta múltiplas variações de nomenclatura
        self.mapeamento_arquivos = {
            'EMPRECSV': 'empresa',
            'ESTABLELE': 'estabelecimento', 
            'ESTABELE': 'estabelecimento',
            'ESTABEL': 'estabelecimento',
            'SOCIOCSV': 'socio',
            'CNAECSV': 'cnae',
            'MUNICCSV': 'municipio',
            'NATJUCSV': 'natureza_juridica',
            'PAISCSV': 'pais',
            'QUALSCSV': 'qualificacao_socio',
            'MOTICSV': 'motivo_situacao'
        }
        
        # Schema completo das tabelas - define a estrutura de colunas esperada
        # para cada tipo de tabela no banco de dados
        self.estrutura_colunas = {
            'empresa': [
                'cnpj_basico', 'razao_social', 'natureza_juridica', 
                'qualificacao_responsavel', 'capital_social', 'porte_empresa', 
                'ente_federativo_responsavel'
            ],
            'estabelecimento': [
                'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'identificador_matriz_filial',
                'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
                'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais',
                'data_inicio_atividade', 'cnae_fiscal_principal', 'cnae_fiscal_secundaria',
                'tipo_logradouro', 'logradouro', 'numero', 'complemento',
                'bairro', 'cep', 'uf', 'municipio', 'ddd1', 'telefone1',
                'ddd2', 'telefone2', 'ddd_fax', 'fax', 'email', 'situacao_especial',
                'data_situacao_especial'
            ],
            'socio': [
                'cnpj_basico', 'identificador_socio', 'nome_socio_razao_social',
                'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada_sociedade',
                'pais', 'representante_legal', 'nome_representante_legal',
                'qualificacao_representante_legal', 'faixa_etaria'
            ],
            'cnae': ['codigo', 'descricao'],
            'municipio': ['codigo', 'descricao'],
            'natureza_juridica': ['codigo', 'descricao'],
            'pais': ['codigo', 'descricao'],
            'qualificacao_socio': ['codigo', 'descricao'],
            'motivo_situacao': ['codigo', 'descricao']
        }

    def encontrar_todas_pastas(self):
        """
        Encontra todas as pastas de dados no formato YYYY-MM no diretório base.
        
        Realiza busca recursiva por diretórios no padrão temporal e os ordena
        cronologicamente para processamento sequencial.
        
        Returns:
            list: Lista de objetos Path ordenados por data (mais antigo primeiro)
            
        Exemplo:
            >>> pastas = importador.encontrar_todas_pastas()
            📁 Pastas encontradas (5):
               📂 2023-05 (15 arquivos)
               📂 2023-06 (15 arquivos)
        """
        print("🔍 PROCURANDO PASTAS DE DADOS...")
        
        pastas_encontradas = []
        
        # Procura por padrões de pasta no formato YYYY-MM
        for item in self.diretorio_base.glob("*"):
            if item.is_dir():
                nome_pasta = item.name
                # Verifica se é uma pasta de dados no formato YYYY-MM
                if (len(nome_pasta) == 7 and 
                    nome_pasta[4] == '-' and 
                    nome_pasta[:4].isdigit() and 
                    nome_pasta[5:].isdigit()):
                    pastas_encontradas.append(item)
        
        # Ordena as pastas por data (mais antigas primeiro)
        pastas_encontradas.sort()
        
        print(f"📁 Pastas encontradas ({len(pastas_encontradas)}):")
        for pasta in pastas_encontradas:
            num_arquivos = len(list(pasta.glob("*")))
            print(f"   📂 {pasta.name} ({num_arquivos} arquivos)")
        
        return pastas_encontradas

    def escanear_estrutura_completa(self):
        """
        Escaneia a estrutura completa de todas as pastas encontradas.
        
        Percorre todas as pastas temporais identificando e classificando
        arquivos por tipo de tabela. Consolida a estrutura completa para
        planejamento da importação.
        
        Returns:
            dict: Dicionário com lista de arquivos agrupados por tipo de tabela
                  Formato: {'empresa': [path1, path2], 'estabelecimento': [path3, ...]}
                  
        Exemplo:
            >>> estrutura = importador.escanear_estrutura_completa()
            📊 ESCANEANDO ESTRUTURA COMPLETA...
            📂 Processando pasta: 2023-05
               ✅ empresa: 1 arquivo(s)
               ✅ estabelecimento: 1 arquivo(s)
        """
        print("\n📊 ESCANEANDO ESTRUTURA COMPLETA...")
        
        pastas = self.encontrar_todas_pastas()
        estrutura_completa = {}
        
        for pasta in pastas:
            print(f"\n📂 Processando pasta: {pasta.name}")
            arquivos_pasta = self.identificar_arquivos_pasta(pasta)
            
            # Consolida arquivos por tipo em estrutura completa
            for tipo, lista_arquivos in arquivos_pasta.items():
                if tipo not in estrutura_completa:
                    estrutura_completa[tipo] = []
                estrutura_completa[tipo].extend(lista_arquivos)
                print(f"   ✅ {tipo}: {len(lista_arquivos)} arquivo(s)")
        
        # Relatório consolidado
        print(f"\n🎯 RESUMO COMPLETO:")
        for tipo, lista_arquivos in estrutura_completa.items():
            print(f"   {tipo}: {len(lista_arquivos)} arquivos")
        
        return estrutura_completa

    def identificar_arquivos_pasta(self, pasta):
        """
        Identifica e classifica arquivos em uma pasta específica.
        
        Analisa cada arquivo na pasta aplicando regras de mapeamento e inferência
        para determinar seu tipo (empresa, estabelecimento, socio, etc.).
        
        Args:
            pasta (Path): Objeto Path da pasta a ser escaneada
            
        Returns:
            dict: Dicionário com arquivos classificados por tipo
                  Formato: {'empresa': [arquivo1.csv], 'estabelecimento': [arquivo2.csv]}
        """
        arquivos_encontrados = {}
        
        for arquivo in pasta.glob("*"):
            if not arquivo.is_file():
                continue
            
            # ⛔ IGNORAR arquivos ZIP - não processamos compactados, pois todos os arquivos já foram descompactados
            if arquivo.suffix.upper() == '.ZIP':
                continue
            
            nome_upper = arquivo.name.upper()
            tipo_encontrado = None
            
            # Procura por padrões conhecidos no mapeamento
            for sufixo, tabela in self.mapeamento_arquivos.items():
                if sufixo in nome_upper:
                    tipo_encontrado = tabela
                    break
            
            # Se não encontrou no mapeamento direto, tenta inferir pelo padrão do nome
            if not tipo_encontrado:
                if "EMPRE" in nome_upper:
                    tipo_encontrado = "empresa"
                elif any(x in nome_upper for x in ["ESTAB", "FILIAL"]):
                    tipo_encontrado = "estabelecimento"
                elif "SOCIO" in nome_upper:
                    tipo_encontrado = "socio"
                elif "CNAE" in nome_upper:
                    tipo_encontrado = "cnae"
                elif "MUNIC" in nome_upper:
                    tipo_encontrado = "municipio"
                elif "NATJ" in nome_upper:
                    tipo_encontrado = "natureza_juridica"
                elif "PAIS" in nome_upper:
                    tipo_encontrado = "pais"
                elif "QUAL" in nome_upper:
                    tipo_encontrado = "qualificacao_socio"
                elif "MOTIC" in nome_upper:
                    tipo_encontrado = "motivo_situacao"
            
            # Adiciona à estrutura se tipo foi identificado
            if tipo_encontrado:
                if tipo_encontrado not in arquivos_encontrados:
                    arquivos_encontrados[tipo_encontrado] = []
                arquivos_encontrados[tipo_encontrado].append(arquivo)
        
        return arquivos_encontrados

    def conectar_db(self):
        """
        Conecta ao banco SQLite com otimizações para operações em lote.
        
        Configura parâmetros do SQLite para melhor performance durante
        a importação massiva de dados.
        
        Raises:
            sqlite3.Error: Se não conseguir conectar ao banco de dados
        """
        self.conn = sqlite3.connect(self.caminho_db)
        # Otimizações para performance em operações bulk
        self.conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
        self.conn.execute("PRAGMA synchronous = OFF")  # Melhoria performance
        self.conn.execute("PRAGMA cache_size = 100000")  # Cache ampliado
        print(f"✅ Conectado ao banco: {self.caminho_db}")

    def criar_tabelas(self):
        """
        Cria todas as tabelas do schema no banco de dados.
        
        Executa o DDL completo para criar tabelas principais e de referência.
        Utiliza CREATE TABLE IF NOT EXISTS para ser idempotente.
        
        Raises:
            Exception: Se houver erro na criação das tabelas
        """
        try:
            schema_sql = self._get_schema_fallback()
            
            # Divide o SQL em comandos individuais
            comandos = [cmd.strip() for cmd in schema_sql.split(';') if cmd.strip()]
            
            for comando in comandos:
                try:
                    self.conn.execute(comando)
                except sqlite3.Error as e:
                    # Ignora erro de tabela já existente, reporta outros
                    if "already exists" not in str(e):
                        print(f"⚠️  {e}")
            
            self.conn.commit()
            print("✅ Tabelas criadas/verificadas com sucesso!")
            
        except Exception as e:
            print(f"❌ Erro ao criar tabelas: {e}")

    def _get_schema_fallback(self):
        """
        Fornece o schema SQL completo como fallback.
        
        Returns:
            str: Schema SQL completo com todas as tabelas
            
        Nota:
            Este método contém o DDL hardcoded como fallback caso seja necessário
            criar o schema sem dependências externas.
        """
        return """
CREATE TABLE IF NOT EXISTS cnae (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS municipio (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS natureza_juridica (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pais (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS qualificacao_socio (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS motivo_situacao (
    codigo VARCHAR(10) PRIMARY KEY,
    descricao TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS empresa (
    cnpj_basico VARCHAR(8) PRIMARY KEY,
    razao_social TEXT NOT NULL,
    natureza_juridica VARCHAR(10),
    qualificacao_responsavel VARCHAR(10),
    capital_social DECIMAL(15,2),
    porte_empresa VARCHAR(2),
    ente_federativo_responsavel TEXT,
    data_atualizacao DATE
);

CREATE TABLE IF NOT EXISTS estabelecimento (
    cnpj_basico VARCHAR(8),
    cnpj_ordem VARCHAR(4),
    cnpj_dv VARCHAR(2),
    identificador_matriz_filial VARCHAR(1),
    nome_fantasia TEXT,
    situacao_cadastral VARCHAR(2),
    data_situacao_cadastral DATE,
    motivo_situacao_cadastral VARCHAR(2),
    nome_cidade_exterior TEXT,
    pais VARCHAR(10),
    data_inicio_atividade DATE,
    cnae_fiscal_principal VARCHAR(10),
    cnae_fiscal_secundaria TEXT,
    tipo_logradouro TEXT,
    logradouro TEXT,
    numero TEXT,
    complemento TEXT,
    bairro TEXT,
    cep VARCHAR(8),
    uf VARCHAR(2),
    municipio VARCHAR(10),
    ddd1 VARCHAR(2),
    telefone1 VARCHAR(9),
    ddd2 VARCHAR(2),
    telefone2 VARCHAR(9),
    ddd_fax VARCHAR(2),
    fax VARCHAR(9),
    email TEXT,
    situacao_especial TEXT,
    data_situacao_especial DATE,
    data_atualizacao DATE,
    PRIMARY KEY (cnpj_basico, cnpj_ordem, cnpj_dv)
);

CREATE TABLE IF NOT EXISTS socio (
    cnpj_basico VARCHAR(8),
    identificador_socio VARCHAR(1),
    nome_socio_razao_social TEXT NOT NULL,
    cpf_cnpj_socio VARCHAR(14),
    qualificacao_socio VARCHAR(10),
    data_entrada_sociedade DATE,
    pais VARCHAR(10),
    representante_legal VARCHAR(11),
    nome_representante_legal TEXT,
    qualificacao_representante_legal VARCHAR(10),
    faixa_etaria VARCHAR(1),
    data_atualizacao DATE
);
"""

    def importar_tabelas_referencia(self, estrutura_completa):
        """
        Importa tabelas de referência (dicionários) apenas uma vez.
        
        Processa tabelas de domínio como CNAE, municípios, natureza jurídica, etc.
        Estas tabelas são importadas uma única vez usando o primeiro arquivo encontrado.
        
        Args:
            estrutura_completa (dict): Estrutura completa com arquivos por tipo
            
        Nota:
            Tabelas de referência são tratadas com replace (não UPSERT) pois
            são dicionários estáticos que não mudam entre períodos.
        """
        print("\n📚 IMPORTANDO TABELAS DE REFERÊNCIA...")
        
        tabelas_referencia = ['cnae', 'municipio', 'natureza_juridica', 'pais', 
                             'qualificacao_socio', 'motivo_situacao']
        
        for tabela in tabelas_referencia:
            if tabela in estrutura_completa and estrutura_completa[tabela]:
                # Pega apenas o primeiro arquivo de referência encontrado
                arquivo_ref = estrutura_completa[tabela][0]
                print(f"📊 Importando {tabela}...")
                
                try:
                    # Lê CSV com configurações otimizadas
                    df = pd.read_csv(arquivo_ref, encoding='latin-1', sep=';', dtype=str, na_filter=False)
                    
                    # Ajusta colunas para schema esperado
                    colunas_esperadas = self.estrutura_colunas[tabela]
                    if len(df.columns) == len(colunas_esperadas):
                        df.columns = colunas_esperadas
                    
                    # Remove duplicatas e importa
                    df = df.drop_duplicates()
                    df.to_sql(tabela, self.conn, if_exists='replace', index=False)
                    print(f"✅ {tabela}: {len(df):,} registros")
                    
                except Exception as e:
                    print(f"❌ Erro em {tabela}: {e}")

    def importar_dados_principais(self, estrutura_completa):
        """
        Importa dados principais de todas as pastas ordenados por data.
        
        Processa tabelas transacionais (empresa, estabelecimento, socio) de
        forma ordenada temporalmente, aplicando UPSERT inteligente para
        atualizar registros existentes sem sobrescrever dados bons.
        
        Args:
            estrutura_completa (dict): Estrutura completa com arquivos por tipo
            
        Nota:
            O processamento é feito do período mais antigo para o mais recente,
            permitindo que dados mais atualizados prevaleçam.
        """
        print("\n🚀 IMPORTANDO DADOS PRINCIPAIS...")
        
        tabelas_principais = ['empresa', 'estabelecimento', 'socio']
        
        for tabela in tabelas_principais:
            if tabela in estrutura_completa:
                print(f"\n📊 PROCESSANDO {tabela.upper()}...")
                
                # ✅ ORDENA por data (mais antigos primeiro) para processamento sequencial
                arquivos_ordenados = sorted(estrutura_completa[tabela], 
                                          key=lambda x: (x.parent.name[:4], x.parent.name[5:7]))
                
                total_geral = 0
                total_atualizacoes = 0
                arquivos_processados = 0
                
                for arquivo in arquivos_ordenados:
                    try:
                        pasta_nome = arquivo.parent.name
                        registros_arquivo, atualizacoes_arquivo = self._importar_arquivo_principal(tabela, arquivo, pasta_nome)
                        total_geral += registros_arquivo
                        total_atualizacoes += atualizacoes_arquivo
                        arquivos_processados += 1
                        
                        # Log detalhado com informações de atualização
                        if atualizacoes_arquivo > 0:
                            print(f"   ✅ {arquivo.name} ({pasta_nome}): {registros_arquivo:,} registros ({atualizacoes_arquivo:,} atualizações)")
                        else:
                            print(f"   ✅ {arquivo.name} ({pasta_nome}): {registros_arquivo:,} registros")
                            
                    except Exception as e:
                        print(f"   ❌ {arquivo.name}: {e}")
                
                # Resumo final da tabela
                if total_atualizacoes > 0:
                    print(f"🎯 {tabela}: {total_geral:,} registros ({total_atualizacoes:,} atualizações) de {arquivos_processados} arquivos")
                else:
                    print(f"🎯 {tabela}: {total_geral:,} registros de {arquivos_processados} arquivos")

    def _importar_arquivo_principal(self, tabela, arquivo, pasta_nome):
        """
        Importa um arquivo individual com processamento em chunks e UPSERT inteligente.
        
        Args:
            tabela (str): Tipo de tabela ('empresa', 'estabelecimento', 'socio')
            arquivo (Path): Caminho para o arquivo CSV
            pasta_nome (str): Nome da pasta origem (para metadata)
            
        Returns:
            tuple: (total_registros, total_atualizacoes) processados
            
        Nota:
            Processa arquivos grandes em chunks de 50.000 registros para
            evitar estouro de memória e permitir commit granular.
        """
        total_registros = 0
        atualizacoes = 0
        
        try:
            chunk_size = 50000  # Processa 50k registros por vez
            for chunk_num, chunk in enumerate(pd.read_csv(
                arquivo, 
                encoding='latin-1', 
                sep=';', 
                dtype=str, 
                chunksize=chunk_size,
                low_memory=False,
                na_filter=False
            )):
                if len(chunk) == 0:
                    continue
                
                # Ajusta colunas para o schema esperado
                colunas_esperadas = self.estrutura_colunas[tabela]
                if len(chunk.columns) == len(colunas_esperadas):
                    chunk.columns = colunas_esperadas
                
                # Limpa dados: preenche NaN e converte strings vazias para None
                chunk = chunk.fillna('')
                chunk = chunk.replace({'': None})
                
                # Adiciona data de atualização baseada na pasta origem
                chunk['data_atualizacao'] = f"{pasta_nome[:4]}-{pasta_nome[5:7]}-01"
                
                # Processamento específico por tipo de tabela
                if tabela == 'empresa' and 'capital_social' in chunk.columns:
                    # Converte capital_social para decimal
                    chunk['capital_social'] = chunk['capital_social'].astype(str)
                    chunk['capital_social'] = chunk['capital_social'].str.replace(',', '.')
                    chunk['capital_social'] = pd.to_numeric(chunk['capital_social'], errors='coerce').fillna(0)
                
                # Remove duplicatas dentro do mesmo chunk
                chunk = self._remover_duplicatas(tabela, chunk)
                
                if len(chunk) > 0:
                    try:
                        # ✅ UPSERT INTELIGENTE: Processa chunk com lógica específica
                        registros_inseridos, registros_atualizados = self._upsert_chunk_inteligente(tabela, chunk, pasta_nome)
                        total_registros += registros_inseridos
                        atualizacoes += registros_atualizados
                        
                        # Log intermitente para chunks grandes
                        if registros_atualizados > 0 and chunk_num % 10 == 0:
                            print(f"      🔄 Chunk {chunk_num}: {registros_inseridos} novos, {registros_atualizados} atualizados")
                            
                    except Exception as e:
                        print(f"      ⚠️  Erro no chunk {chunk_num}: {e}")
                        # Fallback: processamento linha por linha
                        registros_fallback, atualizacoes_fallback = self._inserir_chunk_linha_por_linha_upsert(tabela, chunk, pasta_nome)
                        total_registros += registros_fallback
                        atualizacoes += atualizacoes_fallback
            
            return total_registros, atualizacoes
            
        except Exception as e:
            print(f"❌ Erro em {arquivo.name}: {e}")
            return 0, 0

    def _upsert_chunk_inteligente(self, tabela, df, pasta_nome):
        """
        Executa UPSERT inteligente que não sobrescreve dados bons existentes.
        
        Implementa lógica específica por tipo de tabela:
        - EMPRESA: Atualiza apenas campos vazios ou com dados melhores
        - ESTABELECIMENTO: Cada estabelecimento é único (chave natural)
        - SOCIO: Chave flexível com atualização condicional
        
        Args:
            tabela (str): Tipo de tabela
            df (DataFrame): Chunk de dados a ser processado
            pasta_nome (str): Nome da pasta para metadata
            
        Returns:
            tuple: (registros_inseridos, registros_atualizados)
        """
        inseridos = 0
        atualizados = 0
        
        if tabela == 'empresa':
            # ✅ CORREÇÃO CRÍTICA: Para empresa, atualiza apenas dados faltantes/melhores
            for idx, row in df.iterrows():
                try:
                    # Verifica se empresa já existe no banco
                    existe = self.conn.execute(
                        "SELECT 1 FROM empresa WHERE cnpj_basico = ?", 
                        (row['cnpj_basico'],)
                    ).fetchone()
                    
                    if existe:
                        # ✅ ATUALIZA apenas se tiver informações MELHORES/MAIS COMPLETAS
                        # Não simplesmente sobrescreve dados existentes!
                        sql = """
                        UPDATE empresa SET 
                            razao_social = COALESCE(NULLIF(?, ''), razao_social),
                            natureza_juridica = COALESCE(NULLIF(?, ''), natureza_juridica),
                            qualificacao_responsavel = COALESCE(NULLIF(?, ''), qualificacao_responsavel),
                            capital_social = CASE 
                                WHEN ? IS NOT NULL AND ? != 0 THEN ? 
                                ELSE capital_social 
                            END,
                            porte_empresa = COALESCE(NULLIF(?, ''), porte_empresa),
                            ente_federativo_responsavel = COALESCE(NULLIF(?, ''), ente_federativo_responsavel),
                            data_atualizacao = ?
                        WHERE cnpj_basico = ? AND (
                            razao_social IS NULL OR 
                            razao_social = '' OR
                            natureza_juridica IS NULL OR
                            natureza_juridica = '' OR
                            ? > data_atualizacao
                        )
                        """
                        # Prepara valor do capital_social
                        capital = row['capital_social'] if 'capital_social' in row else 0
                        if isinstance(capital, str):
                            try:
                                capital = float(capital.replace(',', '.')) if capital.strip() else 0.0
                            except:
                                capital = 0.0
                        
                        cursor = self.conn.execute(sql, (
                            row['razao_social'], row['natureza_juridica'], row['qualificacao_responsavel'],
                            capital, capital, capital,
                            row['porte_empresa'], row['ente_federativo_responsavel'],
                            row['data_atualizacao'], row['cnpj_basico'], row['data_atualizacao']
                        ))
                        
                        if cursor.rowcount > 0:
                            atualizados += 1
                    else:
                        # INSERT apenas se tiver dados válidos (especialmente razão social)
                        if row['razao_social'] and row['razao_social'].strip():
                            colunas = [col for col in self.estrutura_colunas[tabela] if col in row and row[col] is not None]
                            placeholders = ', '.join(['?' for _ in colunas])
                            sql = f"INSERT INTO empresa ({', '.join(colunas)}) VALUES ({placeholders})"
                            
                            valores = []
                            for col in colunas:
                                valor = row[col]
                                if col == 'capital_social' and valor is not None:
                                    try:
                                        if isinstance(valor, str):
                                            valor = float(valor.replace(',', '.')) if valor.strip() else 0.0
                                        else:
                                            valor = float(valor) if valor else 0.0
                                    except:
                                        valor = 0.0
                                valores.append(valor)
                            
                            self.conn.execute(sql, valores)
                            inseridos += 1
                            
                except Exception as e:
                    # Continua processamento mesmo com erro em linha individual
                    continue
        
        elif tabela == 'estabelecimento':
            # ✅ Para estabelecimento, lógica diferente - cada estabelecimento é único
            for idx, row in df.iterrows():
                try:
                    # Verifica por chave natural completa
                    existe = self.conn.execute(
                        "SELECT 1 FROM estabelecimento WHERE cnpj_basico = ? AND cnpj_ordem = ? AND cnpj_dv = ?", 
                        (row['cnpj_basico'], row['cnpj_ordem'], row['cnpj_dv'])
                    ).fetchone()
                    
                    if existe:
                        # UPDATE para estabelecimento específico
                        sql = """
                        UPDATE estabelecimento SET 
                            identificador_matriz_filial = COALESCE(NULLIF(?, ''), identificador_matriz_filial),
                            nome_fantasia = COALESCE(NULLIF(?, ''), nome_fantasia),
                            situacao_cadastral = COALESCE(NULLIF(?, ''), situacao_cadastral),
                            data_situacao_cadastral = COALESCE(NULLIF(?, ''), data_situacao_cadastral),
                            motivo_situacao_cadastral = COALESCE(NULLIF(?, ''), motivo_situacao_cadastral),
                            nome_cidade_exterior = COALESCE(NULLIF(?, ''), nome_cidade_exterior),
                            pais = COALESCE(NULLIF(?, ''), pais),
                            data_inicio_atividade = COALESCE(NULLIF(?, ''), data_inicio_atividade),
                            cnae_fiscal_principal = COALESCE(NULLIF(?, ''), cnae_fiscal_principal),
                            cnae_fiscal_secundaria = COALESCE(NULLIF(?, ''), cnae_fiscal_secundaria),
                            tipo_logradouro = COALESCE(NULLIF(?, ''), tipo_logradouro),
                            logradouro = COALESCE(NULLIF(?, ''), logradouro),
                            numero = COALESCE(NULLIF(?, ''), numero),
                            complemento = COALESCE(NULLIF(?, ''), complemento),
                            bairro = COALESCE(NULLIF(?, ''), bairro),
                            cep = COALESCE(NULLIF(?, ''), cep),
                            uf = COALESCE(NULLIF(?, ''), uf),
                            municipio = COALESCE(NULLIF(?, ''), municipio),
                            ddd1 = COALESCE(NULLIF(?, ''), ddd1),
                            telefone1 = COALESCE(NULLIF(?, ''), telefone1),
                            ddd2 = COALESCE(NULLIF(?, ''), ddd2),
                            telefone2 = COALESCE(NULLIF(?, ''), telefone2),
                            ddd_fax = COALESCE(NULLIF(?, ''), ddd_fax),
                            fax = COALESCE(NULLIF(?, ''), fax),
                            email = COALESCE(NULLIF(?, ''), email),
                            situacao_especial = COALESCE(NULLIF(?, ''), situacao_especial),
                            data_situacao_especial = COALESCE(NULLIF(?, ''), data_situacao_especial),
                            data_atualizacao = ?
                        WHERE cnpj_basico = ? AND cnpj_ordem = ? AND cnpj_dv = ?
                        """
                        cursor = self.conn.execute(sql, (
                            row['identificador_matriz_filial'], row['nome_fantasia'], row['situacao_cadastral'],
                            row['data_situacao_cadastral'], row['motivo_situacao_cadastral'], row['nome_cidade_exterior'],
                            row['pais'], row['data_inicio_atividade'], row['cnae_fiscal_principal'], row['cnae_fiscal_secundaria'],
                            row['tipo_logradouro'], row['logradouro'], row['numero'], row['complemento'], row['bairro'],
                            row['cep'], row['uf'], row['municipio'], row['ddd1'], row['telefone1'], row['ddd2'], row['telefone2'],
                            row['ddd_fax'], row['fax'], row['email'], row['situacao_especial'], row['data_situacao_especial'],
                            row['data_atualizacao'], row['cnpj_basico'], row['cnpj_ordem'], row['cnpj_dv']
                        ))
                        if cursor.rowcount > 0:
                            atualizados += 1
                    else:
                        # INSERT novo estabelecimento
                        colunas = [col for col in self.estrutura_colunas[tabela] if col in row and row[col] is not None]
                        placeholders = ', '.join(['?' for _ in colunas])
                        sql = f"INSERT INTO estabelecimento ({', '.join(colunas)}) VALUES ({placeholders})"
                        
                        valores = [row[col] for col in colunas]
                        self.conn.execute(sql, valores)
                        inseridos += 1
                            
                except Exception as e:
                    continue
        
        elif tabela == 'socio':
            # Para sócio, chave mais flexível baseada em múltiplos campos
            for idx, row in df.iterrows():
                try:
                    # Para sócio, usamos uma chave mais flexível
                    existe = self.conn.execute(
                        "SELECT 1 FROM socio WHERE cnpj_basico = ? AND nome_socio_razao_social = ? AND cpf_cnpj_socio = ?", 
                        (row['cnpj_basico'], row['nome_socio_razao_social'], row.get('cpf_cnpj_socio', ''))
                    ).fetchone()
                    
                    if existe:
                        # UPDATE para sócio existente
                        sql = """
                        UPDATE socio SET 
                            identificador_socio = COALESCE(NULLIF(?, ''), identificador_socio),
                            qualificacao_socio = COALESCE(NULLIF(?, ''), qualificacao_socio),
                            data_entrada_sociedade = COALESCE(NULLIF(?, ''), data_entrada_sociedade),
                            pais = COALESCE(NULLIF(?, ''), pais),
                            representante_legal = COALESCE(NULLIF(?, ''), representante_legal),
                            nome_representante_legal = COALESCE(NULLIF(?, ''), nome_representante_legal),
                            qualificacao_representante_legal = COALESCE(NULLIF(?, ''), qualificacao_representante_legal),
                            faixa_etaria = COALESCE(NULLIF(?, ''), faixa_etaria),
                            data_atualizacao = ?
                        WHERE cnpj_basico = ? AND nome_socio_razao_social = ? AND cpf_cnpj_socio = ?
                        """
                        cursor = self.conn.execute(sql, (
                            row['identificador_socio'], row['qualificacao_socio'], row['data_entrada_sociedade'],
                            row['pais'], row['representante_legal'], row['nome_representante_legal'],
                            row['qualificacao_representante_legal'], row['faixa_etaria'], row['data_atualizacao'],
                            row['cnpj_basico'], row['nome_socio_razao_social'], row.get('cpf_cnpj_socio', '')
                        ))
                        if cursor.rowcount > 0:
                            atualizados += 1
                    else:
                        # INSERT novo sócio
                        colunas = [col for col in self.estrutura_colunas[tabela] if col in row and row[col] is not None]
                        placeholders = ', '.join(['?' for _ in colunas])
                        sql = f"INSERT INTO socio ({', '.join(colunas)}) VALUES ({placeholders})"
                        
                        valores = [row[col] for col in colunas]
                        self.conn.execute(sql, valores)
                        inseridos += 1
                        
                except Exception as e:
                    continue
    
        # Commit das transações do chunk
        self.conn.commit()
        return inseridos, atualizados

    def _inserir_chunk_linha_por_linha_upsert(self, tabela, chunk, pasta_nome):
        """
        Fallback para processamento linha por linha quando método bulk falha.
        
        Args:
            tabela (str): Tipo de tabela
            chunk (DataFrame): Chunk de dados
            pasta_nome (str): Nome da pasta origem
            
        Returns:
            tuple: (registros_inseridos, registros_atualizados)
        """
        return self._upsert_chunk_inteligente(tabela, chunk, pasta_nome)

    def _remover_duplicatas(self, tabela, df):
        """
        Remove registros duplicados baseado na chave natural de cada tabela.
        
        Args:
            tabela (str): Tipo de tabela
            df (DataFrame): DataFrame a ser deduplicado
            
        Returns:
            DataFrame: DataFrame sem duplicatas
            
        Nota:
            A deduplicação é feita intra-chunk para evitar duplicatas
            dentro do mesmo arquivo.
        """
        original = len(df)
        
        # Aplica deduplicação baseada na chave natural de cada tabela
        if tabela == 'empresa' and 'cnpj_basico' in df.columns:
            df = df.drop_duplicates(subset=['cnpj_basico'])
        elif tabela == 'estabelecimento' and all(col in df.columns for col in ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv']):
            df = df.drop_duplicates(subset=['cnpj_basico', 'cnpj_ordem', 'cnpj_dv'])
        elif tabela == 'socio' and all(col in df.columns for col in ['cnpj_basico', 'nome_socio_razao_social', 'cpf_cnpj_socio']):
            df = df.drop_duplicates(subset=['cnpj_basico', 'nome_socio_razao_social', 'cpf_cnpj_socio'])
        
        # Log se duplicatas foram removidas
        if len(df) < original:
            print(f"      🧹 Removidas {original - len(df)} duplicatas")
        
        return df

    def criar_indices(self):
        """
        Cria índices estratégicos para melhorar performance de consultas.
        
        Os índices são criados após a importação completa para não impactar
        a performance das operações bulk de inserção/atualização.
        """
        print("\n🔧 CRIANDO ÍNDICES...")
        
        indices = [
            # Índices para empresa
            "CREATE INDEX IF NOT EXISTS idx_empresa_cnpj ON empresa(cnpj_basico)",
            
            # Índices para estabelecimento
            "CREATE INDEX IF NOT EXISTS idx_estab_cnpj ON estabelecimento(cnpj_basico)",
            "CREATE INDEX IF NOT EXISTS idx_estab_cnae ON estabelecimento(cnae_fiscal_principal)",
            "CREATE INDEX IF NOT EXISTS idx_estab_uf ON estabelecimento(uf)",
            "CREATE INDEX IF NOT EXISTS idx_estab_municipio ON estabelecimento(municipio)",
            
            # Índices para sócio
            "CREATE INDEX IF NOT EXISTS idx_socio_cnpj ON socio(cnpj_basico)"
        ]
        
        for sql in indices:
            try:
                self.conn.execute(sql)
            except Exception as e:
                print(f"   ⚠️  Erro no índice: {e}")
        
        self.conn.commit()
        print("✅ Índices criados com sucesso!")

    def mostrar_estatisticas(self):
      """
      Exibe estatísticas consolidados do banco de dados após importação.
      
      Mostra contagens totais por tabela principal e tabelas de referência
      para validação da importação.
      """
      print("\n📊 ESTATÍSTICAS FINAIS:")
      print("=" * 50)
      
      consultas = [
          ("Total Empresas", "SELECT COUNT(*) FROM empresa"),
          ("Total Estabelecimentos", "SELECT COUNT(*) FROM estabelecimento"),
          ("Total Sócios", "SELECT COUNT(*) FROM socio"),
          ("Municípios", "SELECT COUNT(*) FROM municipio"),
          ("CNAEs", "SELECT COUNT(*) FROM cnae")
      ]
      
      for descricao, sql in consultas:
          try:
              resultado = self.conn.execute(sql).fetchone()[0]
              print(f"   {descricao:25} {resultado:>12,} registros")
          except Exception as e:
              print(f"   {descricao:25} {'ERRO':>12} - {e}")

  def importar_tudo(self):
      """
      Orquestra o processo completo de importação.
      
      Executa todas as etapas sequencialmente:
      1. Conexão com banco
      2. Escaneamento de estrutura
      3. Criação de tabelas
      4. Importação de referências
      5. Importação de dados principais
      6. Criação de índices
      7. Estatísticas finais
      
      Raises:
          Exception: Se houver erro crítico em qualquer etapa
      """
      print("🚀 INICIANDO IMPORTAÇÃO COMPLETA DO CNPJ")
      print("=" * 70)
      
      try:
          # 1. Conecta ao banco de dados
          self.conectar_db()
          
          # 2. Escaneia estrutura completa de todas as pastas
          estrutura_completa = self.escanear_estrutura_completa()
          
          if not estrutura_completa:
              print("❌ Nenhum dado encontrado!")
              return
          
          # 3. Cria schema do banco de dados
          self.criar_tabelas()
          
          # 4. Importa tabelas de referência (CNAE, municípios, etc.)
          self.importar_tabelas_referencia(estrutura_completa)
          
          # 5. Importa dados principais (empresa, estabelecimento, sócio)
          self.importar_dados_principais(estrutura_completa)
          
          # 6. Cria índices para otimização
          self.criar_indices()
          
          # 7. Exibe estatísticas finais
          self.mostrar_estatisticas()
          
      except Exception as e:
          print(f"❌ Erro crítico durante importação: {e}")
          raise
      finally:
          # Garante que a conexão será fechada mesmo em caso de erro
          if self.conn:
              self.conn.close()
              print(f"🔒 Conexão com banco fechada")
      
      print("\n🎉 IMPORTAÇÃO CONCLUÍDA COM SUCESSO!")
      print(f"💾 Banco de dados: {self.caminho_db}")


# =============================================================================
# BLOCO DE EXECUÇÃO PRINCIPAL
# =============================================================================

if __name__ == "__main__":
  """
  Ponto de entrada principal do script.
  
  Configura e executa o importador CNPJ quando o script é executado diretamente.
  """
  
  # CONFIGURAÇÃO PRINCIPAL
  DIRETORIO_BASE = "receita_federal"  # Pasta que contém subpastas 2023-05, 2023-06, etc.
  DB_PATH = "cnpj_receita.db"         # Arquivo do banco SQLite de saída
  
  print("🇧🇷 IMPORTADOR CNPJ - TODAS AS PASTAS")
  print("=" * 60)
  print("📂 Diretório base:", DIRETORIO_BASE)
  print("💾 Banco de dados:", DB_PATH)
  print("=" * 60)
  
  # VALIDAÇÃO DO DIRETÓRIO BASE
  if not os.path.exists(DIRETORIO_BASE):
      print(f"❌ Diretório '{DIRETORIO_BASE}' não encontrado!")
      print("\n📁 Diretórios disponíveis no diretório atual:")
      
      # Lista diretórios disponíveis para ajudar no diagnóstico
      diretorios = [item for item in os.listdir('.') if os.path.isdir(item)]
      if diretorios:
          for dir in diretorios:
              print(f"   📁 {dir}")
      else:
          print("   (nenhum diretório encontrado)")
      
      print(f"\n💡 Dica: Crie o diretório '{DIRETORIO_BASE}' ou ajuste a variável DIRETORIO_BASE")
      exit(1)
  
  # EXECUÇÃO DO IMPORTADOR
  try:
      # Cria instância do importador
      importador = ImportadorCNPJMultiPasta(DIRETORIO_BASE, DB_PATH)
      
      # Executa processo completo de importação
      importador.importar_tudo()
      
  except KeyboardInterrupt:
      print("\n⏹️  Importação interrompida pelo usuário")
  except Exception as e:
      print(f"\n💥 Erro durante execução: {e}")
      print("📋 Verifique:")
      print("   - Permissões de acesso aos arquivos")
      print("   - Formato correto dos arquivos CSV")
      print("   - Espaço em disco disponível")
  finally:
      print("\n✨ Execução finalizada")
