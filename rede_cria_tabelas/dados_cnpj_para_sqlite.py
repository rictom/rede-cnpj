# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 02:54:50 2021
Modified on Sun Jan 16 15:12:00 2022

@author: rictom
https://github.com/rictom/cnpj-sqlite

A rotina:
    -descompacta os arquivos já baixados do site da receita a partir de 2021 (layout novo);
    -cria uma base cnpj.db no formato sqlite;
    -cria indices nas colunas CNPJ, Razão Social, cpf/cnpj de sócios e nome de Sócios.
A rotina NÃO FAZ download dos arquivos do site da Receita. Utilize um gerenciador de downloads.
Os arquivos zipados da Receita devem ser colocados na definida na variável pasta_compactados.
Os arquivos descompactados e a base em sqlite serão gerados na pasta definida na variável pasta_saida.
A utilização da biblioteca DASK tem desempenho melhor do que o uso de PANDAS (quase 10x mais rápido).
"""

import pandas as pd, sqlite3, sqlalchemy
import glob, time, dask.dataframe as dd
import os, sys, zipfile

dataReferencia = 'xx/xx/2024' #input('Data de referência da base dd/mm/aaaa: ')
pasta_compactados = r"dados-publicos-zip" #local dos arquivos zipados da Receita
pasta_saida = r"dados-publicos" #esta pasta deve estar vazia. 

cam = os.path.join(pasta_saida, 'cnpj.db') 
if os.path.exists(cam):
    input(f'O arquivo {cam} já existe. Apague-o primeiro e rode este script novamente.')
    sys.exit()

bApagaDescompactadosAposUso = True

arquivos_zip = list(glob.glob(os.path.join(pasta_compactados,r'*.zip')))

if len(arquivos_zip) != 37:
    r = input(f'A pasta {pasta_compactados} deveria conter 37 arquivos zip, mas tem {len(arquivos_zip)}. É recomendável prosseguir apenas com todos os arquivos, senão a base ficará incompleta. Deseja prosseguir assim mesmo? (y/n) ')    
    if not r or r.lower()!='y':
        print('Para baixar os arquivos, use um gerenciador de downloads ou use o comando python dados_cnpj_baixa.py')	
        sys.exit()

print('Início:', time.asctime())
for arq in arquivos_zip:
    print(time.asctime(), 'descompactando ' + arq)
    with zipfile.ZipFile(arq, 'r') as zip_ref:
        zip_ref.extractall(pasta_saida)

dataReferenciaAux = list(glob.glob(os.path.join(pasta_saida, '*.EMPRECSV')))[0].split('.')[2] #formato DAMMDD, vai ser usado no final para inserir na tabela  _ref
if len(dataReferenciaAux)==len('D30610') and dataReferenciaAux.startswith('D'):
    dataReferencia = dataReferenciaAux[4:6] + '/' + dataReferenciaAux[2:4] + '/202' + dataReferenciaAux[1]
	
engine = sqlite3.connect(cam)
engine_url = f'sqlite:///{cam}'

#carrega tabelas pequenas e indexa
def carregaTabelaCodigo(extensaoArquivo, nomeTabela):
    arquivo = list(glob.glob(os.path.join(pasta_saida, '*' + extensaoArquivo)))[0]
    print('carregando tabela '+arquivo)
    dtab = pd.read_csv(arquivo, dtype=str, sep=';', encoding='latin1', header=None, names=['codigo','descricao'])
    dtab.to_sql(nomeTabela, engine, if_exists='replace', index=None)
    engine.execute(f'CREATE INDEX idx_{nomeTabela} ON {nomeTabela}(codigo);')
    if bApagaDescompactadosAposUso:
        os.remove(arquivo)
	    
carregaTabelaCodigo('.CNAECSV','cnae')
carregaTabelaCodigo('.MOTICSV', 'motivo')
carregaTabelaCodigo('.MUNICCSV', 'municipio')
carregaTabelaCodigo('.NATJUCSV', 'natureza_juridica')
carregaTabelaCodigo('.PAISCSV', 'pais')
carregaTabelaCodigo('.QUALSCSV', 'qualificacao_socio')

#carrega as tabelas grandes
#tipos = ['.EMPRECSV', '.ESTABELE', '.SOCIOCSV']

def sqlCriaTabela(nomeTabela, colunas):
    sql = 'CREATE TABLE ' + nomeTabela + ' ('
    for k, coluna in enumerate(colunas):
        sql += '\n' + coluna + ' TEXT'
        if k+1<len(colunas):
            sql+= ',' #'\n'
    sql += ')\n'
    return sql
    
colunas_empresas = ['cnpj_basico', 'razao_social',
           'natureza_juridica',
           'qualificacao_responsavel',
           'capital_social_str',
           'porte_empresa',
           'ente_federativo_responsavel']
           
colunas_estabelecimento = ['cnpj_basico','cnpj_ordem', 'cnpj_dv','matriz_filial', 
              'nome_fantasia',
              'situacao_cadastral','data_situacao_cadastral', 
              'motivo_situacao_cadastral',
              'nome_cidade_exterior',
              'pais',
              'data_inicio_atividades',
              'cnae_fiscal',
              'cnae_fiscal_secundaria',
              'tipo_logradouro',
              'logradouro', 
              'numero',
              'complemento','bairro',
              'cep','uf','municipio',
              'ddd1', 'telefone1',
              'ddd2', 'telefone2',
              'ddd_fax', 'fax',
              'correio_eletronico',
              'situacao_especial',
              'data_situacao_especial']    

colunas_socios = [
            'cnpj_basico',
            'identificador_de_socio',
            'nome_socio',
            'cnpj_cpf_socio',
            'qualificacao_socio',
            'data_entrada_sociedade',
            'pais',
            'representante_legal',
            'nome_representante',
            'qualificacao_representante_legal',
            'faixa_etaria'
          ]

colunas_simples = [
    'cnpj_basico',
    'opcao_simples',
    'data_opcao_simples',
    'data_exclusao_simples',
    'opcao_mei',
    'data_opcao_mei',
    'data_exclusao_mei']

sql = sqlCriaTabela('empresas', colunas_empresas)
engine.execute(sql)
sql = sqlCriaTabela('estabelecimento', colunas_estabelecimento)
engine.execute(sql)
sql = sqlCriaTabela('socios_original', colunas_socios)
engine.execute(sql)
sql = sqlCriaTabela('simples', colunas_simples)
engine.execute(sql)

def carregaTipo(nome_tabela, tipo, colunas):
    #usando dask, bem mais rápido que pandas
    arquivos = list(glob.glob(os.path.join(pasta_saida, '*' + tipo)))
    for arq in arquivos:
        print(f'carregando: {arq=}')
        print('lendo csv ...', time.asctime())
        ddf = dd.read_csv(arq, sep=';', header=None, names=colunas, encoding='latin1', dtype=str, na_filter=None)
        #dask possibilita usar curinga no nome de arquivo, por ex: 
        #ddf = dd.read_csv(pasta_saida+r'\*' + tipo, sep=';', header=None, names=colunas ...
        ddf.to_sql(nome_tabela, engine_url, index=None, if_exists='append', dtype=sqlalchemy.sql.sqltypes.TEXT)
	if bApagaDescompactadosAposUso:
            os.remove(arq)
        print('fim parcial...', time.asctime())

carregaTipo('empresas', '.EMPRECSV', colunas_empresas)
carregaTipo('estabelecimento', '.ESTABELE', colunas_estabelecimento)
carregaTipo('socios_original', '.SOCIOCSV', colunas_socios)
carregaTipo('simples', '.SIMPLES.CSV.*', colunas_simples)

#ajusta capital social e indexa as colunas

sqls = '''
ALTER TABLE empresas ADD COLUMN capital_social real;
update  empresas
set capital_social = cast( replace(capital_social_str,',', '.') as real);

ALTER TABLE empresas DROP COLUMN capital_social_str;

ALTER TABLE estabelecimento ADD COLUMN cnpj text;
Update estabelecimento
set cnpj = cnpj_basico||cnpj_ordem||cnpj_dv;

CREATE  INDEX idx_empresas_cnpj_basico ON empresas (cnpj_basico);
CREATE  INDEX idx_empresas_razao_social ON empresas (razao_social);
CREATE  INDEX idx_estabelecimento_cnpj_basico ON estabelecimento (cnpj_basico);
CREATE  INDEX idx_estabelecimento_cnpj ON estabelecimento (cnpj);
CREATE  INDEX idx_estabelecimento_nomefantasia ON estabelecimento (nome_fantasia);

CREATE INDEX idx_socios_original_cnpj_basico
ON socios_original(cnpj_basico);

CREATE TABLE socios AS 
SELECT te.cnpj as cnpj, ts.*
from socios_original ts
left join estabelecimento te on te.cnpj_basico = ts.cnpj_basico
where te.matriz_filial='1';

DROP TABLE IF EXISTS socios_original;

CREATE INDEX idx_socios_cnpj ON socios(cnpj);
CREATE INDEX idx_socios_cnpj_cpf_socio ON socios(cnpj_cpf_socio);
CREATE INDEX idx_socios_nome_socio ON socios(nome_socio);
CREATE INDEX idx_socios_representante ON socios(representante_legal);
CREATE INDEX idx_socios_representante_nome ON socios(nome_representante);

CREATE INDEX idx_simples_cnpj_basico ON simples(cnpj_basico);

CREATE TABLE "_referencia" (
	"referencia"	TEXT,
	"valor"	TEXT
);
'''

print('Inicio sqls:', time.asctime())
ktotal = len(sqls.split(';'))
for k, sql in enumerate(sqls.split(';')):
    print('-'*20 + f'\nexecutando parte {k+1}/{ktotal}:\n', sql)
    engine.execute(sql)
    print('fim parcial...', time.asctime())
print('fim sqls...', time.asctime())

#inserir na tabela referencia_

qtde_cnpjs = engine.execute('select count(*) as contagem from estabelecimento;').fetchone()[0]

engine.execute(f"insert into _referencia (referencia, valor) values ('CNPJ', '{dataReferencia}')")
engine.execute(f"insert into _referencia (referencia, valor) values ('cnpj_qtde', '{qtde_cnpjs}')")

#print('Aplicando VACUUM para diminuir o tamanho da base--------------------------------', time.ctime())
#engine.execute('VACUUM')
#print('Aplicando VACUUM-FIM-------------------------------', time.ctime())

# print('compactando... ', time.ctime())
# with zipfile.ZipFile(cam + '.7z', 'w',  zipfile.ZIP_DEFLATED) as zipf:
#     zipf.write(cam, os.path.split(cam)[1])    
# print('compactando... FIM ', time.ctime())

# import py7zr
# print(time.ctime(), 'compactando... ')
# with py7zr.SevenZipFile(cam + '.7z', 'w') as archive:
#     archive.writeall(cam, os.path.split(cam)[1])
# print(time.ctime(), 'compactando... Fim')

print('-'*20)
print(f'Foi criado o arquivo {cam}, com a base de dados no formato SQLITE.')
print('Qtde de empresas (matrizes):', engine.execute('SELECT COUNT(*) FROM empresas').fetchone()[0])
print('Qtde de estabelecimentos (matrizes e fiiais):', engine.execute('SELECT COUNT(*) FROM estabelecimento').fetchone()[0])
print('Qtde de sócios:', engine.execute('SELECT COUNT(*) FROM socios').fetchone()[0])

engine.commit()
engine.close()
print('FIM!!!', time.asctime())
