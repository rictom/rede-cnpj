# -*- coding: utf-8 -*-
"""
Created on set/2020
json a partir da tabela sqlite

@author: github rictom/rede-cnpj
2020-11-25 - Se uma tabela já existir, parece causar lentidão para o pandas pd.to_sql. 
Não fazer Create table ou criar índice para uma tabela a ser criada ou modificada pelo pandas
"""
import os, sys, glob
import time, copy, re, string, unicodedata, collections
import pandas as pd, sqlalchemy
from fnmatch import fnmatch 
'''
from sqlalchemy.pool import StaticPool
engine = create_engine('sqlite://',
                    connect_args={'check_same_thread':False},
                    poolclass=StaticPool)
'''
import config

try:
    camDbSqlite = config.config['BASE']['base_receita']
except:
    sys.exit('o arquivo sqlite não foi localizado. Veja o caminho da base no arquivo de configuracao rede.ini está correto.')
camDBSqliteFTS = config.config['BASE'].get('base_receita_fulltext','')
caminhoDBLinks = config.config['BASE'].get('base_links', '')
caminhoDBEnderecoNormalizado = config.config['BASE'].get('base_endereco_normalizado', '')
#logAtivo = True if config['rede']['logAtivo']=='1' else False #registra cnpjs consultados
logAtivo = config.config['ETC'].getboolean('logativo',False) #registra cnpjs consultados
#    ligacaoSocioFilial = True if config['rede']['ligacaoSocioFilial']=='1' else False #registra cnpjs consultados
ligacaoSocioFilial = config.config['ETC'].getboolean('ligacao_socio_filial',False) #registra cnpjs consultados

class DicionariosCodigos():
    def __init__(self):
        dfaux = pd.read_csv(r"tabelas/tabela-de-qualificacao-do-socio-representante.csv", sep=';')
        self.dicQualificacao_socio = pd.Series(dfaux.descricao.values,index=dfaux.codigo).to_dict()
        dfaux = pd.read_csv(r"tabelas/DominiosMotivoSituaoCadastral.csv", sep=';', encoding='latin1')
        self.dicMotivoSituacao = pd.Series(dfaux['Descrição'].values, index=dfaux['Código']).to_dict()
        dfaux = pd.read_excel(r"tabelas/cnae.xlsx", sheet_name='codigo-grupo-classe-descr')
        self.dicCnae = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        self.dicSituacaoCadastral = {'01':'Nula', '02':'Ativa', '03':'Suspensa', '04':'Inapta', '08':'Baixada'}
        self.dicPorteEmpresa = {'00':'Não informado', '01':'Micro empresa', '03':'Empresa de pequeno porte', '05':'Demais (Médio ou Grande porte)'}
        dfaux = pd.read_csv(r"tabelas/natureza_juridica.csv", sep=';', encoding='utf8', dtype=str)
        self.dicNaturezaJuridica = pd.Series(dfaux['natureza_juridica'].values, index=dfaux['codigo']).to_dict()

gdic = DicionariosCodigos()

dfaux=None

gTableIndex = 0
gEngineExecutionOptions = {"sqlite_raw_colnames": True, 'pool_size':1} #poll_size=1 força usar só uma conexão??
kCaractereSeparadorLimite = '@'

#decorator para medir tempo de execução de função
def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()        
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print ('%r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result    
    return timed

def apagaTabelasTemporarias():
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    con.execute('DROP TABLE if exists tmp_cnpjs')
    con.execute('DROP TABLE if exists tmp_cpfnomes')
    con.execute('DROP TABLE if exists tmp_ids')
    con.execute('DROP TABLE if exists tmp_socios')
    con = None

apagaTabelasTemporarias() #apaga quando abrir o módulo

def buscaPorNome(nomeIn, limite=10): #nome tem que ser completo. Com Teste, pega item randomico
    '''camDBSqliteFTS base com indice full text search, fica rápido com match mas com = fila lento, por isso
        precisa fazer consulta em camDbSqlite quando não for usar match
    '''
    #remove acentos
    nomeIn = nomeIn.strip().upper()
    nomeMatch = ''
    try:
        limite = int(limite)
    except:
        limite = 0
    # print('limite', limite)
    limite =  min(limite,100) if limite else 10
    if '*' in nomeIn or '?' in nomeIn or '"' in nomeIn:
        nomeMatch = nomeIn.strip()
        if nomeMatch.startswith('*'): #match do sqlite não aceita * no começo
            nomeMatch = nomeMatch[1:].strip()
        if camDBSqliteFTS:
            con = sqlalchemy.create_engine(f"sqlite:///{camDBSqliteFTS}", execution_options=gEngineExecutionOptions)
        tabelaSocios = 'socios_search'
        tabelaEmpresas = 'empresas_search'
    else:
        con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
        tabelaSocios = 'socios'
        tabelaEmpresas = 'empresas'

    nome = ''.join(x for x in unicodedata.normalize('NFKD', nomeIn) if x in string.printable).upper()
    cjs, cps = set(), set()
    #if (' ' not in nome) and (nome not in ('TESTE',)): #só busca nome
    #    return cjs, cps
    # print('nomeMatch', nomeMatch)
    # print('nome',nome)
    if nomeMatch:
         if not camDBSqliteFTS: #como não há tabela, não faz consulta por match
             #con = None
             return set(), set()
         query = f'''
                SELECT DISTINCT cnpj_cpf_socio, nome_socio
                FROM {tabelaSocios} 
                where nome_socio match \'{nomeMatch}\'
                limit {limite*10}
            '''
    elif nomeIn=='TESTE':
        query = f'select cnpj_cpf_socio, nome_socio from {tabelaSocios} where rowid > (abs(random()) % (select (select max(rowid) from {tabelaSocios})+1)) LIMIT 1;'

    else:
        query = f'''
                SELECT distinct cnpj_cpf_socio, nome_socio
                FROM {tabelaSocios} 
                where nome_socio=\'{nome}\'
                limit {limite}
            '''
    nomeMatch = nomeMatch.replace('"','')
    # print('query', query)
    contagemRegistros = 0
    for r in con.execute(query):
        if contagemRegistros>=limite:
            break
        if nomeMatch:
            if not fnmatch(r.nome_socio, nomeMatch):
                continue
        if len(r.cnpj_cpf_socio)==14:
            cjs.add(r.cnpj_cpf_socio)
        elif len(r.cnpj_cpf_socio)==11:
            cps.add((r.cnpj_cpf_socio, r.nome_socio))
        contagemRegistros += 1

    if nome=='TESTE':
        print('##TESTE com identificador aleatorio:', cjs, cps)    
        con = None
        return cjs, cps
    if nomeMatch:
        query = f'''
                    SELECT cnpj, razao_social
                    FROM {tabelaEmpresas} 
                    where razao_social match \'{nomeMatch}\'  
                    limit {limite*10}
                '''        
    else:
        # pra fazer busca por razao_social, a coluna deve estar indexada
        query = f'''
                    SELECT cnpj, razao_social
                    FROM {tabelaEmpresas} 
                    where razao_social=\'{nome}\'
                    limit {limite}
                '''        
    for r in con.execute(query):
        if contagemRegistros>=limite:
            break
        if nomeMatch:
            if not fnmatch(r.razao_social, nomeMatch):
                continue
        cjs.add(r.cnpj)     
        contagemRegistros +=1
    con = None
    return cjs, cps
#.def buscaPorNome(

def buscaPorNome01(nome): #nome tem que ser completo. Com Teste, pega item randomico
    #remove acentos
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    nome = ''.join(x for x in unicodedata.normalize('NFKD', nome) if x in string.printable).upper()
    cjs, cps = set(), set()
    if (' ' not in nome) and (nome not in ('TESTE',)): #só busca nome
        return cjs, cps
    if nome=='TESTE':
        query = 'select cnpj_cpf_socio, nome_socio from socios where rowid > (abs(random()) % (select (select max(rowid) from socios)+1)) LIMIT 1;'
    else:
        query = f'''
                SELECT cnpj_cpf_socio, nome_socio
                FROM socios 
                where nome_socio=\"{nome}\"
            '''
    for r in con.execute(query):
        if len(r.cnpj_cpf_socio)==14:
            cjs.add(r.cnpj_cpf_socio)
        elif len(r.cnpj_cpf_socio)==11:
            cps.add((r.cnpj_cpf_socio, r.nome_socio))
    if nome=='TESTE':
        print('##TESTE', cjs, cps)
    # pra fazer busca por razao_social, a coluna deve estar indexada
    query = f'''
                SELECT cnpj
                FROM empresas 
                where razao_social=\"{nome}\"
            '''        
    for r in con.execute(query):
        cjs.add(r.cnpj)     
    con = None
    return cjs, cps
#.def buscaPorNome01

def separaEntrada(cpfcnpjIn='', listaIds=None):
    cnpjs = set()
    cpfnomes = set()
    outrosIdentificadores = set() #outros identificadores, com EN_ (supondo dois caracteres mais underscore) 
    if cpfcnpjIn:
        lista = cpfcnpjIn.split(';')
        lista = [i.strip() for i in lista]
    else:
        lista = listaIds
    for i in lista:
        if i.startswith('PJ_'):
            cnpjs.add(i[3:])
        elif i.startswith('PF_'):
            cpfcnpjnome = i[3:]
            cpf = cpfcnpjnome[:11]
            nome = cpfcnpjnome[12:]
            cpfnomes.add((cpf,nome))  
        elif len(i)>3 and i[2]=='_':
            outrosIdentificadores.add(i)
        else:
            limite = 0
            if kCaractereSeparadorLimite in i:
                i, limite = kCaractereSeparadorLimite.join(i.split(kCaractereSeparadorLimite)[0:-1]).strip(), i.split(kCaractereSeparadorLimite)[-1]
            soDigitos = ''.join(re.findall('\d', str(i)))
            if len(soDigitos)==14:
                cnpjs.add(soDigitos)
            elif len(soDigitos)==11:
                pass #fazer verificação por CPF??
            elif not soDigitos:
                cnpjsaux, cpfnomesaux = buscaPorNome(i, limite=limite)
                cnpjs.update(cnpjsaux)
                cpfnomes.update(cpfnomesaux)  
    return cnpjs, cpfnomes, outrosIdentificadores
#.def separaEntrada


def jsonRede(cpfcnpjIn, camada=1 ):    
    if cpfcnpjIn:
        return camadasRede(cpfcnpjIn = cpfcnpjIn, camada=camada, bjson=True)
    else:
        return {'no': [], 'ligacao':[]} 
#.def jsonRede

dtype_tmp_ids={'identificador':sqlalchemy.types.VARCHAR,
                       'grupo':sqlalchemy.types.VARCHAR,
                       'camada':sqlalchemy.types.INTEGER }
dtype_tmp_cnpjs={'cnpj':sqlalchemy.types.VARCHAR,
                       'grupo':sqlalchemy.types.VARCHAR,
                       'camada':sqlalchemy.types.INTEGER }
dtype_tmp_cpfnomes={'cpf':sqlalchemy.types.VARCHAR,
                       'nome':sqlalchemy.types.VARCHAR,
                       'grupo':sqlalchemy.types.VARCHAR,
                       'camada':sqlalchemy.types.INTEGER }

def criaTabelasTmpParaCamadas(con, cpfcnpjIn='', listaIds=None, grupo=''):
    #con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})
    global gTable
    #con.execute('DROP TABLE IF EXISTS tmp_cnpjs;') #xx
    #con.execute('DROP TABLE IF EXISTS tmp_cpfnomes;') #xx
    apagaTabelasTemporarias()
    # con.execute('''
    #     CREATE TEMP TABLE tmp_cnpjs (
    #  	cnpj TEXT, 
    #  	grupo TEXT, 
    #  	camada INTEGER
    #     )''')
    # con.execute('''
    #     CREATE TEMP TABLE tmp_cpfnomes (
    #  	cpf TEXT, 
    #  	nome TEXT, 
    #  	grupo TEXT, 
    #  	camada INTEGER
    #     )''')
    if cpfcnpjIn:
        cnpjs, cpfnomes, outrosIdentificadores = separaEntrada(cpfcnpjIn=cpfcnpjIn)
    else:
        cnpjs, cpfnomes, outrosIdentificadores = separaEntrada(listaIds=listaIds)
    camadasIds = {}
    
    ids = set(['PJ_'+c for c in cnpjs])
    ids.update(set(['PF_'+cpf+'-'+nome for cpf,nome in cpfnomes]))
    ids.update(outrosIdentificadores)
    #con.execute('DROP TABLE IF EXISTS tmp_ids;') #xx
    # con.execute('''
    #     CREATE TEMP TABLE tmp_ids (
    #  	identificador TEXT,
    #     grupo TEXT
    #     camada INTEGER
    #     )''')
    dftmptable = pd.DataFrame({'identificador' : list(ids)})
    dftmptable['camada'] = 0
    dftmptable['grupo'] = grupo
    #con.execute('DELETE FROM tmp_ids')
    #dftmptable.set_index('identificador', inplace=True)
    dftmptable.to_sql('tmp_ids', con=con, if_exists='replace', index=False, dtype=dtype_tmp_ids)
    #indice deixa a busca lenta!
    #con.execute('CREATE INDEX ix_tmp_ids_index ON tmp_ids ("identificador")')
    camadasIds = {i:0 for i in ids}
    # for cnpj in cnpjs:
    #     camadasIds[cnpj]=0
    # for cpf,nome in cpfnomes:
    #     camadasIds[(cpf, nome)] = 0;
    for outros in outrosIdentificadores:
        camadasIds[outros]=0   
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable['grupo'] = grupo
    dftmptable['camada'] = 0
    #con.execute('DELETE FROM tmp_cnpjs')
    dftmptable.to_sql('tmp_cnpjs', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cnpjs)
    #dftmptable.to_sql('tmp_cnpjs', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cnpjs)

    dftmptable = pd.DataFrame(list(cpfnomes), columns=['cpf', 'nome'])
    dftmptable['grupo'] = grupo
    dftmptable['camada'] = 0
    #con.execute('DELETE FROM tmp_cpfnomes')
    dftmptable.to_sql('tmp_cpfnomes', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cpfnomes)    
    return camadasIds, cnpjs, cpfnomes #, ids
#.def criaTabelasTmpParaCamadas

def cnpj2id(cnpj):
    return 'PJ_' + cnpj

def cpfnome2id(cpf,nome):
    return 'PF_'+cpf+'-'+nome

def id2cpfnome(id):
    if id.startswith('PF_'):
        return id[3:14], id[15:]
    
def id2cnpj(id):
    return id[3:]


@timeit
def camadasRede(cpfcnpjIn='', listaIds=None, camada=1, grupo='', bjson=True):    
    # usando SQL
    #se cpfcnpjIn=='', usa dados das tabelas tmp_cnpjs e tmp_cpfnomes, não haverá camada=0
    #se fromTmpTable=False, espera que cpfcnpjIn='cpf-nome;cnpj;nome...'
    #se fromTmpTable=True, ignora cpfcnpjIn e pega dados a partir de tmp_cnpjs e tmp_cpfnomes
    #print('INICIANDO-------------------------')
    #print(f'camadasRede ({camada})-{cpfcnpjIn}-inicio: ' + time.ctime() + ' ', end='')
    mensagem = {'lateral':'', 'popup':'', 'confirmar':''}
    #con=sqlite3.connect(camDbSqlite)
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    '''
    https://stackoverflow.com/questions/17497614/sqlalchemy-core-connection-context-manager
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        print(conn.closed)
    print(conn.closed)'''
    grupo = str(grupo)
    nosaux = []
    #nosids = set()
    ligacoes = []
    #setOrigDest = set()
    camadasIds, cnpjs, cpfnomes  = criaTabelasTmpParaCamadas(con, cpfcnpjIn=cpfcnpjIn, listaIds=listaIds, grupo=grupo)
    # if cpfcnpjIn:
    #     camadasIds = criaTabelasTmpParaCamadas(con, cpfcnpjIn=cpfcnpjIn, grupo=grupo, listaCpfCnpjs=listaCpfCnpjs)
    # else:
    #     camadasIds = {}
 
    #cnpjs=set() #precisa adicionar os cnpjs que não tem sócios
    #cpfnomes = set()
    dicRazaoSocial = {} #excepcional, se um cnpj que é sócio na tabela de socios não tem cadastro na tabela empresas
                
    for cam in range(camada):  
#         query_indices = '''
#             CREATE unique INDEX ix_tmp_cnpjs ON tmp_cnpjs (cnpj);
#             CREATE unique INDEX ix_tmp_cpfnomes ON tmp_cpfnomes (cpf, nome);           
# 			CREATE  INDEX ix_tmp_cpfnomes_cpf ON tmp_cpfnomes (cpf);
# 			CREATE  INDEX ix_tmp_cpfnomes_nome ON tmp_cpfnomes (nome);
#             CREATE unique INDEX ix_tmp_ids ON tmp_ids (identificador);
#             '''
        whereMatriz = ''
        if bjson and not ligacaoSocioFilial:
            if cam==-1:
                whereMatriz = ''
            else:
                # verificar, talvez não esteja correto, precisa ver a camada da filial
                whereMatriz = '''
                 WHERE substr(t.cnpj,9,4)="0001" 
                '''
                #AND (length(cnpj_cpf_socio)<>14 OR substr(cnpj_cpf_socio, 9, 4)="0001")
           
        
        query = f''' 
        DROP TABLE if exists tmp_socios;
        
        CREATE TABLE tmp_socios AS
        SELECT DISTINCT 
        * From (
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
        FROM socios t
        INNER JOIN tmp_cnpjs tl
        ON  tl.cnpj = t.cnpj
        UNION
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
        FROM socios t
        INNER JOIN tmp_cnpjs tl
        ON tl.cnpj = t.cnpj_cpf_socio
        {whereMatriz}
        UNION
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
        FROM socios t
        INNER JOIN tmp_cpfnomes tn ON tn.nome= t.nome_socio AND tn.cpf=t.cnpj_cpf_socio
        {whereMatriz}
        ) as taux 
        ; 
        			
        Insert INTO tmp_cnpjs (cnpj, grupo, camada)
        select distinct ts.cnpj, "{grupo}" as grupo, {cam+1} as camada
        From tmp_socios ts
        left join tmp_cnpjs tc on tc.cnpj = ts.cnpj 
        where tc.cnpj is NULL;
        
        Insert INTO tmp_cnpjs (cnpj, grupo, camada)
        select distinct cnpj_cpf_socio as cnpj,"{grupo}" as grupo, {cam+1} as camada
        From tmp_socios ts
        left join tmp_cnpjs tc on tc.cnpj = ts.cnpj_cpf_socio 
        where (tc.cnpj is NULL) AND (length(cnpj_cpf_socio)=14);
        
        Insert INTO tmp_cpfnomes (cpf, nome, grupo, camada)
        select distinct cnpj_cpf_socio as cpf, nome_socio as nome, "{grupo}" as grupo, {cam+1} as camada
        From tmp_socios ts
        left join tmp_cpfnomes tcn on tcn.cpf = ts.cnpj_cpf_socio and tcn.nome = ts.nome_socio
        where  tcn.cpf is NULL AND tcn.nome is NULL and length(cnpj_cpf_socio)=11;
                    
        Insert INTO tmp_ids (identificador, grupo, camada)
        select distinct "PJ_" || t.cnpj as identificador,  t.grupo, t.camada
        From tmp_cnpjs t
        left join tmp_ids on tmp_ids.identificador =  ("PJ_" || t.cnpj)
        where tmp_ids.identificador is NULL;
        
        Insert INTO tmp_ids (identificador, grupo, camada)
        select distinct "PF_" || t.cpf || "-" || t.nome  as identificador, t.grupo, t.camada
        From tmp_cpfnomes t
        left join tmp_ids on tmp_ids.identificador = ("PF_" || t.cpf || "-" || t.nome)
        where tmp_ids.identificador is NULL;
        '''
        for sql in query.split(';'):
            con.execute(sql)
    #.for cam in range(camada): 
    if camada==0:
        #gambiarra, em camada 0, não apaga a tabela tmp_socios, por isso pega dados de consulta anterior.
        query0 = ''' 
        CREATE TABLE tmp_socios AS
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
        FROM socios t
        limit 0
        '''
        con.execute(query0)
 
    
    queryLertmp = '''
        Select *
        from tmp_ids
        where substr(identificador,1,3)='PF_'
    '''
    for k in con.execute(queryLertmp):
        kid = k['identificador']
        _, descricao = id2cpfnome(kid) #kid[15:]
        no = {'id': kid, 'descricao':descricao, 
                'camada': k['camada'], 
                'situacao_ativa': True, 
                #'empresa_situacao': 0, 'empresa_matriz': 1, 'empresa_cod_natureza': 0, 
                'logradouro':'',
                'municipio': '', 'uf': ''} 
        camadasIds[kid] = k['camada']
        nosaux.append(copy.deepcopy(no))         
    querySocios = '''
        select *
        from tmp_socios
    '''
    for k in con.execute(querySocios):
        ksocio = k['cnpj_cpf_socio']
        if len(ksocio)==14:
            destino = cnpj2id(ksocio) #'PJ_'+ ksocio  
        else:
            destino = cpfnome2id(ksocio,k['nome_socio']) # 'PF_'+ksocio+'-'+k['nome_socio']
        ligacao = {"origem":cnpj2id(k['cnpj']), #'PJ_'+k['cnpj'], 
                   "destino":destino, 
                   "cor": "silver", #"cor":"gray", 
                   "camada":0,
                   "tipoDescricao":'sócio',
                   "label":gdic.dicQualificacao_socio.get(int(k['cod_qualificacao']),'').strip()}
        ligacoes.append(copy.deepcopy(ligacao))
    if logAtivo or not bjson:
        conlog = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
        conlog.execute('create table if not exists log_cnpjs (cnpj text, grupo text, camada text)')
        conlog.execute('''insert into log_cnpjs 
            select * from tmp_cnpjs; ''')
        conlog.execute('create table if not exists log_cpfnomes (cpf text, nome text, grupo text, camada text);')
        conlog.execute('''insert into log_cpfnomes 
            select cpf, nome, grupo, cast(camada as int) from tmp_cpfnomes; ''')
        conlog = None
    if not bjson:
        con = None
        return len(camadasIds)
    for k in con.execute('Select * from tmp_cnpjs'):
        kcnpj = k['cnpj']
        cnpjs.add(kcnpj)
        #camadasIds[kcnpj] = k['camada']
        camadasIds[cnpj2id(kcnpj)] = k['camada']
    nos = dadosDosNosCNPJs(con=con, cnpjs=cnpjs, nosaux=nosaux, dicRazaoSocial=dicRazaoSocial, camadasIds=camadasIds)
    #camada=0, pega só os dados
    if camada>0: 
        #esta chamada de endereco estava deixando a rotina lenta. Ficou OK removendo a criação de tabela prévia.
        #jsonEnderecos = camadaLink(cpfcnpjIn='',conCNPJ=con, camada=1,  grupo=grupo,  listaIds=list(camadasIds.keys()), tipoLink='endereco')
        ids = set() #{'PJ_' + icnpj for icnpj in cnpjs}
        for item in camadasIds.keys():
            prefixo = ''
            try: #se for cpfnome, é tuple e não consigo separar o pedaço.
                prefixo = item[:3]
                if prefixo[2]=='_' and prefixo!='PF_':
                    ids.add(item)
            except:
                continue
        jsonEnderecos = camadaLink(cpfcnpjIn='',conCNPJ=con, camada=1,  grupo=grupo,  listaIds=ids, tipoLink='endereco')
        
        #nos.extend([copy.deepcopy(item) for item in jsonEnderecos['no'] if item['id'] not in camadasIds])
        #nos.extend([item for item in jsonEnderecos['no'] if item['id'] not in camadasIds]) #ISTO é mais lento que for???
        for item in jsonEnderecos['no']:
            if item['id'] not in camadasIds:
                nos.append(item)
        ligacoes.extend(jsonEnderecos['ligacao'])
    #print(' jsonenderecos-fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    textoJson={'no': nos, 'ligacao':ligacoes, 'mensagem':mensagem} 
    con = None
    #print(listaIds)
    #print(textoJson)
    #print(' fim: ' + time.ctime())
    #print(' fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    return textoJson
#.def camadasRede



def dadosDosNosCNPJs(con, cnpjs, nosaux, dicRazaoSocial, camadasIds):
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable['grupo'] = ''
    dftmptable['camada'] = 0
    #con.execute('DELETE FROM tmp_cnpjs')
    dftmptable.to_sql('tmp_cnpjsdados', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cnpjs)
    query = '''
                SELECT t.cnpj, razao_social, situacao, matriz_filial,
                tipo_logradouro, logradouro, numero, complemento, bairro,
                municipio, uf,  cod_nat_juridica
                FROM empresas t
                INNER JOIN tmp_cnpjsdados tp on tp.cnpj=t.cnpj
            ''' #pode haver empresas fora da base de teste

    setCNPJsRecuperados = set()
    for k in con.execute(query):
        listalogradouro = [j.strip() for j in [k['logradouro'].strip(), k['numero'], k['complemento'].strip(';'), k['bairro']] if j.strip()]
        logradouro = ', '.join(listalogradouro)
        no = {'id': cnpj2id(k['cnpj']), 'descricao': k['razao_social'], 
              'camada': camadasIds[cnpj2id(k['cnpj'])], 'tipo':0, 'situacao_ativa': k['situacao']=='02',
              'logradouro': f'''{k['tipo_logradouro']} {logradouro}''',
              'municipio': k['municipio'], 'uf': k['uf'], 'cod_nat_juridica':k['cod_nat_juridica']
              }
        nosaux.append(copy.deepcopy(no))
        setCNPJsRecuperados.add(k['cnpj'])
    #trata caso excepcional com base de teste, cnpj que é sócio não tem registro na tabela empresas
    diffCnpj = cnpjs.difference(setCNPJsRecuperados)
    for cnpj in diffCnpj:
        no = {'id': cnpj2id(cnpj), 'descricao': dicRazaoSocial.get(cnpj, 'NÃO FOI LOCALIZADO NA BASE'), 
              'camada': camadasIds[cnpj2id(cnpj)], 'tipo':0, 'situacao_ativa': True,
              'logradouro': '',
              'municipio': '', 'uf': '',  'cod_nat_juridica':''
              }
        nosaux.append(copy.deepcopy(no))
    #ajusta nos, colocando label
    nosaux=ajustaLabelIcone(nosaux)
    nos = nosaux #nosaux[::-1] #inverte, assim os nos de camada menor serao inseridas depois, ficando na frente
    nos.sort(key=lambda n: n['camada'], reverse=True) #inverte ordem, porque os últimos icones vão aparecer na frente. Talvez na prática não seja útil.
    con.execute('DROP TABLE if exists tmp_cnpjsdados ') 
    return nos
#.def dadosDosNosCNPJs

@timeit
def camadaLink(cpfcnpjIn='', conCNPJ=None, camada=1, numeroItens=15, valorMinimo=0, valorMaximo=0, grupo='', bjson=True, listaIds=None, tipoLink='link'):    
    #se cpfcnpjIn=='', usa dados das tabelas tmp_cnpjs e tmp_cpfnomes, não haverá camada=0
    #se fromTmpTable=False, espera que cpfcnpjIn='cpf-nome;cnpj;nome...'
    #se fromTmpTable=True, ignora cpfcnpjIn e pega dados a partir de tmp_cnpjs e tmp_cpfnomes
    #se numeroItens=0 ou <0, fica sem limite
    #print('INICIANDO-------------------------')
    #print(f'camadasLink ({camada})-{cpfcnpjIn}-inicio: ' + time.ctime() + ' ', end='')
    mensagem = {'lateral':'', 'popup':'', 'confirmar':''}
    if tipoLink=='endereco':
        if not caminhoDBEnderecoNormalizado:
            #mensagem['popup'] = 'Não há tabela de enderecos configurada.'
            return {'no': [], 'ligacao':[], 'mensagem': mensagem} 

    if tipoLink=='link':
        if not caminhoDBLinks:
            mensagem['popup'] = 'Não há tabela de links configurada.'
            return {'no': [], 'ligacao':[], 'mensagem': mensagem} 
        con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBLinks}",execution_options=gEngineExecutionOptions)
        tabela = 'links'
        bValorInteiro = False
        query = f''' SELECT * From (
                    SELECT t.id1, t.id2, t.descricao, t.valor
                    FROM {tabela} t
                    INNER JOIN tmp_ids tl
                    ON  tl.identificador = t.id1
                    UNION
                    SELECT t.id1, t.id2, t.descricao, t.valor
                    FROM {tabela} t
                    INNER JOIN tmp_ids tl
                    ON  tl.identificador = t.id2
                     ) ORDER by valor DESC
                    '''
    elif tipoLink=='endereco':

        con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBEnderecoNormalizado}", execution_options=gEngineExecutionOptions)        
        #tabela = 'link_endereco'
        tabela = 'link_ete'
        valorMinimo=0
        valorMaximo=0
        numeroItens=0
        bValorInteiro = True
        # query = f''' SELECT t.id1, t.id2, t.descricao, t.valor
        #              FROM {tabela} t
        #              INNER JOIN tmp_ids tl ON  tl.identificador = t.id1
        #              UNION
        #              SELECT t.id1, t.id2, t.descricao, t.valor
        #              FROM {tabela} t
        #              INNER JOIN tmp_ids tl ON  tl.identificador = t.id2
        #              '''
        query = f''' SELECT distinct t.id1, t.id2, t.descricao, t.valor
                     FROM tmp_ids tl
                     INNER JOIN {tabela} t ON  tl.identificador = t.id1
                     UNION
                     SELECT t.id1, t.id2, t.descricao, t.valor
                     FROM tmp_ids tl
                     INNER JOIN {tabela} t ON  tl.identificador = t.id2
                     '''
    else:
        print('tipoLink indefinido')
        
    grupo = str(grupo)
    nosaux = []
    #nosids = set()
    ligacoes = []
    setLigacoes = set()
    camadasIds, cnpjs, cpfnomes = criaTabelasTmpParaCamadas(con, cpfcnpjIn=cpfcnpjIn, listaIds=listaIds, grupo=grupo)
    #print( 'nosids', nosids   )
    cnpjsInicial = copy.copy(cnpjs)
    dicRazaoSocial = {} #excepcional, se um cnpj que é sócio na tabela de socios não tem cadastro na tabela empresas
    limite = numeroItens #15
    #passo = numeroItens*2 #15
    #cnt1 = collections.Counter() #contadores de links para o id1 e id2
    #cnt2 = collections.Counter()    
    cntlink = collections.Counter()
    for cam in range(camada):       
        #no sqlite, o order by é feito após o UNION.
        #ligacoes = [] #tem que reiniciar a cada loop

        #orig_destAnt = ()

        #tem que mudar o método, teria que fazer uma query para cada entrada
        for k in con.execute(query + ' LIMIT ' + str(limite) if limite else query):
            if not(k['id1']) or not(k['id2']):
                print('####link invalido!!!', k['id1'], k['id2'], k['descricao'], k['valor'])
                continue #caso a tabela esteja inconsistente
            #limita a quantidade de ligacoes por item
            if numeroItens>0:
                if cntlink[k['id1']]>numeroItens or cntlink[k['id2']]>numeroItens:
                    continue
            if valorMinimo:
                if k['valor']<valorMinimo:
                     continue
            if valorMaximo:
                if valorMaximo < k['valor']:
                    continue
            cntlink[k['id1']] += 1
            cntlink[k['id2']] += 1
            
            #nosids.add(k['id1'])
            #nosids.add(k['id2'])
            if k['id1'] not in camadasIds:
                camadasIds[k['id1']] = cam+1            
            if k['id2'] not in camadasIds:
                camadasIds[k['id2']] = cam+1
            #neste caso, não deve haver ligação repetida, mas é necessário colocar uma verificação se for ligações generalizadas
            # if orig_destAnt == ('PJ_'+k['cnpj'], destino):
            #     print('XXXXXXXXXXXXXX repetiu ligacao', orig_destAnt)
            # orig_destAnt = ('PJ_'+k['cnpj'], destino)
            if (k['id1'], k['id2']) not in setLigacoes: #cam+1==camada and bjson: #só pega dados na última camada
                ligacao = {"origem":k['id1'], "destino":k['id2'], 
                           "cor": "silver" if  tipoLink=='endereco' else "gold", #"cor":"gray", 
                           "camada":cam+1, "tipoDescricao":'link',"label":k['descricao'] + ':' + ajustaValor(k['valor'], bValorInteiro)}
                ligacoes.append(copy.deepcopy(ligacao))
                setLigacoes.add((k['id1'], k['id2']))
            else:
                print('####ligacao repetida. A implementar')
        #.for k in con.execute(query):
        listaProximaCamada = [item for item in camadasIds if camadasIds[item]>cam]
        dftmptable = pd.DataFrame({'identificador' : listaProximaCamada})
        dftmptable['grupo'] = grupo
        dftmptable['camada'] = dftmptable['identificador'].apply(lambda x: camadasIds[x])
        #dftmptable['camada'] = dftmptable['cnpj'].map(camadasIds)
        #con.execute('DELETE from tmp_ids;')
        #dftmptable.set_index('identificador', inplace=True)
        dftmptable.to_sql('tmp_ids', con=con, if_exists='replace', index=False, dtype=dtype_tmp_ids)
        #curioso, esse índice deixa a busca lenta!!!!
        #con.execute('CREATE INDEX ix_tmp_ids_index ON tmp_ids ("identificador")')
        limite = limite * numeroItens * 2

    # if logAtivo or not bjson:
    #     conlog = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})
    #     conlog.execute('create table if not exists log_cnpjs (cnpj text, grupo text, camada text)')
    #     conlog.execute('''insert into log_cnpjs 
    #         select * from tmp_cnpjs; ''')
    #     conlog.execute('create table if not exists log_cpfnomes (cpf text, nome text, grupo text, camada text);')
    #     conlog.execute('''insert into log_cpfnomes 
    #         select cpf, nome, grupo, cast(camada as int) from tmp_cpfnomes; ''')
    #     conlog = None
    # if not bjson:
    #     print('camadasRede-fim: ' + time.ctime())
    #     return len(camadasIds)
    #cnpjs = set([c[3:] for c in setOrigDest if c.startswith('PJ_')])
    #print('nosids', nosids)
    for c in camadasIds:
        if c.startswith('PJ_'):
            cnpjs.add(c[3:])
        else:
            if c.startswith('PF_'):
                nome = c[15:] #supõe 'PF_12345678901-nome'
                no = {'id': c, 'descricao':nome, 
                       'camada': camadasIds[c], 
                       'situacao_ativa': True, 
                       'logradouro':'',
                       'municipio': '', 'uf': ''} 
            else: #elif c.startswith('EN_'):
                no = {'id': c, 'descricao':'', 
                       'camada': camadasIds[c], 
                       'situacao_ativa': True, 
                       'logradouro':'',
                       'municipio': '', 'uf': ''} 
            nosaux.append(copy.deepcopy(no))
    # for c in cnpjs:
    #     camadasIds[c] = camadasIds['PJ_'+c]
    if conCNPJ:
        conCNPJaux =conCNPJ
    else:
        conCNPJaux = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    cnpjs = cnpjs.difference(cnpjsInicial)
    nos = dadosDosNosCNPJs(conCNPJaux, cnpjs, nosaux, dicRazaoSocial, camadasIds)
    textoJson={'no': nos, 'ligacao':ligacoes, 'mensagem':mensagem} 
    con = None
    if conCNPJ:
        conCNPJaux = None
    #print(' fim: ' + time.ctime())
    #print('camadaLink fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    return textoJson
#.def camadaLink

def apagaLog():
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    con.execute('DROP TABLE IF EXISTS log_cnpjs;')
    con.execute('DROP TABLE IF EXISTS log_cpfnomes;')
    con = None
                
def jsonDados(cpfcnpjIn):    
    #print('INICIANDO-------------------------')
    #dados de cnpj para popup de Dados
    #print('jsonDados-inicio: ' + time.ctime())
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}",execution_options=gEngineExecutionOptions)
    cnpjs, cpfnomes, outrosIdentificadores = separaEntrada(cpfcnpjIn)    
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable['grupo']=''
    dftmptable['camada']=0
    dftmptable.to_sql('tmp_cnpjs1', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
    query = '''
                SELECT *
                FROM empresas t
                INNER JOIN tmp_cnpjs1 tp on tp.cnpj=t.cnpj
            '''
    for k in con.execute(query):
        d = dict(k)  
        capital = d['capital_social']/100 #capital social vem multiplicado por 100
        capital = f"{capital:,.2f}".replace(',','@').replace('.',',').replace('@','.')
        listalogradouro = [k.strip() for k in [d['logradouro'].strip(), d['numero'], d['complemento'].strip(';'), d['bairro']] if k.strip()]
        logradouro = ', '.join(listalogradouro)
        d['cnpj'] = f"{d['cnpj']} - {'Matriz' if d['matriz_filial']=='1' else 'Filial'}"
        d['data_inicio_ativ'] = ajustaData(d['data_inicio_ativ'])
        d['situacao'] = f"{d['situacao']} - {gdic.dicSituacaoCadastral.get(d['situacao'],'')}"
        d['data_situacao'] = ajustaData(d['data_situacao']) 
        d['motivo_situacao'] = f"{d['motivo_situacao']}-{gdic.dicMotivoSituacao.get(int(d['motivo_situacao']),'')}"
        d['cod_nat_juridica'] = f"{d['cod_nat_juridica']}-{gdic.dicNaturezaJuridica.get(d['cod_nat_juridica'],'')}"
        d['cnae_fiscal'] = f"{d['cnae_fiscal']}-{gdic.dicCnae.get(int(d['cnae_fiscal']),'')}"
        d['porte'] = f"{d['porte']}-{gdic.dicPorteEmpresa.get(d['porte'],'')}"
        d['endereco'] = f"{d['tipo_logradouro']} {logradouro}"
        d['capital_social'] = capital 
        break #só pega primeiro
    else:
        d = None
    con = None
    #print('jsonDados-fim: ' + time.ctime())   
    return d
#.def jsonDados


def ajustaValor(valor, tipoInteiro=False):
    if not valor:
        return ''
    if tipoInteiro:
        return '{:.0f}'.format(valor)
    if valor>=10000000.0:
        v = '{:.0f}'.format(valor/1000000).replace('.',',') + ' MI'
    elif valor>=1000000.0:
        v = '{:.1f}'.format(valor/1000000).replace('.',',') + ' MI'
    elif valor>=10000.0:
        v = '{:.0f}'.format(valor/1000).replace('.',',') + ' mil'
    elif valor>=1000.0:
        v = '{:.1f}'.format(valor/1000).replace('.',',') + ' mil'
    else:
        v = '{:.2f}'.format(valor).replace('.',',')
    return v
        
def ajustaData(d): #aaaammdd
    return d[-2:]+'/' + d[4:6] + '/' + d[:4]

def dadosParaExportar(dados):    
    #print('dadosParaExportar-inicio: ' + time.ctime())
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    sids = set()
    for item in dados['no']:
        sids.add(item['id'])
    listaCpfCnpjs = list(sids)
    criaTabelasTmpParaCamadas(con, listaIds=listaCpfCnpjs, grupo='')
    querysocios = '''
                SELECT * from
				(SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
                FROM socios t
                INNER JOIN tmp_cnpjs tl ON  tl.cnpj = t.cnpj
                LEFT JOIN empresas te on te.cnpj=t.cnpj
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
                FROM socios t
                INNER JOIN tmp_cnpjs tl ON tl.cnpj = t.cnpj_cpf_socio
                LEFT JOIN empresas te on te.cnpj=t.cnpj
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, t.tipo_socio, t.cod_qualificacao
                FROM socios t
                INNER JOIN tmp_cpfnomes tn ON tn.nome= t.nome_socio AND tn.cpf=t.cnpj_cpf_socio
                LEFT JOIN empresas te on te.cnpj=t.cnpj)
                ORDER BY nome_socio
            '''

    queryempresas = '''
                SELECT *
                FROM empresas t
                INNER JOIN tmp_cnpjs tp on tp.cnpj=t.cnpj
            '''
    from io import BytesIO
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    #workbook = writer.book
    dfe=pd.read_sql_query(queryempresas, con)
    dfe['capital_social'] = dfe['capital_social'].apply(lambda capital: f"{capital/100:,.2f}".replace(',','@').replace('.',',').replace('@','.'))
    
    dfe['matriz_filial'] = dfe['matriz_filial'].apply(lambda x:'Matriz' if x=='1' else 'Filial')
    dfe['data_inicio_ativ'] = dfe['data_inicio_ativ'].apply(ajustaData)
    dfe['situacao'] = dfe['situacao'].apply(lambda x: gdic.dicSituacaoCadastral.get(x,''))
                                            
    dfe['data_situacao'] =  dfe['data_situacao'].apply(ajustaData)
    dfe['motivo_situacao'] = dfe['motivo_situacao'].apply(lambda x: x + '-' + gdic.dicMotivoSituacao.get(int(x),''))
    dfe['cod_nat_juridica'] = dfe['cod_nat_juridica'].apply(lambda x: x + '-' + gdic.dicNaturezaJuridica.get(x,''))
    dfe['cnae_fiscal'] = dfe['cnae_fiscal'].apply(lambda x: x+'-'+ gdic.dicCnae.get(int(x),''))
    
    dfe['porte'] = dfe['porte'].apply(lambda x: x+'-' + gdic.dicPorteEmpresa.get(x,''))
    
    dfe.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "Empresas", index=False)

    dfs=pd.read_sql_query(querysocios, con)
    dfs['cod_qualificacao'] =  dfs['cod_qualificacao'].apply(lambda x:x + '-' + gdic.dicQualificacao_socio.get(int(x),''))
    dfs.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "Socios", index=False)

    #dfin = pd.DataFrame(listaCpfCnpjs, columns=['cpfcnpj'])    
    dfin = pd.DataFrame.from_dict(dados['no']) #,orient='index',  columns=['id', 'descricao', 'nota', 'camada', 'cor', 'posicao', 'pinado', 'imagem', 'logradouro', 'municipio', 'uf', 'cod_nat_juridica', 'situacao_ativa', 'tipo', 'sexo'])
    dfin.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "identificadores", index=False)

    ligacoes = []
    for lig in dados['ligacao']:
        ligacoes.append([lig['origem'], lig['destino'], lig['label'], lig['tipoDescricao']])
    dflig = pd.DataFrame(ligacoes, columns=['origem','destino','ligacao','tipo_ligacao'])
    dflig.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "ligacoes", index=False)
     
    writer.close()
    output.seek(0)
    con = None
    return output

    #https://github.com/jmcarpenter2/swifter
    #dfe['data_inicio_ativ'] = dfe['data_inicio_ativ'].swifter.apply(lambda x: )
#.def dadosParaExportar

def ajustaLabelIcone(nosaux):
    nos = []
    for no in nosaux:
        prefixo =no['id'].split('_')[0]
        # no['tipo'] = prefixo
        # if prefixo=='PF':    
        #     no['label'] =  no['id'].replace('-','\n',1)[3:]
        # elif prefixo=='PJ':
        #     no['label'] =  no['id'][3:] + '\n' + no.get('descricao','')
        # elif prefixo=='EN':
        #     partes = no['id'][3:].split('-')
        #     no['label'] =  '-'.join(partes[:-2]) + '\n' + '-'.join(partes[-2:])
        # elif prefixo=='TE' or prefixo=='EM':
        #     no['label'] =  no['id'][3:]
        # else:
        #     no['label'] = no['id']
        # no['label'] = ''
        if prefixo=='PF':
            no['sexo'] = provavelSexo(no.get('id',''))
            if no['sexo']==1:
                imagem = 'icone-grafo-masculino.png'
            elif no['sexo']==2:
                imagem = 'icone-grafo-feminino.png'
            else:
                imagem = 'icone-grafo-desconhecido.png'
        elif prefixo=='EN':
            imagem = 'icone-grafo-endereco.png'
        elif prefixo=='TE':
            imagem = 'icone-grafo-telefone.png'
        elif prefixo=='EM':
            imagem = 'icone-grafo-email.png'
        elif prefixo=='PJ':
            codnat = no['cod_nat_juridica']
            if codnat.startswith('1'):
                imagem = 'icone-grafo-empresa-publica.png'
            elif codnat =='2135': #empresario individual
                imagem = 'icone-grafo-empresa-individual.png'
            elif codnat.startswith('2'):
                imagem = 'icone-grafo-empresa.png'
            elif codnat.startswith('3'):
                imagem = 'icone-grafo-empresa-fundacao.png'
            elif codnat.startswith('4'):
                imagem = 'icone-grafo-empresa-individual.png'
            elif codnat.startswith('5'):
                imagem = 'icone-grafo-empresa-estrangeira.png'  
            else:
                imagem = 'icone-grafo-empresa.png'
        else:
            imagem = 'icone-grafo-desconhecido.png' #caso genérico
        #no['imagem'] = '/rede/static/imagem/' + imagem
        no['imagem'] = imagem
        no['cor'] = ''
        if not no['camada']:
            no['cor'] = 'orange'
        no['nota'] = ''
        nos.append(copy.deepcopy(no))
    return nos 
#.def ajustaLabelIcone

def provavelSexo(nome):
    carac = nome.split(' ')[0][-1].upper()
    if carac=='O':
        sexo = 1
    elif carac=='A':
        sexo = 2
    else:
        sexo = 0
    return sexo

def numeroDeEmpresasNaBase(): #nome tem que ser completo. Com Teste, pega item randomico
    #remove acentos
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options=gEngineExecutionOptions)
    r = con.execute('select count(*) as contagem from empresas;')
    return r.fetchone()[0]

def imagensNaPastaF(bRetornaLista=False):
    dic = {}
    for item in glob.glob('static/imagem/**/*.png', recursive=True):
        if '/nao_usado/' not in item.replace("\\","/"):
            dic[os.path.split(item)[1]] = item.replace("\\","/")
    if bRetornaLista:
        return sorted(list(dic.keys()))
    else:
        return dic
        
gdicImagens = imagensNaPastaF()