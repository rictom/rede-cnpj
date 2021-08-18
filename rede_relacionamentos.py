# -*- coding: utf-8 -*-
"""
Created on set/2020
json a partir da tabela sqlite

@author: github rictom/rede-cnpj
2020-11-25 - Se uma tabela já existir, parece causar lentidão para o pandas pd.to_sql. 
Não fazer Create table ou criar índice para uma tabela a ser criada ou modificada pelo pandas 
"""
import os, sys, glob
import time, copy, re, string, unicodedata, collections, json
import pandas as pd, sqlalchemy
from fnmatch import fnmatch 
'''
from sqlalchemy.pool import StaticPool
engine = create_engine('sqlite://',
                    connect_args={'check_same_thread':False},
                    poolclass=StaticPool)
'''
import rede_config as config

try:
    caminhoDBReceita = config.config['BASE']['base_receita']
except:
    sys.exit('o arquivo sqlite não foi localizado. Veja o caminho da base no arquivo de configuracao rede.ini está correto.')
if not caminhoDBReceita: #se não houver db da receita, carrega um template para evitar erros nas consultas
    caminhoDBReceita = 'base_cnpj_vazia.db'
caminhoDBReceitaFTS = config.config['BASE'].get('base_receita_fulltext','')
caminhoDBEnderecoNormalizado = config.config['BASE'].get('base_endereco_normalizado', '')
caminhoDBLinks = config.config['BASE'].get('base_links', '')

caminhoDBBaseLocal =  config.config['BASE'].get('base_local', '')

#logAtivo = True if config['rede']['logAtivo']=='1' else False #registra cnpjs consultados
logAtivo = config.config['ETC'].getboolean('logativo',False) #registra cnpjs consultados
#    ligacaoSocioFilial = True if config['rede']['ligacaoSocioFilial']=='1' else False #registra cnpjs consultados
ligacaoSocioFilial = config.config['ETC'].getboolean('ligacao_socio_filial',False) #registra cnpjs consultados

gEngineExecutionOptions = {"sqlite_raw_colnames": True, 'pool_size':1} #poll_size=1 força usar só uma conexão??

class DicionariosCodigosCNPJ():
    def __init__(self):
        if not caminhoDBReceita:
            return
        con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
        #dfaux = pd.read_csv(r"tabelas/tabela-de-qualificacao-do-socio-representante.csv", sep=';')
        dfaux = pd.read_sql_table('qualificacao_socio', con, index_col=None )
        self.dicQualificacao_socio = pd.Series(dfaux.descricao.values,index=dfaux.codigo).to_dict()
        #dfaux = pd.read_csv(r"tabelas/DominiosMotivoSituaoCadastral.csv", sep=';', encoding='latin1', dtype=str)
        dfaux = pd.read_sql_table('motivo', con, index_col=None )
        self.dicMotivoSituacao = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        #dfaux = pd.read_excel(r"tabelas/cnae.xlsx", sheet_name='codigo-grupo-classe-descr')
        dfaux = pd.read_sql_table('cnae', con, index_col=None )
        self.dicCnae = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        #dfaux = pd.read_csv(r"tabelas/natureza_juridica.csv", sep=';', encoding='latin1', dtype=str)
        dfaux = pd.read_sql_table('natureza_juridica', con, index_col=None )
        self.dicNaturezaJuridica = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        
        self.dicSituacaoCadastral = {'01':'Nula', '02':'Ativa', '03':'Suspensa', '04':'Inapta', '08':'Baixada'}
        #self.dicSituacaoCadastral = {'1':'Nula', '2':'Ativa', '3':'Suspensa', '4':'Inapta', '8':'Baixada'}
        self.dicPorteEmpresa = {'00':'Não informado', '01':'Micro empresa', '03':'Empresa de pequeno porte', '05':'Demais (Médio ou Grande porte)'}


gdic = DicionariosCodigosCNPJ()

dfaux=None

gTableIndex = 0
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
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    #con.execute('DROP TABLE if exists tmp_cnpjs')
    con.execute('DROP TABLE if exists tmp_cpfpjnomes')
    con.execute('DROP TABLE if exists tmp_ids')
    con.execute('DROP TABLE if exists tmp_socios')
    con.execute('DROP TABLE if exists tmp_busca_nome')
    con = None

apagaTabelasTemporarias() #apaga quando abrir o módulo

def buscaPorNome(nomeIn, limite=10): #nome tem que ser completo. Com Teste, pega item randomico
    '''caminhoDBReceitaFTS base com indice full text search, fica rápido com match mas com = fila lento, por isso
        precisa fazer consulta em caminhoDBReceita quando não for usar match
    '''
    #remove acentos
    #se limite==-1, não havia @N no nome
    nomeIn = nomeIn.strip().upper()
    nomeMatch = ''
    try:
        limite = int(limite)
    except:
        limite = 0

    if (not( ('*' in nomeIn) or ('?' in nomeIn) or ('"' in nomeIn))) and limite>0: # se tinha arroba mas sem caractere curinga, acrescenta *
        nomeIn = '*' + nomeIn + '*'
    limite =  min(limite,100) if limite else 10
    #print('limite', limite, nomeIn)
    if ('*' in nomeIn) or ('?' in nomeIn) or ('"' in nomeIn):
        nomeMatchInicial = nomeIn.strip()
        nomeMatch = nomeMatchInicial
        nomeMatchInicial = nomeMatchInicial.replace('"','') #para usar com fnmatch
        if nomeMatch.startswith('*'): #match do sqlite não aceita * no começo
            nomeMatch = nomeMatch[1:].strip()
        if '?' in nomeMatch: #? não é aceito em match do sqlite, mas pode ser usado no fnmatch
            nomeMatch = nomeMatch.replace('?', '*')
        if caminhoDBReceitaFTS:
            confts = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceitaFTS}", execution_options=gEngineExecutionOptions)
        nomeMatch = ''.join(x for x in unicodedata.normalize('NFKD', nomeMatch) if x in string.printable).upper()
        #nomeMatch = re.sub(r'[^a-zA-Z0-9_ *""]', '', nomeMatch)
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)


    nome = ''.join(x for x in unicodedata.normalize('NFKD', nomeIn) if x in string.printable).upper()
    #nome = re.sub(r'[^a-zA-Z0-9_ *""]', '', nome)
    cjs, cps = set(), set()

    cursor = []
    if nomeMatch:
         if not caminhoDBReceitaFTS: #como não há tabela, não faz consulta por match
             #con = None
             return set(), set()
         queryfts = f'''
                SELECT DISTINCT  nome_socio as nome
                FROM socios_search 
                where nome_socio match :nomeMatch
                limit {limite*20}
            ''' 
         df_busca_nomesPF = pd.read_sql(queryfts, confts, index_col=None, params={'nomeMatch':nomeMatch})
         df_busca_nomesPF.to_sql('tmp_busca_nomePF', con, if_exists='replace', index=None)
         query = f'''
                    SELECT distinct cnpj_cpf_socio, nome_socio
                    from tmp_busca_nomePF tn 
                    left join socios ts on tn.nome=ts.nome_socio
                    where cnpj_cpf_socio not null and nome_socio<>"" and length(cnpj_cpf_socio)=11
                    limit {limite*2}       
         '''
         cursor = con.execute(query)
        #obs 26/4/2021, a rigor não seria necessário length(cnpj_cpf_socio)=11, o problema é que a base está com erro no nome de sócios em que são empresas
    elif nomeIn=='TESTE':
        query = 'select cnpj_cpf_socio, nome_socio from socios where rowid > (abs(random()) % (select (select max(rowid) from socios)+1)) LIMIT 1;'
        cursor = con.execute(query)
    else:
        query = f'''
                SELECT distinct cnpj_cpf_socio, nome_socio
                FROM socios
                where nome_socio=:nome
                limit {limite}
            '''
        cursor = con.execute(query, {'nome':nome})
    #nomeMatch = nomeMatch.replace('"','')
    # print('query', query)
    contagemRegistros = 0
    for r in cursor: #con.execute(query):
        if contagemRegistros>=limite:
            break
        if nomeMatch:
            if not fnmatch(r.nome_socio.strip(), nomeMatchInicial):
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
    #pega cnpjs
    cursor = []
    if nomeMatch:
        queryfts = f'''
                SELECT DISTINCT  razao_social as nome
                FROM empresas_search
                where razao_social match :nomeMatch
                limit {limite*20}
            ''' 
        df_busca_nomesPJ = pd.read_sql(queryfts, confts, index_col=None, params={'nomeMatch':nomeMatch})
        df_busca_nomesPJ.to_sql('tmp_busca_nomePJ', con, if_exists='replace', index=None)
        query = f'''
                    SELECT te.cnpj, t.razao_social
                    from tmp_busca_nomePJ tn
                    inner join empresas t on tn.nome = t.razao_social
                    left join estabelecimento te on te.cnpj_basico=t.cnpj_basico --inner join fica lento??
                    limit {limite*2}
            '''         
        cursor = con.execute(query)
    else:
        # pra fazer busca por razao_social, a coluna deve estar indexada
        query = f'''
                    SELECT te.cnpj, razao_social
                    FROM empresas t
                    inner join estabelecimento te on te.cnpj_basico=t.cnpj_basico
                    where t.razao_social=:nome 
                    limit {limite}
                '''        
        cursor = con.execute(query, {'nome':nome})
    for r in cursor: #con.execute(query):
        if contagemRegistros>=limite:
            break
        if nomeMatch:
            if not fnmatch(r.razao_social.strip(), nomeMatchInicial): #strip porque tem espaço no começo da razão social
                continue
        cjs.add(r.cnpj)    
        contagemRegistros +=1
    con = None
    return cjs, cps
#.def buscaPorNome(

def busca_cnpj(cnpj_basico, limiteIn):
    kLimiteFiliais = 200
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    try:
        limite = int(limiteIn)
    except ValueError:
        limite = 0
    limite = min(limite, 100)
    if limite>0: #limita a quantidade de filias
        query = f'''
                SELECT te.cnpj
                FROM empresas t
                inner join estabelecimento te on te.cnpj_basico=t.cnpj_basico
                where t.cnpj_basico=\'{cnpj_basico}\'
                order by te.matriz_filial, te.cnpj_ordem
                limit {limite+1}
            ''' 

    elif limite<0: #mostra todos as filiais e matriz
        #limitando a kLimiteFiliais registros, quando tenta exibir todos os do BB pode travar o script
        query = f'''
                SELECT te.cnpj
                FROM empresas t
                left join estabelecimento te on te.cnpj_basico=t.cnpj_basico
                where t.cnpj_basico=\'{cnpj_basico}\'
                order by te.matriz_filial, te.cnpj_ordem
                limit {kLimiteFiliais} 
            ''' 
    else: #sem limite definido, só matriz
        query = f'''
                    SELECT te.cnpj
                    FROM empresas t
                    inner join estabelecimento te on te.cnpj_basico=t.cnpj_basico
                    where t.cnpj_basico=\'{cnpj_basico}\' and te.matriz_filial is '1' 
                ''' 
    r = con.execute(query).fetchall()
    return {k[0] for k in r}

    
def busca_cpf(cpfin):
    '''como a base não tem cpfs de sócios completos, faz busca só do miolo'''
    cpf = '***' + cpfin[3:9] + '**'
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    query = f'''
                SELECT distinct cnpj_cpf_socio, nome_socio
                FROM socios
                where cnpj_cpf_socio=\'{cpf}\'
                limit 100
            '''
    lista = []
    for c, n in con.execute(query).fetchall():
        lista.append((c,n))
    return lista
            
def separaEntrada(cpfcnpjIn='', listaIds=None):
    cnpjs = set()
    cpfnomes = set()
    outrosIdentificadores = set() #outros identificadores, com EN_ (supondo dois caracteres mais underscore) 
    if cpfcnpjIn:
        lista = cpfcnpjIn.split(';')
        lista = [i.strip() for i in lista if i.strip()]
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
        elif i.startswith('PE_'):
            cpfcnpjnome = i[3:]
            nome = cpfcnpjnome
            cpf = ''
            cpfnomes.add((cpf,nome))  
        elif len(i)>3 and i[2]=='_':
            outrosIdentificadores.add(i)
        else:
            limite = 0
            if kCaractereSeparadorLimite in i:
                i, limite = kCaractereSeparadorLimite.join(i.split(kCaractereSeparadorLimite)[0:-1]).strip(), i.split(kCaractereSeparadorLimite)[-1]
                if not limite:
                    limite=-1
            soDigitos = ''.join(re.findall('\d', str(i)))
            if len(soDigitos)==14:
                cnpjs.add(soDigitos)
            elif len(soDigitos)==8:
                scnpj_aux = busca_cnpj(soDigitos, limite)
                if scnpj_aux:
                    cnpjs.update(scnpj_aux)
            elif len(soDigitos)==11:
                lcpfs = busca_cpf(soDigitos)
                if lcpfs:
                    cpfnomes.update(set(lcpfs))
            elif re.search('\*\*\*\d\d\d\d\d\d\*\*',str(i)):
                lcpfs = set(busca_cpf(str(i)))
                if lcpfs:
                    cpfnomes.update(set(lcpfs))
                pass #fazer verificação por CPF??
            elif not soDigitos and i.strip():
                cnpjsaux, cpfnomesaux = buscaPorNome(i, limite=limite)
                if cnpjsaux:
                    cnpjs.update(cnpjsaux)
                if cpfnomesaux:
                    cpfnomes.update(cpfnomesaux)  
    cpfpjnomes = copy.deepcopy(cpfnomes)
    for c in cnpjs:
        cpfpjnomes.add((c,''))
    return cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes
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

dtype_tmp_cpfpjnomes={'cpfpj':sqlalchemy.types.VARCHAR,
                       'nome':sqlalchemy.types.VARCHAR,
                       'grupo':sqlalchemy.types.VARCHAR,
                       'camada':sqlalchemy.types.INTEGER }

def criaTabelasTmpParaCamadas(con, cpfcnpjIn='', listaIds=None, grupo=''):
    global gTable

    apagaTabelasTemporarias()

    if cpfcnpjIn:
        cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes = separaEntrada(cpfcnpjIn=cpfcnpjIn)
    else:
        cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes = separaEntrada(listaIds=listaIds)
    camadasIds = {}
    
    ids = set(['PJ_'+c for c in cnpjs])
    ids.update(set(['PF_'+cpf+'-'+nome for cpf,nome in cpfnomes if cpf]))
    ids.update(set(['PE_'+nome for cpf,nome in cpfnomes if not cpf]))
    ids.update(outrosIdentificadores)

    dftmptable = pd.DataFrame({'identificador' : list(ids)})
    dftmptable['camada'] = 0
    dftmptable['grupo'] = grupo

    dftmptable.to_sql('tmp_ids', con=con, if_exists='replace', index=False, dtype=dtype_tmp_ids)
    #indice deixa a busca lenta!
    #con.execute('CREATE INDEX ix_tmp_ids_index ON tmp_ids ("identificador")')
    camadasIds = {i:0 for i in ids}

    for outros in outrosIdentificadores:
        camadasIds[outros]=0   
    
    dftmptable = pd.DataFrame(list(cpfpjnomes), columns=['cpfpj', 'nome'])
    dftmptable['grupo'] = grupo
    dftmptable['camada'] = 0
    #con.execute('DELETE FROM tmp_cpfnomes')
    dftmptable.to_sql('tmp_cpfpjnomes', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cpfpjnomes)       
    
    return camadasIds, cnpjs, cpfnomes #, ids
#.def criaTabelasTmpParaCamadas

def cnpj2id(cnpj):
    return 'PJ_' + cnpj

def cpfnome2id(cpf,nome):
    if cpf!='':
        return 'PF_'+cpf+'-'+nome
    else:
        return 'PE_'+nome

def id2cpfnome(id):
    if id.startswith('PF_'):
        return id[3:14], id[15:]
    if id.startswith('PE_'):
        return '', id[3:]
    
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
    #con=sqlite3.connect(caminhoDBReceita)
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
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


    dicRazaoSocial = {} #excepcional, se um cnpj que é sócio na tabela de socios não tem cadastro na tabela empresas
                
    for cam in range(camada):  
        whereMatriz = ''
        if bjson and not ligacaoSocioFilial:
            if cam==-1:
                whereMatriz = ''
            else:
                # verificar, talvez não esteja correto, precisa ver a camada da filial
                # whereMatriz = '''
                #  WHERE substr(t.cnpj,9,4)="0001" 
                # '''
                whereMatriz = '''
                 AND substr(t.cnpj,9,4)="0001" 
                '''
                #AND (length(cnpj_cpf_socio)<>14 OR substr(cnpj_cpf_socio, 9, 4)="0001")

        query = f''' 
        DROP TABLE if exists tmp_socios;
        
        CREATE TABLE tmp_socios AS
        SELECT DISTINCT 
        * From (
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
        FROM socios t
        INNER JOIN tmp_cpfpjnomes tl ON  tl.cpfpj = t.cnpj
        left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
        where tl.nome=''
        UNION
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
        FROM socios t
        INNER JOIN tmp_cpfpjnomes tl ON tl.cpfpj = t.cnpj_cpf_socio
        left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio 
        where tl.nome=''
        {whereMatriz}
        UNION
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
        FROM socios t
        INNER JOIN tmp_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
        left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
        where tn.nome<>''
         {whereMatriz}
        ) as taux 
        ; 
        
        
        Insert INTO tmp_socios (cnpj, cnpj_cpf_socio, nome_socio, cod_qualificacao) 
        select  tm.cnpj, tp.cpfpj as cnpj_cpf_socio, "" as nome_socio, "filial" as cod_qualificacao
        from estabelecimento t
        inner join tmp_cpfpjnomes tp on tp.cpfpj=t.cnpj
        left join estabelecimento tm on tm.cnpj_basico=t.cnpj_basico and tm.cnpj<>tp.cpfpj
        where tm.matriz_filial is "1" --is é mais rapido que igual (igual é muito lento)
        and tp.nome='';
        
        
        Insert INTO tmp_cpfpjnomes (cpfpj, nome, grupo, camada) 
        select distinct ts.cnpj as cpfpj, "" as nome, "{grupo}" as grupo, {cam+1} as camada
        From tmp_socios ts;                  
        
        Insert INTO tmp_cpfpjnomes (cpfpj, nome, grupo, camada)
        select distinct cnpj_cpf_socio as cpfpj,"" as nome, "{grupo}" as grupo, {cam+1} as camada
        From tmp_socios ts
        where length(cnpj_cpf_socio)=14;
        
        Insert INTO tmp_cpfpjnomes (cpfpj, nome, grupo, camada)
        select distinct cnpj_cpf_socio as cpfpj, nome_socio as nome, "{grupo}" as grupo, {cam+1} as camada
        From tmp_socios ts
        where  length(cnpj_cpf_socio)<>14;

        drop table if exists tmp_cpfpjnomes_aux;
        
        create table tmp_cpfpjnomes_aux AS
        select cpfpj, nome, min(grupo) as grupo, min(camada) as camada
        from tmp_cpfpjnomes
        group by cpfpj, nome;
        
        drop table if exists tmp_cpfpjnomes;
        
        create table tmp_cpfpjnomes AS
        select *
        from tmp_cpfpjnomes_aux;        
        
        drop table if exists tmp_cpfpjnomes_aux;

                    
        Insert INTO tmp_ids (identificador, grupo, camada)
        select distinct "PJ_" || t.cpfpj as identificador,  t.grupo, t.camada
        From tmp_cpfpjnomes t
        where t.nome='';
        
        Insert INTO tmp_ids (identificador, grupo, camada)
        select distinct "PF_" || t.cpfpj || "-" || t.nome  as identificador, t.grupo, t.camada
        From tmp_cpfpjnomes t
        where t.cpfpj<>"" and t.nome<>'';
        
        Insert INTO tmp_ids (identificador, grupo, camada)
        select distinct "PE_" || t.nome  as identificador, t.grupo, t.camada
        From tmp_cpfpjnomes t
        where t.cpfpj = "" and t.nome<>'';
        
        drop table if exists tmp_ids_aux;
        
        create table tmp_ids_aux AS
        select identificador, min(grupo) as grupo, min(camada) as camada
        from tmp_ids
        group by identificador;
        
        drop table if exists tmp_ids;
        
        create table tmp_ids AS
        select *
        from tmp_ids_aux;
        
        drop table if exists tmp_ids_aux;
        '''

        for sql in query.split(';'):
            con.execute(sql)
    #.for cam in range(camada): 
    if camada==0:
        #gambiarra, em camada 0, não apaga a tabela tmp_socios, por isso pega dados de consulta anterior.
        query0 = ''' 
        CREATE TABLE tmp_socios AS
        SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
        FROM socios t
        left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
        limit 0
        '''
        con.execute(query0)
 
    
    queryLertmp = '''
        Select *
        from tmp_ids
        where substr(identificador,1,3)='PF_' or substr(identificador,1,3)='PE_'
    '''
    for k in con.execute(queryLertmp):
        kid = k['identificador']
        if kid[:3]=='PF_':
            _, descricao = id2cpfnome(kid) #kid[15:] 
        else: #'PE_'
            descricao = '(EMPRESA SÓCIA NO EXTERIOR)'
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
                   "label":k['cod_qualificacao']} #gdic.dicQualificacao_socio.get(int(k['cod_qualificacao']),'').strip()}
        ligacoes.append(copy.deepcopy(ligacao))
    if logAtivo or not bjson:
        conlog = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
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
    for k in con.execute('Select cpfpj as cnpj, camada from tmp_cpfpjnomes where nome="" '):
        kcnpj = k['cnpj']
        cnpjs.add(kcnpj)
        #camadasIds[kcnpj] = k['camada']
        camadasIds[cnpj2id(kcnpj)] = k['camada']

    #nos = dadosDosNosCNPJs(con=con, cnpjs=cnpjs, nosaux=nosaux, dicRazaoSocial=dicRazaoSocial, camadasIds=camadasIds)
    dadosDosNosCNPJs(con=con, cnpjs=cnpjs, nosaux=nosaux, dicRazaoSocial=dicRazaoSocial, camadasIds=camadasIds)
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
        for tipo in ['endereco','base_local']: #endereço ou base_local só pega 1 camada.
            if tipo=='endereco':
                if not caminhoDBEnderecoNormalizado:
                    continue
            elif tipo=='base_local':
                 if not caminhoDBBaseLocal:
                    continue               
            jsonEnderecosBanco = camadaLink(cpfcnpjIn='', conCNPJ=con, camada=1 if tipo=='endereco' else camada,  grupo=grupo,  listaIds=ids, tipoLink=tipo)
            for item in jsonEnderecosBanco['no']:
                if item['id'] not in camadasIds:
                    nosaux.append(item)
            ligacoes.extend(jsonEnderecosBanco['ligacao'])
    #print(' jsonenderecos-fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    dadosDosNosBaseLocal(nosaux, camadasIds)
    nosaux=ajustaLabelIcone(nosaux)
    textoJson={'no': nosaux, 'ligacao':ligacoes, 'mensagem':mensagem} 
    con = None
    #print(listaIds)
    #print(textoJson)
    #print(' fim: ' + time.ctime())
    #print(' fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    return textoJson
#.def camadasRede

def dadosDosNosBaseLocal(nosInOut, camadasIds):
    if not caminhoDBBaseLocal:
        return 
    dicDados = jsonBaseLocal(listaIds=list(camadasIds)) 
    nosaux = []
    for n in nosInOut: 
        if n['id'] in dicDados:
            daux = copy.deepcopy(dicDados[n['id']])
            for k,v in daux.items():
                n[k] = v
        nosaux.append(copy.deepcopy(n))
    nosInOut = nosaux
#.def dadosDosNosBaseLocal
            
def dadosDosNosCNPJs(con, cnpjs, nosaux, dicRazaoSocial, camadasIds):
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable['grupo'] = ''
    dftmptable['camada'] = 0
    #con.execute('DELETE FROM tmp_cnpjs')
    dftmptable.to_sql('tmp_cnpjsdados', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cnpjs)
    query = '''
                SELECT tt.cnpj, te.razao_social, tt.situacao_cadastral as situacao, tt.matriz_filial,
                tt.tipo_logradouro, tt.logradouro, tt.numero, tt.complemento, tt.bairro,
                ifnull(tm.descricao,tt.nome_cidade_exterior) as municipio, tt.uf as uf, tpais.descricao as pais_,
                te.natureza_juridica as cod_nat_juridica
                from tmp_cnpjsdados tp
                inner join estabelecimento tt on tt.cnpj = tp.cnpj
                left join empresas te on te.cnpj_basico = tt.cnpj_basico --trocar por inner join deixa a consulta lenta...
                left join municipio tm on tm.codigo=tt.municipio
                left join pais tpais on tpais.codigo=tt.pais
            ''' #pode haver empresas fora da base de teste
    setCNPJsRecuperados = set()
    for k in con.execute(query):
        listalogradouro = [j.strip() for j in [k['logradouro'].strip(), k['numero'], k['complemento'].strip(';'), k['bairro']] if j.strip()]
        logradouro = ', '.join(listalogradouro)
        no = {'id': cnpj2id(k['cnpj']), 'descricao': k['razao_social'], 
              'camada': camadasIds[cnpj2id(k['cnpj'])], 'tipo':0, 'situacao_ativa': int(k['situacao'])==2,
              'logradouro': f'''{k['tipo_logradouro']} {logradouro}''',
              'municipio': k['municipio'], 'uf': k['pais_'] if k['uf']=='EX' else k['uf'], 
              'cod_nat_juridica':k['cod_nat_juridica']
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
    #nosaux=ajustaLabelIcone(nosaux)
    nos = nosaux #nosaux[::-1] #inverte, assim os nos de camada menor serao inseridas depois, ficando na frente
    nosaux = nos.sort(key=lambda n: n['camada'], reverse=True) #inverte ordem, porque os últimos icones vão aparecer na frente. Talvez na prática não seja útil.   
    con.execute('DROP TABLE if exists tmp_cnpjsdados ') 
    #return nos
#.def dadosDosNosCNPJs

@timeit
def camadaLink(cpfcnpjIn='', conCNPJ=None, camada=1, numeroItens=15, 
               valorMinimo=0, valorMaximo=0, grupo='', bjson=True, 
               listaIds=None, tipoLink='link'):    
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
    elif tipoLink=='endereco' or tipoLink=='base_local':
        if tipoLink=='endereco':
            con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBEnderecoNormalizado}", execution_options=gEngineExecutionOptions)        
            tabela = 'link_ete'
        else:
            con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}", execution_options=gEngineExecutionOptions)
            tabela = 'links'       

        valorMinimo=0
        valorMaximo=0
        numeroItens=0
        bValorInteiro = True

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
        return {'no': [], 'ligacao':[], 'mensagem':'erro, tipoLink indefinido'} 
        
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
            #     print('repetiu ligacao', orig_destAnt)
            # orig_destAnt = ('PJ_'+k['cnpj'], destino)
            if (k['id1'], k['id2']) not in setLigacoes: #cam+1==camada and bjson: #só pega dados na última camada
                # ligacao = {"origem":k['id1'], "destino":k['id2'], 
                #            "cor": "silver" if  tipoLink=='endereco' else "gold", #"cor":"gray", 
                #            "camada":cam+1, "tipoDescricao":'link',"label":k['descricao'] + ':' + ajustaValor(k['valor'], bValorInteiro)}
                ligacao = {"origem":k['id1'], "destino":k['id2'], 
                           "cor": "silver" if  tipoLink=='endereco' else "gold", #"cor":"gray", 
                           "camada":cam+1, "tipoDescricao":'link'} #"label":k['descricao'] + ':' + ajustaValor(k['valor'], bValorInteiro)}


                if tipoLink=='base_local':
                    ligacao['label'] = k['descricao'] 
                else:
                    ligacao['label'] = k['descricao'] + ':' + ajustaValor(k['valor'], bValorInteiro)
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

    for c in camadasIds:
        if c.startswith('PJ_'):
            cnpjs.add(c[3:])
        # else:
        #     if c.startswith('PF_'):
        #         nome = c[15:] #supõe 'PF_12345678901-nome'
        #         no = {'id': c, 'descricao':nome, 
        #                 'camada': camadasIds[c], 
        #                 'situacao_ativa': True, 
        #                 'logradouro':'',
        #                 'municipio': '', 'uf': ''} 
        #     # elif c.startswith('ID_'):
        #     #     
        #     else: #elif c.startswith('EN_'):
        #         no = {'id': c, 'descricao':'', 
        #                 'camada': camadasIds[c], 
        #                 'situacao_ativa': True, 
        #                 'logradouro':'',
        #                 'municipio': '', 'uf': ''} 
        #     nosaux.append(copy.deepcopy(no))

    if conCNPJ:
        conCNPJaux =conCNPJ
    else:
        conCNPJaux = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    cnpjs = cnpjs.difference(cnpjsInicial)
    #nos = dadosDosNosCNPJs(conCNPJaux, cnpjs, nosaux, dicRazaoSocial, camadasIds)
    dadosDosNosCNPJs(conCNPJaux, cnpjs, nosaux, dicRazaoSocial, camadasIds)
    for c in camadasIds:
        if c.startswith('PJ_'):
            continue
        if c.startswith('PF_'):
            nome = c[15:] #supõe 'PF_12345678901-nome'
            no = {'id': c, 'descricao':nome, 
                    'camada': camadasIds[c], 
                    'situacao_ativa': True, 
                    'logradouro':'',
                    'municipio': '', 'uf': ''} 
        # elif c.startswith('ID_'):
        #      
        else: #elif c.startswith('EN_'):
            no = {'id': c, 'descricao':'', 
                    'camada': camadasIds[c], 
                    'situacao_ativa': True, 
                    'logradouro':'',
                    'municipio': '', 'uf': ''} 
        nosaux.append(copy.deepcopy(no))    
    dadosDosNosBaseLocal(nosaux, camadasIds)
    nosaux=ajustaLabelIcone(nosaux)
    textoJson={'no': nosaux, 'ligacao':ligacoes, 'mensagem':mensagem} 
    con = None
    if conCNPJ:
        conCNPJaux = None
    #print(' fim: ' + time.ctime())
    #print('camadaLink fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    return textoJson
#.def camadaLink

def apagaLog():
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    con.execute('DROP TABLE IF EXISTS log_cnpjs;')
    con.execute('DROP TABLE IF EXISTS log_cpfnomes;')
    con = None
                
def jsonDados(cpfcnpjIn, listaIds=False):
    #print('INICIANDO-------------------------')
    #dados de cnpj para popup de Dados
    #print('jsonDados-inicio: ' + time.ctime())
    cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes = separaEntrada(cpfcnpjIn)    
    if outrosIdentificadores:
        return jsonBaseLocal(cpfcnpjIn=cpfcnpjIn, listaIds=listaIds)
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable['grupo']=''
    dftmptable['camada']=0

    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}",execution_options=gEngineExecutionOptions)
    dftmptable.to_sql('tmp_cnpjs1', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)

    query = '''
        select t.*, te.*, ifnull(tm.descricao,t.nome_cidade_exterior) as municipio_texto, tpais.descricao as pais_, tsimples.opcao_mei
        from estabelecimento t
        inner join tmp_cnpjs1 tp on tp.cnpj=t.cnpj
        left join empresas te on te.cnpj_basico=t.cnpj_basico
        left join municipio tm on tm.codigo=t.municipio
        left join simples tsimples on tsimples.cnpj_basico=t.cnpj_basico
        left join pais tpais on tpais.codigo=t.pais
        
            '''
    dlista = []
    for k in con.execute(query):
        d = dict(k)  
        
        capital = d['capital_social'] #capital social vem multiplicado por 100
        capital = f"{capital:,.2f}".replace(',','@').replace('.',',').replace('@','.')
        listalogradouro = [k.strip() for k in [d['logradouro'].strip(), d['numero'], d['complemento'].strip(';'), d['bairro']] if k.strip()]
        logradouro = ', '.join(listalogradouro)
        #d['cnpj'] = f"{d['cnpj']} - {'Matriz' if d['matriz_filial']=='1' else 'Filial'}"       
        d['matriz_filial'] = 'Matriz' if d['matriz_filial']=='1' else 'Filial'
        d['data_inicio_atividades'] = ajustaData(d['data_inicio_atividades'])
        d['situacao_cadastral'] = f"{d['situacao_cadastral']} - {gdic.dicSituacaoCadastral.get(d['situacao_cadastral'],'')}"
        d['data_situacao_cadastral'] = ajustaData(d['data_situacao_cadastral']) 
        if d['motivo_situacao_cadastral']=='0':
            d['motivo_situacao_cadastral'] = ''
        else:
            d['motivo_situacao_cadastral'] = f"{d['motivo_situacao_cadastral']}-{gdic.dicMotivoSituacao.get(d['motivo_situacao_cadastral'],'')}"
        d['natureza_juridica'] = f"{d['natureza_juridica']}-{gdic.dicNaturezaJuridica.get(d['natureza_juridica'],'')}"
        #d['cnae_fiscal'] = f"{d['cnae_fiscal']}-{gdic.dicCnae.get(int(d['cnae_fiscal']),'')}"
        d['cnae_fiscal'] = f"{d['cnae_fiscal']}-{gdic.dicCnae.get(d['cnae_fiscal'],'')}"
        d['porte_empresa'] = f"{d['porte_empresa']}-{gdic.dicPorteEmpresa.get(d['porte_empresa'],'')}"
        d['endereco'] = f"{d['tipo_logradouro']} {logradouro}"
        d['capital_social'] = capital 
        d['municipio'] = d['municipio_texto']
        d['opcao_mei'] = d['opcao_mei'] if  d['opcao_mei']  else ''
        d['uf'] = d['pais_'] if d['uf']=='EX' else d['uf']
        if not listaIds:
            break #só pega primeiro
        dlista.append(copy.deepcopy(d))
    else:
        d = None
    con = None
    #print('jsonDados-fim: ' + time.ctime())   
    if not listaIds:
        return d
    else:
        return dlista
#.def jsonDados

def jsonBaseLocal(cpfcnpjIn=None, listaIds=None):    
    if not caminhoDBBaseLocal:
        return
    if not listaIds and not cpfcnpjIn:
        return
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}",execution_options=gEngineExecutionOptions)
    if not listaIds:
        #dftmptable = pd.DataFrame({'id' : list(cpfcnpjIn.upper().split(';'))})
        dftmptable = pd.DataFrame({'id' : list(cpfcnpjIn.split(';'))})
    else:
        dftmptable = pd.DataFrame({'id' : list(listaIds)})

    dftmptable.to_sql('tmp_idsj', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)

    query = '''
        select tj.id, tj.json
        from tmp_idsj t
        inner join dadosjson tj on tj.id = t.id
            '''
    dlista = []
    dicLista = {}
    for k in con.execute(query):
        try:
            #d = {k['id']:json.loads(k['json'])} #dict(k)  
            d = json.loads(k['json']) #dict(k)  
        except:
            d = {k['id']:'erro na base'}
        if not listaIds:
            break #só pega primeiro
        dlista.append(copy.deepcopy(d))
        dicLista[k['id']]=copy.deepcopy(d)
    else:
        d = None
    con.execute('Drop table if exists tmp_idsj')
    con = None
    #print('jsonDados-fim: ' + time.ctime())   
    if not listaIds:
        return d
    else:
        return dicLista #dlista
#.def jsonBaseLocal

def carregaJSONemBaseLocal(nosLigacoes, comentario=''):
    if not caminhoDBBaseLocal:
        return
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}",execution_options=gEngineExecutionOptions)
    
    listaNo = []
    for dados in nosLigacoes['no']:
        dic = {key:valor for key,valor in dados.items() if key!='id'}
        #print(dic)
        try:
            texto = json.dumps(dic, default=str)
            dadosid = dados['id'].upper()
            listaNo.append([dadosid, texto, comentario])
        except:
            print('erro...', dic)
       
    listaLigacao = []
    for lig in nosLigacoes['ligacao']:
        #dic = {key:valor for key,valor in dados if key!='id'}
        try:
            listaLigacao.append([lig['origem'].upper(), lig['destino'].upper(), lig.get('tipoDescricao','')+':'+ lig.get('label',''),'', comentario])      
        except:
            print('erro...', lig)
  
    dftmptable = pd.DataFrame(listaNo, columns = ['id', 'json', 'comentario'])

    dftmptable.to_sql('dadosjson', con=con, if_exists='append', index=False, dtype=sqlalchemy.types.VARCHAR)

    dftmptable = pd.DataFrame(listaLigacao, columns = ['id1', 'id2', 'descricao','valor', 'comentario'])

    dftmptable.to_sql('links', con=con, if_exists='append', index=False, dtype=sqlalchemy.types.VARCHAR)

    con = None

#.def carregaJSONemBaseLocal

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
    if d:
        return d[-2:]+'/' + d[4:6] + '/' + d[:4]
    else:
        return ''

def dadosParaExportar(dados):    
    #print('dadosParaExportar-inicio: ' + time.ctime())
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    sids = set()
    for item in dados['no']:
        sids.add(item['id'])
    listaCpfCnpjs = list(sids)
    criaTabelasTmpParaCamadas(con, listaIds=listaCpfCnpjs, grupo='')
    querysocios = '''
                SELECT * from
				(SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
                FROM socios t
                --INNER JOIN tmp_cnpjs tl ON  tl.cnpj = t.cnpj
                INNER JOIN tmp_cpfpjnomes tl ON  tl.cpfpj = t.cnpj
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                where tl.nome=''
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
                FROM socios t
                INNER JOIN tmp_cpfpjnomes tl ON tl.cpfpj = t.cnpj_cpf_socio
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                where tl.nome=''
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
                FROM socios t
                INNER JOIN tmp_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                where tn.nome<>''
            )
                ORDER BY nome_socio
            '''

    queryempresas = '''
                SELECT te.*, tm.descricao as municipio_, tt.uf as uf_, pais.descricao as pais_, tt.*
                FROM tmp_cpfpjnomes tp 
                left join estabelecimento tt on tt.cnpj=tp.cpfpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                left join municipio tm on tm.codigo=tt.municipio
                left join pais on pais.codigo=tt.pais
                where tp.nome=''
            '''
    from io import BytesIO
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    #workbook = writer.book
    dfe=pd.read_sql_query(queryempresas, con)
    dfe['capital_social'] = dfe['capital_social'].apply(lambda capital: f"{capital/100:,.2f}".replace(',','@').replace('.',',').replace('@','.'))
    
    dfe['matriz_filial'] = dfe['matriz_filial'].apply(lambda x:'Matriz' if x=='1' else 'Filial')
    dfe['data_inicio_atividades'] = dfe['data_inicio_atividades'].apply(ajustaData)
    dfe['situacao_cadastral'] = dfe['situacao_cadastral'].apply(lambda x: gdic.dicSituacaoCadastral.get(x,'') if x else '')
                                            
    dfe['data_situacao_cadastral'] =  dfe['data_situacao_cadastral'].apply(ajustaData)
    dfe['motivo_situacao_cadastral'] = dfe['motivo_situacao_cadastral'].apply(lambda x: x + '-' + gdic.dicMotivoSituacao.get(x,'') if x else '')
    dfe['natureza_juridica'] = dfe['natureza_juridica'].apply(lambda x: x + '-' + gdic.dicNaturezaJuridica.get(x,'') if x else 11)
    #dfe['cnae_fiscal'] = dfe['cnae_fiscal'].apply(lambda x: x +'-'+ gdic.dicCnae.get(int(x),'') if x else '')
    dfe['cnae_fiscal'] = dfe['cnae_fiscal'].apply(lambda x: x +'-'+ gdic.dicCnae.get(x,'') if x else '')
    
    dfe['porte_empresa'] = dfe['porte_empresa'].apply(lambda x: x+'-' + gdic.dicPorteEmpresa.get(x,'') if x else '')
    
    dfe.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "Empresas", index=False)

    dfs=pd.read_sql_query(querysocios, con)
    #dfs['cod_qualificacao'] =  dfs['cod_qualificacao'].apply(lambda x:x + '-' + gdic.dicQualificacao_socio.get(int(x),''))
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
            codnat = no['cod_nat_juridica'] if no['cod_nat_juridica'] else '' # bug codnat=None, banco do brasil não está na tabela empresa
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
        elif prefixo=='PE':
            imagem = 'icone-grafo-empresa.png'
        elif prefixo=='ID':
            imagem = 'folder-o.png'
        else:
            imagem = 'icone-grafo-desconhecido.png' #caso genérico
        #no['imagem'] = '/rede/static/imagem/' + imagem
        no['imagem'] = no.get('imagem', imagem)
        no['cor'] =  no.get('cor', '')
        if not no['camada']:
            no['cor'] = 'orange'
        no['nota'] =  no.get('nota', '')
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

def numeroDeEmpresasNaBase(): 
    #pega qtde de registros na tabela _referencia para acelerar o início da rotina
    if not caminhoDBReceita:
        return 0
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    try:
        res = con.execute('select valor from _referencia where referencia="cnpj_qtde"').fetchone()[0]
        r = int(res)
    except:
        r = 0
    if not r:
        r = con.execute('select count(*) as contagem from estabelecimento;').fetchone()[0]
    return r

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