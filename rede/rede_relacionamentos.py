# -*- coding: utf-8 -*-
"""
Created on set/2020
json a partir da tabela sqlite

@author: github rictom/rede-cnpj
2020-11-25 - Se uma tabela já existir, parece causar lentidão para o pandas pd.to_sql. 
Não fazer Create table ou criar índice para uma tabela a ser criada ou modificada pelo pandas 
"""
import os, sys, glob
import time, copy, re, string, unicodedata, collections, json, secrets
import pandas as pd, sqlalchemy
from fnmatch import fnmatch 
import cpf_cnpj
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
caminhoDBReceitaFTS = config.config['BASE'].get('base_receita_fulltext','').strip()
caminhoDBEnderecoNormalizado = config.config['BASE'].get('base_endereco_normalizado', '').strip()
caminhoDBLinks = config.config['BASE'].get('base_links', '').strip()

caminhoDBBaseLocal =  config.config['BASE'].get('base_local', '').strip()

#logAtivo = True if config['rede']['logAtivo']=='1' else False #registra cnpjs consultados
logAtivo = config.config['ETC'].getboolean('logativo',False) #registra cnpjs consultados
#    ligacaoSocioFilial = True if config['rede']['ligacaoSocioFilial']=='1' else False #registra cnpjs consultados
ligacaoSocioFilial = config.config['ETC'].getboolean('ligacao_socio_filial',False) #registra cnpjs consultados
kLimiteCamada = config.config['ETC'].getboolean('limite_registros_camada', 10000)

gEngineExecutionOptions = {"sqlite_raw_colnames": True, 'pool_size':1} #poll_size=1 força usar só uma conexão??
#'isolation_level':'AUTOCOMMIT'

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

def tabelaTemp():
    ''' tabela temporaria com numero aleatorio para evitar colisão '''
    return 'tmp' #'tmp_' + secrets.token_hex(4)

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

# def apagaTabelasTemporarias(prefixo_tabela_temporaria='tmp'):
#     con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
#     #con.execute('DROP TABLE if exists tmp_cnpjs')
#     tmp = prefixo_tabela_temporaria
#     con.execute(f'DROP TABLE if exists {tmp}_cpfpjnomes')
#     con.execute(f'DROP TABLE if exists {tmp}_ids')
#     con.execute(f'DROP TABLE if exists {tmp}_ligacao')
#     con.execute(f'DROP TABLE if exists {tmp}_busca_nome')
#     con = None

def apagaTabelasTemporarias(prefixo_tabela_temporaria='tmp', caminhoDB=caminhoDBReceita):
    '''apaga tabelas temporárias. Isto pode dar erro em ambiente com threads??
    se prefixo_tabela_temporaria='', apaga TODAS as tabelas tmp_'''
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDB}", execution_options=gEngineExecutionOptions)
    insp = sqlalchemy.inspect(con)
    tmp = prefixo_tabela_temporaria if prefixo_tabela_temporaria else 'tmp_'
    tmp_tabelas = [t for t in insp.get_table_names() if t.startswith(tmp)]
    for t in tmp_tabelas:
        con.execute(f'Drop table if exists {t}')
    con = None

apagaTabelasTemporarias() #apaga quando abrir o módulo

def buscaPorNome(nomeIn, limite=10): #nome tem que ser completo. Com Teste, pega item randomico
    '''caminhoDBReceitaFTS base com indice full text search, fica rápido com match mas com = fila lento, por isso
        precisa fazer consulta em caminhoDBReceita quando não for usar match
    '''
    #remove acentos
    #se limite==-1, não havia @N no nome
    tmp='tmp'
    nomeIn = nomeIn.strip().upper()
    caracteres_pontuacao = set('''!#$%&\'()+,-./:;<=>@[\\]^_`{|}~''') #sem * ? "
    nomeIn = ''.join(ch for ch in nomeIn if ch not in caracteres_pontuacao)
    if not nomeIn:
        return set(), set()
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
         df_busca_nomesPF.to_sql(f'{tmp}_busca_nomePF', con, if_exists='replace', index=None)
         query = f'''
                    SELECT distinct cnpj_cpf_socio, nome_socio
                    from {tmp}_busca_nomePF tn 
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
    for r in cursor.fetchall(): #con.execute(query):
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
    con.execute(f'drop table if exists {tmp}_busca_nomePF')
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
        df_busca_nomesPJ.to_sql(f'{tmp}_busca_nomePJ', con, if_exists='replace', index=None)
        query = f'''
                    SELECT te.cnpj, t.razao_social
                    from {tmp}_busca_nomePJ tn
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
    for r in cursor.fetchall(): #con.execute(query):
        if contagemRegistros>=limite:
            break
        if nomeMatch:
            if not fnmatch(r.razao_social.strip(), nomeMatchInicial): #strip porque tem espaço no começo da razão social
                continue
        cjs.add(r.cnpj)    
        contagemRegistros +=1
    con.execute(f'drop table if exists {tmp}_busca_nomePJ')
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
    dicr = {k[0] for k in r}
    con = None
    return dicr
    
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
    con = None
    return lista
            
def separaEntrada(cpfcnpjIn='', listaIds=None):
    cnpjs = set()
    cpfnomes = set()
    outrosIdentificadores = set() #outros identificadores, com EN_ (supondo dois caracteres mais underscore) 
    if cpfcnpjIn:
        lista = [i.strip() for i in cpfcnpjIn.split(';') if i.strip()]
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
            elif not re.findall('\D',str(i).replace('.','').replace('-','')): #só tem digitos, tenta acrescentar zeros à esquerda
                if cpf_cnpj.validar_cpf(i):
                    lcpfs = busca_cpf(cpf_cnpj.validar_cpf(i))
                    if lcpfs:
                        cpfnomes.update(set(lcpfs))                    
                if cpf_cnpj.validar_cnpj(i):
                    cnpjs.add(cpf_cnpj.validar_cnpj(i))
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

def criaTabelasTmpParaCamadas(con, cpfcnpjIn='', listaIds=None, grupo='', prefixo_tabela_temporaria=''):
    global gTable
    if prefixo_tabela_temporaria:
        tmp = prefixo_tabela_temporaria
    else:
        tmp = tabelaTemp()
        
    apagaTabelasTemporarias(tmp)
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

    dftmptable.to_sql(f'{tmp}_ids', con=con, if_exists='replace', index=False, dtype=dtype_tmp_ids)
    #indice deixa a busca lenta!
    #con.execute('CREATE INDEX ix_tmp_ids_index ON tmp_ids ("identificador")')
    camadasIds = {i:0 for i in ids}

    for outros in outrosIdentificadores:
        camadasIds[outros]=0   
    
    dftmptable = pd.DataFrame(list(cpfpjnomes), columns=['cpfpj', 'nome'])
    dftmptable['grupo'] = grupo
    dftmptable['camada'] = 0
    #con.execute('DELETE FROM tmp_cpfnomes')
    dftmptable.to_sql(f'{tmp}_cpfpjnomes', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cpfpjnomes)       
    
    return camadasIds, cnpjs, cpfnomes, tmp #, ids
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
    camadasIds, cnpjs, cpfnomes, tmp  = criaTabelasTmpParaCamadas(con, cpfcnpjIn=cpfcnpjIn, listaIds=listaIds, grupo=grupo)


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
        DROP TABLE if exists {tmp}_ligacao;
        
        CREATE TABLE {tmp}_ligacao AS
        SELECT DISTINCT * From 
        (
            SELECT t.cnpj as origem, '' as nome_origem, t.cnpj_cpf_socio , t.nome_socio, sq.descricao as cod_qualificacao
            FROM socios t
            INNER JOIN {tmp}_cpfpjnomes tl ON  tl.cpfpj = t.cnpj
            left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
            where tl.nome=''
            UNION
            SELECT t.cnpj as origem, '' as nome_origem, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
            FROM socios t
            INNER JOIN {tmp}_cpfpjnomes tl ON tl.cpfpj = t.cnpj_cpf_socio
            left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio 
            where tl.nome=''
            {whereMatriz}
            UNION
            SELECT t.cnpj as origem, '' as nome_origem, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
            FROM socios t
            INNER JOIN {tmp}_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
            left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
            where tn.nome<>''
            
            UNION --xxx inclui responsavel por socio
            SELECT t.representante_legal as origem, t.nome_representante as nome_origem, t.cnpj_cpf_socio, t.nome_socio, 'rep-sócio-' || sq.descricao as cod_qualificacao
            FROM socios t
            INNER JOIN {tmp}_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
            left join qualificacao_socio sq ON sq.codigo=t.qualificacao_representante_legal --sera que é a mesma tabela?
            where t.nome_representante<>''
            UNION --responsavel socio que é cnpj
            SELECT t.representante_legal as origem, t.nome_representante as nome_origem, t.cnpj_cpf_socio, t.nome_socio, 'rep-sócio-' || sq.descricao as cod_qualificacao
            FROM socios t
            INNER JOIN {tmp}_cpfpjnomes tl ON  tl.cpfpj = t.cnpj
            left join qualificacao_socio sq ON sq.codigo=t.qualificacao_representante_legal
            where tl.nome='' and t.nome_representante<>''
            
        ) as taux 
        ; 
        
        --inclui filiais
        Insert INTO {tmp}_ligacao (origem, nome_origem, cnpj_cpf_socio, nome_socio, cod_qualificacao) 
        select  tm.cnpj as origem,'' as nome_origem, tp.cpfpj as cnpj_cpf_socio, "" as nome_socio, "filial" as cod_qualificacao
        from estabelecimento t
        inner join {tmp}_cpfpjnomes tp on tp.cpfpj=t.cnpj
        left join estabelecimento tm on tm.cnpj_basico=t.cnpj_basico and tm.cnpj<>tp.cpfpj
        where tm.matriz_filial is "1" --is é mais rapido que igual (igual é muito lento)
        and tp.nome='';
        
        /*
        Insert INTO {tmp}_cpfpjnomes (cpfpj, nome, grupo, camada) 
        select distinct ts.origem as cpfpj, "" as nome, "{grupo}" as grupo, {cam+1} as camada
        From {tmp}_ligacao ts
        where length(cnpj_cpf_socio)=14
        */
        
        --acrescentado
        Insert INTO {tmp}_cpfpjnomes (cpfpj, nome, grupo, camada)
        select distinct ts.origem as cpfpj, nome_origem as nome, "{grupo}" as grupo, {cam+1} as camada
        From {tmp}_ligacao ts
        --where length(cnpj_cpf_socio)<>14
        ;        
        
        Insert INTO {tmp}_cpfpjnomes (cpfpj, nome, grupo, camada)
        select distinct cnpj_cpf_socio as cpfpj,"" as nome, "{grupo}" as grupo, {cam+1} as camada
        From {tmp}_ligacao ts
        where length(cnpj_cpf_socio)=14;
        
        Insert INTO {tmp}_cpfpjnomes (cpfpj, nome, grupo, camada)
        select distinct cnpj_cpf_socio as cpfpj, nome_socio as nome, "{grupo}" as grupo, {cam+1} as camada
        From {tmp}_ligacao ts
        where  length(cnpj_cpf_socio)<>14;

        drop table if exists {tmp}_cpfpjnomes_aux;
        
        create table {tmp}_cpfpjnomes_aux AS
        select cpfpj, nome, min(grupo) as grupo, min(camada) as camada
        from {tmp}_cpfpjnomes
        group by cpfpj, nome;
        
        drop table if exists {tmp}_cpfpjnomes;
        
        create table {tmp}_cpfpjnomes AS
        select *
        from {tmp}_cpfpjnomes_aux;        
        
        drop table if exists {tmp}_cpfpjnomes_aux;

                    
        Insert INTO {tmp}_ids (identificador, grupo, camada)
        select distinct "PJ_" || t.cpfpj as identificador,  t.grupo, t.camada
        From {tmp}_cpfpjnomes t
        where t.nome='';
        
        Insert INTO {tmp}_ids (identificador, grupo, camada)
        select distinct "PF_" || t.cpfpj || "-" || t.nome  as identificador, t.grupo, t.camada
        From {tmp}_cpfpjnomes t
        where t.cpfpj<>"" and t.nome<>'';
        
        Insert INTO {tmp}_ids (identificador, grupo, camada)
        select distinct "PE_" || t.nome  as identificador, t.grupo, t.camada
        From {tmp}_cpfpjnomes t
        where t.cpfpj = "" and t.nome<>'';
        
        drop table if exists {tmp}_ids_aux;
        
        create table {tmp}_ids_aux AS
        select identificador, min(grupo) as grupo, min(camada) as camada
        from {tmp}_ids
        group by identificador;
        
        drop table if exists {tmp}_ids;
        
        create table {tmp}_ids AS
        select *
        from {tmp}_ids_aux;
        
        drop table if exists {tmp}_ids_aux;
        '''

        for sql in query.split(';'):
            con.execute(sql)
        registros = con.execute(f'select count(*) from {tmp}_ids').fetchone()[0]
        if registros>kLimiteCamada:
            mensagem['popup'] = f'Alcançou apenas a camada {cam}, a camada {camada} não foi alcançada, por excesso de itens.'
            break
    #.for cam in range(camada): 
    if camada==0:
        #gambiarra, em camada 0, não apaga a tabela tmp_ligacao, por isso pega dados de consulta anterior.
        query0 = f''' 
        CREATE TABLE {tmp}_ligacao AS
        SELECT t.cnpj as origem, '' as nome_origem, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao
        FROM socios t
        left join qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
        limit 0
        '''
        con.execute(query0)
 
    
    queryLertmp = f'''
        Select *
        from {tmp}_ids
        where substr(identificador,1,3)='PF_' or substr(identificador,1,3)='PE_'
    '''
    for k in con.execute(queryLertmp):
        kid = k['identificador']
        if kid[:3]=='PF_':
            _, descricao = id2cpfnome(kid) #kid[15:] 
        else: #'PE_'
            descricao = '(EMPRESA SÓCIA NO EXTERIOR)'
        no = {'id': kid, 'descricao':descricao, 
                'camada': k['camada'] } #, 
                # 'situacao_ativa': True, 
                # #'empresa_situacao': 0, 'empresa_matriz': 1, 'empresa_cod_natureza': 0, 
                # 'logradouro':'',
                # 'municipio': '', 'uf': ''} 
        camadasIds[kid] = k['camada']
        nosaux.append(copy.deepcopy(no))         
    querySocios = f'''
        select *
        from {tmp}_ligacao
    '''
    for k in con.execute(querySocios):
        korigem = k['origem'] #e
        if len(korigem)==14:
            origem = cnpj2id(korigem) #'PJ_'+ ksocio
        else:
            origem = cpfnome2id(korigem, k['nome_origem']) # 'PF_'+ksocio+'-'+k['nome_socio']       
        kdestino = k['cnpj_cpf_socio']
        if len(kdestino)==14:
            destino = cnpj2id(kdestino) #'PJ_'+ ksocio
        else:
            destino = cpfnome2id(kdestino, k['nome_socio']) # 'PF_'+ksocio+'-'+k['nome_socio']
        ligacao = {"origem":origem, #cnpj2id(k['cnpj']), #'PJ_'+k['cnpj'], 
                   "destino":destino, 
                   "cor": "silver", #"cor":"gray", 
                   "camada":0,
                   "tipoDescricao":'sócio',
                   "label":k['cod_qualificacao']} #gdic.dicQualificacao_socio.get(int(k['cod_qualificacao']),'').strip()}
        ligacoes.append(copy.deepcopy(ligacao))
    if logAtivo or not bjson:
        conlog = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
        conlog.execute('create table if not exists log_cnpjs (cnpj text, grupo text, camada text)')
        conlog.execute(f'''insert into log_cnpjs 
            select * from {tmp}_cnpjs; ''')
        conlog.execute('create table if not exists log_cpfnomes (cpf text, nome text, grupo text, camada text);')
        conlog.execute(f'''insert into log_cpfnomes 
            select cpf, nome, grupo, cast(camada as int) from {tmp}_cpfnomes; ''')
        conlog = None
    if not bjson:
        con = None
        return len(camadasIds)
    for k in con.execute(f'Select cpfpj as cnpj, camada from {tmp}_cpfpjnomes where nome="" '):
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
                #if prefixo[2]=='_' and prefixo!='PF_':                
                if prefixo[2]=='_':
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
            #jsonEnderecosBanco = camadaLink(cpfcnpjIn='', listaIds=ids, conCNPJ=con, camada=1 if tipo=='endereco' else camada,  grupo=grupo, tipoLink=tipo)
            jsonEnderecosBanco = camadaLink(cpfcnpjIn='', listaIds=ids, conCNPJ=None, camada=1 if tipo=='endereco' else camada,  grupo=grupo, tipoLink=tipo)
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
    apagaTabelasTemporarias(tmp)
    return textoJson
#.def camadasRede

def dadosDosNosBaseLocal(nosInOut, camadasIds):
    if not caminhoDBBaseLocal:
        return 
    dicDados = jsonDadosBaseLocal(listaIds=list(camadasIds)) 
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
    tmp = tabelaTemp()
    dftmptable.to_sql(f'{tmp}_cnpjsdados', con=con, if_exists='replace', index=False, dtype=dtype_tmp_cnpjs)
    query = f'''
                SELECT tt.cnpj, te.razao_social, tt.situacao_cadastral as situacao, tt.matriz_filial,
                tt.tipo_logradouro, tt.logradouro, tt.numero, tt.complemento, tt.bairro,
                ifnull(tm.descricao,tt.nome_cidade_exterior) as municipio, tt.uf as uf, tpais.descricao as pais_,
                te.natureza_juridica as cod_nat_juridica
                from {tmp}_cnpjsdados tp
                inner join estabelecimento tt on tt.cnpj = tp.cnpj
                left join empresas te on te.cnpj_basico = tt.cnpj_basico --trocar por inner join deixa a consulta lenta...
                left join municipio tm on tm.codigo=tt.municipio
                left join pais tpais on tpais.codigo=tt.pais
            ''' #pode haver empresas fora da base de teste
    setCNPJsRecuperados = set()
    for k in con.execute(query).fetchall():
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
    con.execute(f'DROP TABLE if exists {tmp}_cnpjsdados ') 
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
    #camada = min(camada, 10) #camada alta causa erro no limite da sql, que fica muito grande. A camada de link
    if tipoLink=='endereco':
        if not caminhoDBEnderecoNormalizado:
            #mensagem['popup'] = 'Não há tabela de enderecos configurada.'
            return {'no': [], 'ligacao':[], 'mensagem': mensagem} 
    con = None
    tabela = ''
    tmp = tabelaTemp()
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
                    INNER JOIN {tmp}_ids tl
                    ON  tl.identificador = t.id1
                    UNION
                    SELECT t.id1, t.id2, t.descricao, t.valor
                    FROM {tabela} t
                    INNER JOIN {tmp}_ids tl
                    ON  tl.identificador = t.id2
                     ) ORDER by valor DESC
                    '''
    elif tipoLink=='endereco' or tipoLink=='base_local':
        if tipoLink=='endereco':
            if caminhoDBEnderecoNormalizado:
                con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBEnderecoNormalizado}", execution_options=gEngineExecutionOptions)        
                tabela = 'link_ete'
        else:
            if caminhoDBBaseLocal:
                con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}", execution_options=gEngineExecutionOptions)
                tabela = 'links'       

        valorMinimo=0
        valorMaximo=0
        numeroItens=0
        bValorInteiro = True

        query = f''' SELECT distinct t.id1, t.id2, t.descricao, t.valor
                     FROM {tmp}_ids tl
                     INNER JOIN {tabela} t ON  tl.identificador = t.id1
                     UNION
                     SELECT t.id1, t.id2, t.descricao, t.valor
                     FROM {tmp}_ids tl
                     INNER JOIN {tabela} t ON  tl.identificador = t.id2
                     '''
    # else:
    #     print('tipoLink indefinido')
    #     return {'no': [], 'ligacao':[], 'mensagem':'erro, tipoLink indefinido'} 
    if not con:
        print('tipoLink indefinido')
        return {'no': [], 'ligacao':[], 'mensagem':'erro, tipoLink indefinido'}         
    grupo = str(grupo)
    nosaux = []
    #nosids = set()
    ligacoes = []
    setLigacoes = set()
    camadasIds, cnpjs, cpfnomes, tmp = criaTabelasTmpParaCamadas(con, cpfcnpjIn=cpfcnpjIn, listaIds=listaIds, grupo=grupo, prefixo_tabela_temporaria=tmp)

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
                    ligacao['tipoDescricao'] = 'base_local'
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
        dftmptable.to_sql(f'{tmp}_ids', con=con, if_exists='replace', index=False, dtype=dtype_tmp_ids)
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
                    'camada': camadasIds[c]} #, 
                    # 'situacao_ativa': True, 
                    # 'logradouro':'',
                    # 'municipio': '', 'uf': ''} 
        # elif c.startswith('ID_'):
        #      
        else: #elif c.startswith('EN_'):
            no = {'id': c, 'descricao':'', 
                    'camada': camadasIds[c]} #, 
                    # 'situacao_ativa': True, 
                    # 'logradouro':'',
                    # 'municipio': '', 'uf': ''} 
        nosaux.append(copy.deepcopy(no))    
    dadosDosNosBaseLocal(nosaux, camadasIds)
    nosaux=ajustaLabelIcone(nosaux)
    textoJson={'no': nosaux, 'ligacao':ligacoes, 'mensagem':mensagem} 
    con = None
    if conCNPJ:
        conCNPJaux = None
    #print(' fim: ' + time.ctime())
    #print('camadaLink fim: ' + ' '.join(str(time.ctime()).split()[3:]))
    apagaTabelasTemporarias(tmp)
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
    # if outrosIdentificadores: #pegar o jsonDadosBaseLocal sempre
    #     return jsonDadosBaseLocal(cpfcnpjIn=cpfcnpjIn, listaIds=listaIds)
    # dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    # dftmptable['grupo']=''
    # dftmptable['camada']=0
    # tmp = tabelaTemp()
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}",execution_options=gEngineExecutionOptions)
    # dftmptable.to_sql(f'{tmp}_cnpjs1', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)

    # query = f'''
    #     select t.*, te.*, ifnull(tm.descricao,t.nome_cidade_exterior) as municipio_texto, tpais.descricao as pais_, tsimples.opcao_mei
    #     from estabelecimento t
    #     inner join {tmp}_cnpjs1 tp on tp.cnpj=t.cnpj
    #     left join empresas te on te.cnpj_basico=t.cnpj_basico
    #     left join municipio tm on tm.codigo=t.municipio
    #     left join simples tsimples on tsimples.cnpj_basico=t.cnpj_basico
    #     left join pais tpais on tpais.codigo=t.pais
        
    #         '''
    query = f'''
        select t.*, te.*, ifnull(tm.descricao,t.nome_cidade_exterior) as municipio_texto, tpais.descricao as pais_, tsimples.opcao_mei
        from estabelecimento t
        
        left join empresas te on te.cnpj_basico=t.cnpj_basico
        left join municipio tm on tm.codigo=t.municipio
        left join simples tsimples on tsimples.cnpj_basico=t.cnpj_basico
        left join pais tpais on tpais.codigo=t.pais
        where t.cnpj=:cnpjin
            '''
    camposPJ = ['cnpj', 'matriz_filial', 'razao_social', 'nome_fantasia', 'data_inicio_atividades', 'situacao_cadastral', 
				'data_situacao_cadastral', 'motivo_situacao_cadastral', 'natureza_juridica', 'cnae_fiscal', 'porte_empresa', 'opcao_mei',
				'endereco', 'municipio', 'uf', 'cep', 'nm_cidade_exterior', 'nome_pais', 'nm_cidade_exterior', 'nome_pais',
				'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'capital_social'
				]
    dlista = []
    if not cnpjs:
        d = None
    else:
        for k in con.execute(query, {'cnpjin':list(cnpjs)[0]}):
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
    
            d = {k:v for k,v in d.items() if k in camposPJ}
            d['id'] = 'PJ_'+ d['cnpj']
            dlista.append(copy.deepcopy(d))
            if not listaIds:
                break #só pega primeiro
        else:
            d = None
    #con.execute(f'Drop table if exists {tmp}_cnpjs1')
    if caminhoDBBaseLocal:
        dicDados = jsonDadosBaseLocal(cpfcnpjIn=cpfcnpjIn) 
        if dicDados:
            nosaux = []
            for n in dlista: 
                if n['id'] in dicDados:
                    daux = copy.deepcopy(dicDados[n['id']])
                    for q,v in daux.items():
                        n[q] = v
                nosaux.append(copy.deepcopy(n))
            dlista = nosaux
    #print('jsonDados-fim: ' + time.ctime())   

    con = None
    if not listaIds:
        return dlista[0] if dlista else {}
    else:
        return dlista
#.def jsonDados

def jsonDadosBaseLocal(cpfcnpjIn=None, listaIds=None):    
    if not caminhoDBBaseLocal:
        return {}
    if not listaIds and not cpfcnpjIn:
        return {}
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}",execution_options=gEngineExecutionOptions)
    if not listaIds:
        #dftmptable = pd.DataFrame({'id' : list(cpfcnpjIn.upper().split(';'))})
        dftmptable = pd.DataFrame({'id' : list(cpfcnpjIn.split(';'))})
    else:
        dftmptable = pd.DataFrame({'id' : list(listaIds)})
    
    tmp = tabelaTemp()
    dftmptable.to_sql(f'{tmp}_idsj', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)

    query = f'''
        select tj.id, tj.json
        from {tmp}_idsj t
        inner join dadosjson tj on tj.id = t.id
            '''
    #dlista = []
    dicLista = {}
    for k in con.execute(query):
        try:
            #d = {k['id']:json.loads(k['json'])} #dict(k)  
            d = json.loads(k['json']) #dict(k)  
        except:
            d = {k['id']:'erro na base'}
        # if not listaIds:
        #     break #só pega primeiro
        #dlista.append(copy.deepcopy(d))
        if k['id'] not in dicLista:
            dicLista[k['id']]=copy.deepcopy(d)
        else: #já havia registro de dados com id, sobrepoe os campos novos
            daux = copy.deepcopy(dicLista[k['id']])
            for q,v in d.items():
                daux[q] = v
            dicLista[k['id']]=copy.deepcopy(daux)
    # else:
    #     d = None
    con.execute(f'Drop table if exists {tmp}_idsj')
    con = None
    #print('jsonDados-fim: ' + time.ctime())   
    return dicLista
    # if not listaIds:
    #     return d
    # else:
    #     return dicLista #dlista
#.def jsonDadosBaseLocal

def carregaJSONemBaseLocal(nosLigacoes, comentario=''):
    if not caminhoDBBaseLocal:
        return
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}",execution_options=gEngineExecutionOptions)
    listaNo = []
    for dados in nosLigacoes['no']:
        dic = {key:valor for key,valor in dados.items() if key!='id'}
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
            tdescricao = junta(lig.get('tipoDescricao',''), ':', lig.get('label',''))
            listaLigacao.append([lig['origem'].upper(), lig['destino'].upper(), tdescricao, '', comentario])      
        except:
            print('erro...', lig)
    dftmptable = pd.DataFrame(listaNo, columns = ['id', 'json', 'comentario'])
    dftmptable.to_sql('dadosjson', con=con, if_exists='append', index=False, dtype=sqlalchemy.types.VARCHAR)
    dftmptable = pd.DataFrame(listaLigacao, columns = ['id1', 'id2', 'descricao','valor', 'comentario'])
    dftmptable.to_sql('links', con=con, if_exists='append', index=False, dtype=sqlalchemy.types.VARCHAR)
    con = None
#.def carregaJSONemBaseLocal

def junta(a, separador, b):
    if a and b:
        return a + ':' + b
    elif a:
        return a
    elif b:
        return b
    else:
        return ''

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
    camadasIds_, cnpjs_, cpfnomes_, tmp = criaTabelasTmpParaCamadas(con, listaIds=listaCpfCnpjs, grupo='')
    querysocios = f'''
                SELECT distinct * from
    				(SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                     t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
                FROM socios t
                --INNER JOIN tmp_cnpjs tl ON  tl.cnpj = t.cnpj
                INNER JOIN {tmp}_cpfpjnomes tl ON  tl.cpfpj = t.cnpj
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join pais tpais on tpais.codigo=t.pais
                where tl.nome=''
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao,
                    t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
                FROM socios t
                INNER JOIN {tmp}_cpfpjnomes tl ON tl.cpfpj = t.cnpj_cpf_socio
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join pais tpais on tpais.codigo=t.pais
                where tl.nome=''
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                    t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
                FROM socios t
                INNER JOIN {tmp}_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join pais tpais on tpais.codigo=t.pais
                where tn.nome<>''
                /* XXX
                UNION --xxx inclui responsavel por socio
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                    t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_,  t.faixa_etaria
                --SELECT t.representante_legal as origem, t.nome_representante as nome_origem, t.cnpj_cpf_socio, t.nome_socio, 'rep-sócio-' || sq.descricao as cod_qualificacao
                FROM socios t
                INNER JOIN {tmp}_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
                left join estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join pais tpais on tpais.codigo=t.pais
                where t.nome_representante<>'' */
            )
                ORDER BY nome_socio
            '''

    queryempresas = f'''
                SELECT te.*, tm.descricao as municipio_, tt.uf as uf_, pais.descricao as pais_, tt.*
                FROM {tmp}_cpfpjnomes tp 
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
    dfe['capital_social'] = dfe['capital_social'].apply(lambda capital: f"{capital/100:,.2f}".replace(',','@').replace('.',',').replace('@','.') if capital else '')
    
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
    apagaTabelasTemporarias(tmp)
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
        print('select count(*) as contagem from estabelecimento')
        r = con.execute('select count(*) as contagem from estabelecimento').fetchone()[0]
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

if __name__ == '__main__':
    apagaTabelasTemporarias('tmp_') #apaga todas as tabelas tmp_
    