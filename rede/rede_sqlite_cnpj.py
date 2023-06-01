# -*- coding: utf-8 -*-
"""
Created on set/2020
json a partir da tabela sqlite

@author: github rictom/rede-cnpj
2020-11-25 - Se uma tabela já existir, parece causar lentidão para o pandas pd.to_sql. 
Não fazer Create table ou criar índice para uma tabela a ser criada ou modificada pelo pandas 
2022-07-20 - Parâmetro WAL no sqlite para consultas concorrentes. (não funcionou, base trava)
2022-11 - usando sqlite3 para fazer attach. fazendo consulta in memory.
"""
import sys, os, time, copy, re, string, unicodedata, collections, json, secrets, io

from functools import lru_cache
import pandas as pd, sqlalchemy, sqlite3
#from fnmatch import fnmatch 
import util_cpf_cnpj as cpf_cnpj

import rede_config as config

caminhoDBReceita = config.config['BASE']['base_receita'].strip()
caminhoDBRede = config.config['BASE']['base_rede'].strip()
caminhoDBRedeSearch = config.config['BASE'].get('base_rede_search', caminhoDBRede).strip()
caminhoDBEnderecoNormalizado = config.config['BASE'].get('base_endereco_normalizado', '').strip()
caminhoDBLinks = config.config['BASE'].get('base_links', '').strip()
caminhoDBBaseLocal =  config.config['BASE'].get('base_local', '').strip()

if not caminhoDBReceita: #se não houver db da receita, carrega um template para evitar erros nas consultas
    caminhoDBReceita = 'base_cnpj_vazia.db'

if not caminhoDBRede or not os.path.isfile(caminhoDBRede) or not caminhoDBReceita or not os.path.isfile(caminhoDBReceita):
    sys.exit('Arquivo base cnpj.db ou base rede.db não foi localizado. Veja o caminho da base no arquivo de configuracao rede.ini está correto. Devem existir as tabelas rede.db e cnpj.db. O arquivo rede.db é criado com o script rede_cria_tabela.db.')

ligacaoSocioFilial = config.config['ETC'].getboolean('ligacao_socio_filial',False) #registra cnpjs consultados
kLimiteCamada = config.config['ETC'].getint('limite_registros_camada', 1000)
kTempoMaxConsulta = config.config['ETC'].getfloat('tempo_maximo_consulta', 10) #em segundos

class DicionariosCodigosCNPJ():
    def __init__(self):
        if not caminhoDBReceita:
            return
        con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}")
        dfaux = pd.read_sql_table('qualificacao_socio', con, index_col=None )
        self.dicQualificacao_socio = pd.Series(dfaux.descricao.values,index=dfaux.codigo).to_dict()
        dfaux = pd.read_sql_table('motivo', con, index_col=None )
        self.dicMotivoSituacao = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        dfaux = pd.read_sql_table('cnae', con, index_col=None )
        self.dicCnae = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        dfaux = pd.read_sql_table('natureza_juridica', con, index_col=None )
        self.dicNaturezaJuridica = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
        self.dicSituacaoCadastral = {'01':'Nula', '02':'Ativa', '03':'Suspensa', '04':'Inapta', '08':'Baixada'}
        self.dicPorteEmpresa = {'00':'Não informado', '01':'Micro empresa', '03':'Empresa de pequeno porte', '05':'Demais (Médio ou Grande porte)'}
        con = None
#.class DicionariosCodigosCNPJ():        
gdic = DicionariosCodigosCNPJ()

#dfaux=None

#gTableIndex = 0
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
            print (time.asctime() + ' %r  %2.2f ms' % \
                  (method.__name__, (te - ts) * 1000))
        return result    
    return timed

# def apagaTabelasTemporarias(prefixo_tabela_temporaria='tmp', caminhoDB=caminhoDBReceita):
#     '''apaga tabelas temporárias. Isto pode dar erro em ambiente com threads??
#     se prefixo_tabela_temporaria='', apaga TODAS as tabelas tmp_'''
#     con = sqlalchemy.create_engine(f"sqlite:///{caminhoDB}", execution_options=gEngineExecutionOptions)
#     insp = sqlalchemy.inspect(con)
#     tmp = prefixo_tabela_temporaria if prefixo_tabela_temporaria else 'tmp_'
#     tmp_tabelas = [t for t in insp.get_table_names() if t.startswith(tmp)]
#     for t in tmp_tabelas:
#         con.execute(f'Drop table if exists {t}')
#     con = None

# apagaTabelasTemporarias() #apaga quando abrir o módulo

def checaTabelaLigacao(caminhoDB=caminhoDBReceita):
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBRede}")
    insp = sqlalchemy.inspect(con)
    if 'ligacao' not in  insp.get_table_names():
        print('-'*50)
        print('ATENÇÃO!!!! A partir da versão 0.8.9, é preciso ter uma tabela de "ligacao" na base cnpj.\nRode o script rede_cria_tabela_rede.db.py para criar essa tabela.')
        print('-'*50)
        raise Exception('Rode o script rede_cria_tabela_rede.db.py para criar a tabela de "ligação"')
    con = None

checaTabelaLigacao() #apaga quando abrir o módulo


def buscaPorNome(nomeIn, limite=10): 
    ''' #alterado: nome tinha que ser completo, a partir da versão 0.9 não. Com Teste, pega item randomico
        alteração em 20/11/2022 usando match em todas as consultas. Busca mais flexível, não é preciso por o nome completo
    '''
    #remove acentos
    #se limite==-1, não havia @N no nome

    nomeIn = nomeIn.strip().upper()

    caracteres_pontuacao = set('''!#$%&\'\"()+,-./:;<=>@[\\]^_`{|}~''') #sem * ? "
    #alteração: removendo caracteres só no caso de Match...
    #nomeIn = ''.join(ch for ch in nomeIn if ch not in caracteres_pontuacao) #como o nomeIn é inserido como parametro, há menos risco de injection
    if not nomeIn:
        return set()
    limite =  min(limite,100) if limite else 10
    if nomeIn=='#TESTE#':
         con = sqlite3.connect(caminhoDBRede, uri=True)
    else:
        con = sqlite3.connect(caminhoDBRedeSearch, uri=True)
    nome = ''.join(x for x in unicodedata.normalize('NFKD', nomeIn) if x in string.printable).upper()

    nomeMatch = ''
    nomeGlob = ''
    if nomeIn=='#TESTE#':
        #query = '''select id_descricao as id from id_search where rowid > (abs(random()) % (select (select max(rowid) from id_search)+1)) LIMIT 1;'''
        query = '''select id1 as id from ligacao where rowid > (abs(random()) % (select (select max(rowid) from ligacao)+1)) LIMIT 1;'''
        #query = '''select id_descricao as id from id_search where rowid > (abs(random()) % (select (select max(rowid) from id_search)+1)) LIMIT 1;'''
        #cursor = con.execute(query)
    elif ('*' in nomeIn) or ('?' in nomeIn):
        nomeMatch = ''.join(ch if ch not in caracteres_pontuacao else ' ' for ch in nome).strip()
        nomeGlob = '*-' +''.join(ch if ch not in caracteres_pontuacao else '?' for ch in nome).strip()
        #match só dá com palavras inteiras, por isso tem que fazer um ajuste, se a palavra tiver curinga, substitui por espaço
        nomeMatch = nomeMatch.replace('*', ' ').strip()
        nomeMatch = nomeMatch.replace('?', '*')

        #nomeMatch = nomeMatch.removeprefix('*') #match não pode começar com *
        while True:
            tam = len(nomeMatch)
            #nomeMatch = re.sub('^\**', '', nomeMatch).strip() #remove * do começo
            nomeMatch = nomeMatch.removeprefix('*').strip()
            if len(nomeMatch)==tam:
                break
        if not nomeMatch:
            return set()

        query = '''
                    select distinct id_descricao as id
                    FROM id_search
                    where -- id_descricao match :palavraM and
                    id_descricao match :nomeMatch
                    and id_descricao glob :nomeGlob
                    limit :limite     
        
        '''

    else: #sem curinga. a busca está mais flexível, retorna registro que contém todas as palavras da busca, independente da ordem
        nomeMatch = ''.join(ch if ch not in caracteres_pontuacao else ' ' for ch in nome).strip()
        if not nomeMatch:
            return set()
        #palavraM = nomeMatch.split(' ')[0]
        query = '''
                SELECT id_descricao as id
                FROM id_search
                where -- id_descricao match :palavraM and
                id_descricao match :nomeMatch
                limit :limite 
            '''
    con.row_factory = sqlite3.Row #para ver registros do sqlite3 como dicionário
    cur = con.cursor()

    try:
        #print(query, f'{nomeMatch=}', f'{nomeGlob=}', f'{palavraM=}') #xxx
        cur.execute(query, {'nomeMatch':nomeMatch, 'nomeGlob':nomeGlob, 'limite':limite}) #, 'palavraM':palavraM})
    except Exception as e:
        print("ERROR : "+str(e))
        return set()
    cids = set()

    for r in cur:
    #for r in cursor.fetchall(): #con.execute(query):
        rid = r['id']
        if rid.startswith('PJ_'):
            rid = rid[:len('PJ_12345678901234')]
        cids.add(rid)
        # contagemRegistros += 1
    cur.close()
    con = None
    return cids #cjs, cps
#.def buscaPorNome

def busca_cnpj(cnpj_basico, limiteIn):
    kLimiteFiliais = 200
    #con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBRede}", execution_options=gEngineExecutionOptions)
    con = sqlite3.connect(caminhoDBRedeSearch, uri=True)
    limite = min(limiteIn, kLimiteFiliais) 
    if not limite:
        limite = 10 #xxx
        #melhor ignorar isso, com esse método fica difícil encontrar a matriz. Não necessariamente existe 0001 .Retorna o primeiro
        #cnpjMatch = 'PJ_' + cnpj_basico + '0001*' #não é bem a matriz, mas são poucas exceções de matriz que não seguem essa regra. E necessariamente vai aparecer a matriz ligada a alguma filial.
    
    cnpjMatch = 'PJ_' + cnpj_basico + '*'
    query = '''
            SELECT distinct substr(id_descricao, 1, 17) as id
            FROM id_search
            where id_descricao MATCH :cnpjMatch
            limit :limite '''
    con.row_factory=sqlite3.Row
    cur = con.cursor()
    cur.execute(query, {'cnpjMatch':cnpjMatch,'limite':limite})
    spj = {k['id'] for k in cur}
    #r = con.execute(query, {'cnpjMatch':cnpjMatch}).fetchall()
    #spj = {'PJ_'+k[0] for k in r}
    #spj = {k[0] for k in r}
    cur.close()
    con = None
    return spj
#.def busca_cnpj
    
def busca_cpf(cpfin, limiteIn):
    '''como a base não tem cpfs de sócios completos, faz busca só do miolo. retorna PF_xxx-nome'''
    #print('busca_cpf')
    limite = min(limiteIn, 100)
    if not limite:
        limite = 10 #default
    #cpf = '***' + cpfin[3:9] + '**'
    #con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBRede}", execution_options=gEngineExecutionOptions)
    con = sqlite3.connect(caminhoDBRedeSearch, uri=True)
    cpfMatch = 'PF_ ' + cpfin[3:9]
    cpfGlob = 'PF_' + '???' + cpfin[3:9] + '??*'
    query = '''
                SELECT distinct id_descricao as id
                FROM id_search
                where id_descricao Match :cpfMatch
                --and id_descricao glob :cpfGlob
                limit :limite
            '''
    #print(query, cpf)
    scpf = set()
    con.row_factory=sqlite3.Row
    cur = con.cursor()
    cur.execute(query, {'cpfMatch':cpfMatch, 'cpfGlob':cpfGlob, 'limite':limite})
    #for c in con.execute(query, {'cpfMatch':cpfMatch, 'cpfGlob':cpfGlob}).fetchall():
    for c in cur:
        #lista.append((c,n))
        #scpf.add('PF_'+c+'-'+n)
        scpf.add(c['id'])
    cur.close()
    con = None
    #print(scpf)
    return scpf
#.def busca_cpf
            
def separaEntrada(listaIds=None):
    cnpjs = set()
    cpfnomes = set()
    outrosIdentificadores = set() #outros identificadores, com EN_ (supondo dois caracteres mais underscore) 
    # if cpfcnpjIn:
    #     lista = [i.strip() for i in cpfcnpjIn.split(';') if i.strip()]
    # else:
    #     lista = listaIds
    lista1 = set()
    lista = set()
    cids = set()
    for cpfcnpjIn in listaIds:
        lista1.update({i.strip() for i in cpfcnpjIn.split(';') if i.strip()})
    for i in lista1:
        #if len(i)>3 and i[2]=='_' and (i.startswith('PF_') or i.startswith('PJ_')  or i.startswith('ID_')):
        if len(i)>3 and i[2]=='_' and (i[:3] in ('PF_', 'PJ_','PE_', 'ID_', 'EN_', 'EM_', 'TE_', 'LI_', 'CC_')):
            lista.add(i)
        else:
            limite = 0
            if kCaractereSeparadorLimite in i:
                i, limiteIn = kCaractereSeparadorLimite.join(i.split(kCaractereSeparadorLimite)[0:-1]).strip(), i.split(kCaractereSeparadorLimite)[-1]
                try:
                    limite = int(limiteIn)
                    if not limite:
                        limite = 1
                except ValueError:
                    limite = 1
            soDigitos = ''.join(re.findall('\d', str(i)))
            if len(soDigitos)==14:
                lista.add('PJ_'+soDigitos)
            elif len(soDigitos)==8:
                scnpj_aux = busca_cnpj(soDigitos, limite)
                lista.update(scnpj_aux) 
            elif len(soDigitos)==11:
                scpfs = busca_cpf(soDigitos, limite)
                lista.update(scpfs)
            elif re.search('\*\*\*\d\d\d\d\d\d\*\*',str(i)):
                scpfs = busca_cpf(str(i), limite) 
                lista.update(scpfs)
                pass #fazer verificação por CPF??
            #elif not soDigitos and i.strip():
            elif re.findall('[a-zA-Z]', str(i)) and i.strip(): #se tiver pelo menos uma letra, busca por nome
                #cnpjsaux, cpfnomesaux = buscaPorNome(i, limite=limite)
                cidsaux = buscaPorNome(i, limite)
                lista.update(cidsaux) 
            elif not re.findall('\D',str(i).replace('.','').replace('-','')): #só tem digitos, tenta acrescentar zeros à esquerda
                if cpf_cnpj.validar_cpf(i):
                    scpfs = busca_cpf(cpf_cnpj.validar_cpf(i), limite)
                    lista.update(scpfs)                    
                if cpf_cnpj.validar_cnpj(i):
                    #cnpjs.add(cpf_cnpj.validar_cnpj(i))
                    lista.add('PJ_'+cpf_cnpj.validar_cnpj(i))
        
    for i in lista:
        if i.startswith('PJ_'):
            cnpjs.add(i[3:])
            cids.add(i)
        elif i.startswith('PF_'):
            cpfcnpjnome = i[3:]
            cpf = cpfcnpjnome[:11]
            nome = cpfcnpjnome[12:]
            cpfnomes.add((cpf,nome))  
            cids.add(i)
        elif i.startswith('PE_'):
            cpfcnpjnome = i[3:]
            nome = cpfcnpjnome
            cpf = ''
            cpfnomes.add((cpf,nome))  
            cids.add(i)
        elif len(i)>3 and i[2]=='_':
            outrosIdentificadores.add(i)
            cids.add(i)

    cpfpjnomes = copy.deepcopy(cpfnomes)
    for c in cnpjs:
        cpfpjnomes.add((c,''))
    return cids, cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes
#.def separaEntrada

# dtype_tmp_ids={'identificador':sqlalchemy.types.VARCHAR,
#                        'grupo':sqlalchemy.types.VARCHAR,
#                        'camada':sqlalchemy.types.INTEGER }
# dtype_tmp_cnpjs={'cnpj':sqlalchemy.types.VARCHAR,
#                        'grupo':sqlalchemy.types.VARCHAR,
#                        'camada':sqlalchemy.types.INTEGER }
# dtype_tmp_cpfnomes={'cpf':sqlalchemy.types.VARCHAR,
#                        'nome':sqlalchemy.types.VARCHAR,
#                        'grupo':sqlalchemy.types.VARCHAR,
#                        'camada':sqlalchemy.types.INTEGER }

# dtype_tmp_cpfpjnomes={'cpfpj':sqlalchemy.types.VARCHAR,
#                        'nome':sqlalchemy.types.VARCHAR,
#                        'grupo':sqlalchemy.types.VARCHAR,
#                        'camada':sqlalchemy.types.INTEGER }

gtabelaTempComPrefixo = False
def tabelaTemp():
    ''' tabela temporaria com numero aleatorio para evitar colisão '''
    if gtabelaTempComPrefixo:      
        return 'tmp_' + secrets.token_hex(4)
    else:
        return 'tmp_'
    
#@timeit
#def criaTabelasTmpParaCamadas(con, listaIds=None, grupo='', prefixo_tabela_temporaria='', bSomenteIds=False):
def criaTabelasTmpParaCamadas(camDBAttach, aliasAttach, listaIds=None, grupo=None, prefixo_tabela_temporaria='', tabelasACriar=None):
    '''se camada<=1, vê se tem nomes. se camada>=2, supões que listaIds começa com padrão de identificador PJ_, PF_, ...
       se grupo for fornecido, ignora listaIds'''
    #https://www.sqlite.org/inmemorydb.html
    global gTable
    if prefixo_tabela_temporaria:
        tmp = prefixo_tabela_temporaria
    else:
        tmp = tabelaTemp()
    #xxx4
    con = sqlite3.connect(':memory:') 
    #con = sqlite3.connect('test_debug.db')
    #?mode=ro para read only
    con.execute("ATTACH DATABASE '" + camDBAttach.replace('\\','/') + "' as " + aliasAttach) 
    
    camadasIds = {}
    #apagaTabelasTemporarias(tmp)
    listGrupo = []
    if grupo:
        listGrupo = []
        
        listaAux = set()
        if type(grupo)==dict:
           for i, l in grupo.items():
               for k in l:
                   listGrupo.append([k, i])
                   #{identificador, grupo, origem (repete o identificador), camada}
                   listaAux.add(k)
        elif type(grupo)==list or type(grupo)==set:
            c = 1
            for l in grupo:
                for k in l:
                    listGrupo.append([k, c])
                    listaAux.add(k)
                c += 1        
        else:
            raise Exception("tipo de grupo não disponível")
        
        listaIds = listaAux
    elif listaIds:
        # for k in listaIds:
        #     listGrupo.append([k, ''])     
        pass
    else:
        raise Exception('situação não prevista, grupo e listaIds vazios na função criaTabelasTmpParaCamadas')

    cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes = set(), set(), set(), set()
    #if bSomenteIds:
    if tabelasACriar is None:
        ids = set(listaIds)
    else:
        ids, cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes = separaEntrada(listaIds=listaIds)

    if (tabelasACriar is None) or (not tabelasACriar) or ('ids' in tabelasACriar):
        if grupo:
            dftmptable = pd.DataFrame(listGrupo, columns=['identificador','grupo']) #, 'id_origem', 'camada'])           
        else:    
            dftmptable = pd.DataFrame({'identificador' : list(ids)})
            # else: 
            #     dftmptable =  pd.DataFrame({'identificador' : ['______xxx']}) #tabela vazia causa lentidão no loop de camadas!!! coloca coisa qualquer
            dftmptable['grupo'] = ''
        dftmptable['id_origem'] = dftmptable['identificador']
        dftmptable['camada'] = 0
        dftmptable.to_sql(f'{tmp}_ids', con=con, if_exists='replace', index=False) #, dtype=dtype_tmp_ids) # chunksize e multi deixa mais lento??, chunksize=50000, method='multi')
    #indice deixa a busca lenta!
    #con.execute('CREATE INDEX ix_tmp_ids_index ON tmp_ids ("identificador")')
    camadasIds = {i:0 for i in ids}
    for outros in outrosIdentificadores:
        camadasIds[outros]=0   
    
    if tabelasACriar and ('cpfpjnomes' in tabelasACriar):
        dftmptable = pd.DataFrame(list(cpfpjnomes), columns=['cpfpj', 'nome'])
        #dftmptable['grupo'] = grupo
        dftmptable['camada'] = 0
        #con.execute('DELETE FROM tmp_cpfnomes')
        dftmptable.to_sql(f'{tmp}_cpfpjnomes', con=con, if_exists='replace', index=False) #, dtype=dtype_tmp_cpfpjnomes)#, chunksize=50000, method='multi')       

    if tabelasACriar and ('cnpjs' in tabelasACriar):
        dftmptable = pd.DataFrame(list(cnpjs), columns=['cnpj'])
        #dftmptable['grupo'] = grupo
        dftmptable['camada'] = 0
        #con.execute('DELETE FROM tmp_cpfnomes')
        dftmptable.to_sql(f'{tmp}_cnpjs', con=con, if_exists='replace', index=False) #, dtype=dtype_tmp_cpfpjnomes)#, chunksize=50000, method='multi')       

    return con, camadasIds, cnpjs, cpfnomes, tmp #, ids
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
def camadasRede(listaIds=None, camada=1, grupo=None, criterioCaminhos='', bjson=True):    
    mensagem = '' #{'lateral':'', 'popup':'', 'confirmar':''}
    #con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBRede}", execution_options=gEngineExecutionOptions)
    '''
    https://stackoverflow.com/questions/17497614/sqlalchemy-core-connection-context-manager
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        print(conn.closed)
    print(conn.closed)'''
    tmp = 'tmp' #xxx verificar
    con, camadasIds, cnpjs_, cpfnomes_, tmp  = criaTabelasTmpParaCamadas(caminhoDBRede, 'rede', listaIds=listaIds, grupo=grupo, prefixo_tabela_temporaria=tmp, tabelasACriar=None if camada>1 else ['ids','cnpjs','cpfs_nomes']) #,' bSomenteIds=(camada>1))
    cur = con.cursor()
    if len(camadasIds)==0:
        #print('consulta sem ids de entrada')
        textoJson={'no': [], 'ligacao':[], 'mensagem':'Não encontrou informações.'} 
        cur.close() 
        con = None        
        return textoJson
    # print(listaIds) #x3
    # print(camadasIds) #x3
    #dicRazaoSocial = {} #excepcional, se um cnpj que é sócio na tabela de socios não tem cadastro na tabela empresas
    registrosAnterior = 0
    tempoInicio = time.time()
    query = f'''
        DROP TABLE if exists {tmp}_ids_inicial;
        CREATE TABLE {tmp}_ids_inicial AS
        SELECT * --identificador, grupo, id_origem, camada
        FROM {tmp}_ids;
        DROP TABLE if exists {tmp}_ligacao;
        CREATE TABLE {tmp}_ligacao  --cria tabela para caso de cam 0, sem ido dá erro na ora de criar tabela final de ids com id1 e id2
        (
            id1 VARCHAR, id2 VARCHAR, descricao VARCHAR            
        )
    '''

    con.executescript(query)
    cam=0
    tinicial = time.time()
    for cam in range(1, camada+1):  
        query = f''' 
        DROP TABLE if exists {tmp}_ligacao;
        
        CREATE TABLE {tmp}_ligacao AS
            SELECT t.id1, t.id2, t.descricao
            FROM {tmp}_ids tl
            INNER JOIN rede.ligacao t ON t.id1=tl.identificador
            UNION
            SELECT t.id1, t.id2, t.descricao
            FROM {tmp}_ids tl
            INNER JOIN rede.ligacao t ON t.id2=tl.identificador 
            WHERE t.descricao<>'filial' 
            --este where filial pode causar inconsistência na procura de caminhos por causar assimetria
        
            /* --fazer join para ver se tem repetição não faz diferença ou deixa um pouco mais lento
            SELECT t.id1, t.id2, t.descricao
            FROM {tmp}_ids ti
            INNER JOIN rede.ligacao t ON t.id1=ti.identificador
            LEFT JOIN {tmp}_ids tib ON t.id2=tib.identificador
            WHERE tib.identificador IS NULL
            UNION
            SELECT t.id1, t.id2, t.descricao
            FROM {tmp}_ids ti
            INNER JOIN rede.ligacao t ON t.id2=ti.identificador 
            LEFT JOIN {tmp}_ids tib ON t.id1=tib.identificador
            WHERE tib.identificador IS NULL 
            AND t.descricao<>'filial' 
            --este where filial pode causar inconsistência na procura de caminhos por causar assimetria
            */
        ;
        DROP TABLE if exists {tmp}_ids;
        CREATE TABLE {tmp}_ids AS
        SELECT DISTINCT * FROM 
        (   SELECT identificador
            FROM {tmp}_ids_inicial
            UNION
            SELECT t.id1 as identificador
            FROM {tmp}_ligacao t
            UNION
            SELECT t.id2 as identificador
            FROM {tmp}_ligacao t
        )
        '''
        
        con.executescript(query)
        #sqlite pode executar vários comandos com executescript(query)
        registros = cur.execute(f'select count(*) from {tmp}_ids').fetchone()[0]

        if registros==registrosAnterior: # and cam>1:
            if cam>1: 
                mensagem += f'A camada {camada} não foi alcançada, pois não havia mais itens além da camada {cam-1}.'
            break
        if cam<camada:
            if registros>kLimiteCamada:
                mensagem +=f'A camada {camada} não foi alcançada, pois excedeu o limite de itens por camada ({kLimiteCamada}). Chegou até a camada {cam}.'
                break
            if (time.time()-tempoInicio)>kTempoMaxConsulta:
                #print('xxx', time.time()-tempoInicio)
                mensagem +=f'A camada {camada} não foi alcançada, pois excedeu o tempo máximo de consulta. Chegou até a camada {cam}.'
                break
        registrosAnterior = registros
    #.for cam in range(camada): 
    #print('camada rede em', time.time()-tinicial) 
    #adiciona endereços, email, telefone e ligacoes da base local
    if camada>0:
        for tipoLink in ['endereco', 'base_local']:
            if tipoLink=='endereco':
                if not caminhoDBEnderecoNormalizado:
                    continue
                camDB = caminhoDBEnderecoNormalizado
                tabela = tipoLink + '.link_ete'
            else:
                if not caminhoDBBaseLocal:
                    continue
                camDB = caminhoDBBaseLocal
                tabela = tipoLink + '.links'   
                #db = 'dlink'
            
            con.execute("ATTACH DATABASE '" + camDB.replace('\\','/') + "' as " + tipoLink)
            query = f''' 
                         INSERT INTO  {tmp}_ligacao
                         SELECT distinct * from (
                         SELECT t.id1, t.id2, t.descricao
                         FROM {tmp}_ids tid
                         INNER JOIN {tabela} t ON  tid.identificador = t.id1
                         UNION
                         SELECT t.id1, t.id2, t.descricao
                         FROM {tmp}_ids tid
                         INNER JOIN {tabela} t ON  tid.identificador = t.id2
                         ) tu
                         '''
            con.execute(query)
    if criterioCaminhos:
        camadasRede_caminhos(con, tmp, camada, criterioCaminhos)
        textoJson = camadasRede_json(con, tmp, camadasIds, mensagem, bCaminhos=True)
    else:
        textoJson = camadasRede_json(con, tmp, camadasIds, mensagem, bCaminhos=False)
    
    cur.close() 
    con = None 
    return textoJson
#.def camadasRede

@timeit
def camadasRede_caminhos(con, tmp, camada, criterioCaminhos):
    #Rotina de caminhos
    #repete rede para os itens, mas agora coletando dados sobre grupo, id_origem e camada
    query = f'''

        DROP TABLE if exists {tmp}_ids;
        CREATE TABLE {tmp}_ids AS
        SELECT distinct *
        FROM {tmp}_ids_inicial;
        
        CREATE TABLE {tmp}_lig AS
        SELECT identificador as id1, identificador as id2, '' as descricao, grupo as grupo, identificador as id_origem, 0 as sentido, 0 as camada
        from {tmp}_ids;
        CREATE INDEX idx_{tmp}_ligacao1 ON  {tmp}_ligacao(id1);
        CREATE INDEX idx_{tmp}_ligacao2 ON  {tmp}_ligacao(id2);
    '''
    con.executescript(query)
    #for cam in range(1, camada+1):     
    for cam in range(1, camada+2):  #+1 por causa de endereços
        #print(f'{cam=}')
        query = f''' 
            -- aqui a tabela {tmp}_ligacao tem as ligações até a camada N.
            -- agora vai se fazer os caminhos, marcando grupo
            
            INSERT INTO {tmp}_lig
            /* codigo anterior, bem mais lento (5x), sem verificar repeticao de identificador no mesmo id_origem
            SELECT DISTINCT * From 
            (
                SELECT t.id1, t.id2, t.descricao, ti.grupo, ti.id_origem, 1 as sentido, {cam} as camada
                FROM {tmp}_ids ti
                INNER JOIN {tmp}_ligacao t ON t.id1=ti.identificador
                where ti.camada={cam-1}
                UNION
                SELECT t.id2 as id1, t.id1 as id2, t.descricao, ti.grupo, ti.id_origem, -1 as sentido, {cam} as camada
                FROM {tmp}_ids ti
                INNER JOIN {tmp}_ligacao t ON t.id2=ti.identificador 
                where ti.camada={cam-1} 
                
            )*/

            SELECT t.id1, t.id2, t.descricao, ti.grupo, ti.id_origem, 1 as sentido, {cam} as camada
            FROM {tmp}_ids ti
            INNER JOIN {tmp}_ligacao t ON t.id1=ti.identificador
            LEFT JOIN {tmp}_ids tib on tib.identificador=t.id2 and tib.id_origem=ti.id_origem
            where ti.camada={cam-1} and tib.identificador is NULL --remove repeticoes (ligacao de volta na camada+2)
            UNION
            SELECT t.id2 as id1, t.id1 as id2, t.descricao, ti.grupo, ti.id_origem, -1 as sentido, {cam} as camada
            FROM {tmp}_ids ti
            INNER JOIN {tmp}_ligacao t ON t.id2=ti.identificador 
            LEFT JOIN {tmp}_ids tib on tib.identificador=t.id1 and tib.id_origem=ti.id_origem
            where ti.camada={cam-1} and tib.identificador is NULL
                
            ;   
            INSERT INTO {tmp}_ids 
            SELECT t.id2 as identificador, t.grupo, t.id_origem, {cam} as camada
            FROM {tmp}_lig t;
        
            DROP TABLE IF EXISTS {tmp}_ids_aux;
            
            CREATE TABLE {tmp}_ids_aux AS
            SELECT identificador, grupo, id_origem, min(camada) as camada
            FROM {tmp}_ids
            GROUP BY identificador, grupo, id_origem;
            
            DROP TABLE if exists {tmp}_ids;
            CREATE TABLE {tmp}_ids AS
            SELECT *
            FROM {tmp}_ids_aux;
    
        '''
    
        con.executescript(query)
    #.for cam in range(camada): 
        
    # print('tamanho temp_ligacao', con.execute('Select count(*) from tmp_ligacao').fetchone())    
    # print('tamanho temp_lig', con.execute('Select count(*) from tmp_lig').fetchone())

    # Faz cruzamento para achar caminhos
    
    if criterioCaminhos=='caminhos':
        where_complemento=''
    elif criterioCaminhos=='intra':
        where_complemento='AND t1.grupo=t2.grupo'
    elif criterioCaminhos=='extra':
        where_complemento='AND t1.grupo<>t2.grupo'
    else:
        raise Exception('critério de caminhos não previsto.')
    
    # print('tmp_lig----------------\n', xlig:=pd.read_sql(f'select * from tmp_lig', con))
    # print(xlig[['id1','id2']])
    
    query = f'''
            DROP TABLE IF EXISTS {tmp}_lig_min;
            CREATE TABLE {tmp}_lig_min AS
            SELECT id1, id2, descricao, grupo, id_origem, sentido, min(camada) as camada
            FROM {tmp}_lig
            GROUP BY id1, id2, descricao, grupo, id_origem, sentido
            ;
            drop table {tmp}_lig;
            Create table {tmp}_lig AS
            select *
            from {tmp}_lig_min
            '''
    con.executescript(query)
    # query = f'''
    #         DROP TABLE IF EXISTS {tmp}_ids_cruzados;
    #         CREATE TABLE {tmp}_ids_cruzados AS 
    #         SELECT DISTINCT t1.id2 as id2, t1.camada, t1.id_origem, t2.id_origem as id_destino, t1.grupo as grupo_origem, t2.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
    #         FROM {tmp}_lig t1
    #         INNER JOIN {tmp}_lig t2 on t2.id2=t1.id2
    #         WHERE t1.id_origem<>t2.id_origem and t1.id1<>t2.id2
    #         {where_complemento}
    #         UNION
    #         SELECT distinct t2.id2 as id2, t2.camada, t2.id_origem, t1.id_origem as id_destino, t2.grupo as grupo_origem, t1.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
    #         FROM {tmp}_lig t1
    #         INNER JOIN {tmp}_lig t2 on t1.id2=t2.id2 
    #         WHERE  t1.id_origem<>t2.id_origem  and t1.id1<>t2.id2
    #         {where_complemento}      
    #         ;
    # '''
  
    # query = f''' --este é o gargalo da rotina
    #         DROP TABLE IF EXISTS {tmp}_ids_cruzados;
    #         CREATE TABLE {tmp}_ids_cruzados AS 
    #         SELECT id2, camada, id_origem, id_destino, grupo_origem, grupo_destino, camada_caminho
    #         FROM (
    #             SELECT DISTINCT t1.id2 as id2, t1.camada, t1.id_origem, t2.id_origem as id_destino, t1.grupo as grupo_origem, t2.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
    #             FROM {tmp}_lig t1
    #             INNER JOIN {tmp}_lig t2 on t2.id2=t1.id2
    #             WHERE t1.id_origem<t2.id_origem and t1.id1<>t2.id1 --inves de t1.id_origem<t2.id_origem, para evitar repetição
    #             {where_complemento}
    #             UNION
    #             SELECT DISTINCT t2.id2 as id2, t2.camada, t2.id_origem, t1.id_origem as id_destino, t2.grupo as grupo_origem, t1.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
    #             FROM {tmp}_lig t1
    #             INNER JOIN {tmp}_lig t2 on t1.id2=t2.id2 
    #             WHERE  t1.id_origem<t2.id_origem  and t1.id1<>t2.id1
    #             {where_complemento}  
    #         )
    # '''
    #xxx4
    query = f''' --este era um gargalo da rotina, melhorou reduzindo o tamanho da tabela {tmp}_lig, removendo repetições (volta do nó)
            --CREATE INDEX idx_{tmp}_lig1 ON  {tmp}_lig(id1); --parece mais rapido sem indice
            --CREATE INDEX idx_{tmp}_lig2 ON  {tmp}_lig(id2); 

            DROP TABLE IF EXISTS {tmp}_ids_cruzados;
            CREATE TABLE {tmp}_ids_cruzados AS 

            SELECT  t1.id2 as id2, t1.camada, t1.id_origem, t2.id_origem as id_destino, t1.grupo as grupo_origem, t2.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
            FROM {tmp}_lig t1
            INNER JOIN {tmp}_lig t2 on t2.id2=t1.id2
            WHERE t1.id_origem<t2.id_origem and t1.id1<>t2.id1 --t1.id_origem<t2.id_origem, para evitar repetição
            {where_complemento}
            UNION --union já remove duplicações
            SELECT  t2.id2 as id2, t2.camada, t2.id_origem, t1.id_origem as id_destino, t2.grupo as grupo_origem, t1.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
            FROM {tmp}_lig t1
            INNER JOIN {tmp}_lig t2 on t1.id2=t2.id2 
            WHERE  t1.id_origem<t2.id_origem  and t1.id1<>t2.id1
            {where_complemento}  
            
    '''
    # query1 = f''' --este é o gargalo da rotina
    #         --teste sem self join, não ajuda
    #         DROP TABLE if exists {tmp}_lig2;
    #         CREATE TABLE {tmp}_lig2 AS 
    #         SELECT * from {tmp}_lig;
    #         DROP TABLE IF EXISTS {tmp}_ids_cruzados;
    #         CREATE TABLE {tmp}_ids_cruzados AS 
    #         SELECT DISTINCT id2, camada, id_origem, id_destino, grupo_origem, grupo_destino, camada_caminho
    #         FROM (
    #             SELECT DISTINCT t1.id2 as id2, t1.camada, t1.id_origem, t2.id_origem as id_destino, t1.grupo as grupo_origem, t2.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
    #             FROM {tmp}_lig t1
    #             LEFT JOIN {tmp}_lig2 t2 on t2.id2=t1.id2
    #             WHERE t1.id_origem<>t2.id_origem and t1.id1<>t2.id2 
    #             {where_complemento}
    #             UNION
    #             SELECT DISTINCT t2.id2 as id2, t2.camada, t2.id_origem, t1.id_origem as id_destino, t2.grupo as grupo_origem, t1.grupo as grupo_destino, t1.camada+t2.camada as camada_caminho
    #             FROM {tmp}_lig t1
    #             LEFT JOIN {tmp}_lig2 t2 on t1.id2=t2.id2 
    #             WHERE  t1.id_origem<>t2.id_origem  and t1.id1<>t2.id2 
    #             {where_complemento}  
    #         )
            
    # '''

    con.executescript(query)
    query = f'''
            --cria tabela que indica qual o menor camada_caminho entre origem e destino
            DROP TABLE IF EXISTS {tmp}_menor_camada;
            CREATE TABLE {tmp}_menor_camada AS
            SELECT id_origem, id_destino, min(camada_caminho) as camada_caminho
            FROM {tmp}_ids_cruzados
            GROUP BY id_origem, id_destino
            ;
    '''
    con.executescript(query)
    query = f'''
            --cria tabela com os id2 para refazer os caminhos para as origens
            DROP TABLE IF EXISTS {tmp}_ids_cruzados_caminhos;
            CREATE TABLE {tmp}_ids_cruzados_caminhos AS
            SELECT DISTINCT tc.*
            --id2, camada, id_origem, id_destino, grupo_origem, grupo_destino, camada_caminho
            FROM {tmp}_ids_cruzados tc
            INNER JOIN {tmp}_menor_camada tm
            WHERE tc.id_origem=tm.id_origem AND tc.id_destino=tm.id_destino AND tc.camada_caminho=tm.camada_caminho
    '''
    con.executescript(query)
    #print('tmp_lig\n', xtmp_lig:=pd.read_sql(f'select * from tmp_lig', con))
    #print('_ids_cruzados\n', xids_cruzados:=pd.read_sql(f'select * from tmp_ids_cruzados', con))
    #print('_menor_camada-------------\n', xmenor_camada:=pd.read_sql(f'select * from tmp_menor_camada', con))
    #print('_ids_cruzados_caminhos\n', xids_cruzados_caminhos:=pd.read_sql(f'select * from tmp_ids_cruzados_caminhos', con))
    # junta os caminhos fazendo caminho inverso a partir dos pontos de cruzamento
    query = f'''
            --tmp_ids_grupo tem as colunas
            --SELECT t.id1, t.id2, t.descricao, tl.grupo, tl.id_origem, 1 as sentido, {cam} as camada
            
            --Faz caminho inverso de id2 até a origem
            DROP TABLE IF EXISTS {tmp}_ids_cruzados_caminhos_passos;
            CREATE TABLE {tmp}_ids_cruzados_caminhos_passos AS
            SELECT DISTINCT tl.id1, tc.id2, tl.descricao, tc.camada, tl.sentido, tc.id_origem, tc.id_destino, tc.grupo_origem, tc.grupo_destino, tc.camada_caminho
            FROM {tmp}_ids_cruzados_caminhos tc
            INNER JOIN {tmp}_menor_camada tm on tm.id_origem=tc.id_origem and tm.id_destino=tc.id_destino and tm.camada_caminho=tc.camada_caminho
            INNER JOIN {tmp}_lig tl on tl.id2=tc.id2 and tl.grupo=tc.grupo_origem and tl.id_origem=tc.id_origem 
                and tl.camada=tc.camada
    '''
    con.executescript(query)
    #print('_ids_cruzados_caminhos_passos-----\n', x:=pd.read_sql(f'select * from tmp_ids_cruzados_caminhos_passos', con))
    #for cam in range(camada, -1, -1):
    for cam in range(camada+1, -1, -1):
        query = f'''
            INSERT INTO {tmp}_ids_cruzados_caminhos_passos
            SELECT tl.id1, tl.id2, tl.descricao, tl.camada, tl.sentido, tc.id_origem, tc.id_destino, tc.grupo_origem, tc.grupo_destino, tc.camada_caminho
            FROM {tmp}_ids_cruzados_caminhos_passos tc
            INNER JOIN {tmp}_lig tl on tl.id2=tc.id1 and tl.grupo=tc.grupo_origem and tl.id_origem=tc.id_origem 
                and tl.camada+1=tc.camada   
            where tc.camada={cam};
            
            DROP TABLE IF EXISTS {tmp}_ids_cruzados_caminhos_passos_aux;
            CREATE TABLE {tmp}_ids_cruzados_caminhos_passos_aux AS
            SELECT DISTINCT *
            FROM {tmp}_ids_cruzados_caminhos_passos;
            
            DROP TABLE IF EXISTS {tmp}_ids_cruzados_caminhos_passos;
            CREATE TABLE {tmp}_ids_cruzados_caminhos_passos AS
            SELECT DISTINCT *
            FROM {tmp}_ids_cruzados_caminhos_passos_aux;
            
        '''
        con.executescript(query)
    #.for cam in range(camada, -1, -1):
        
    #print('_ids_cruzados_caminhos_passos-----\n', xids_cruzados_caminhos_passos:=pd.read_sql(f'select * from tmp_ids_cruzados_caminhos_passos', con))
    # organiza para ajustar o sentido
    query = f'''
            --organiza para ajustar o sentido
            CREATE TABLE {tmp}_caminhos_final AS
            SELECT  * 
            FROM (
                SELECT 
                    id1, id2, descricao, camada, id_origem, id_destino, grupo_origem, grupo_destino, camada_caminho
                FROM
                {tmp}_ids_cruzados_caminhos_passos
                WHERE sentido=1
                UNION
                SELECT 
                    id2, id1, descricao, camada, id_origem, id_destino, grupo_origem, grupo_destino, camada_caminho
                FROM
                {tmp}_ids_cruzados_caminhos_passos
                WHERE sentido=-1
                
            ) ORDER BY camada
            '''
    con.executescript(query)     
    
    #print('tmp_caminhos_final-----------\n', xcaminhos_final:=pd.read_sql(f'select * from tmp_caminhos_final', con))         
    query = f'''
            --somente ligacoes
            
            /*
            --mostra duplas origem destino
            CREATE TABLE {tmp}_origem_destino AS
            SELECT DISTINCT id_origem, id_destino, camada_caminho
            FROM {tmp}_caminhos_final;
            */
            --mostra duplas origem destino_grupo
            CREATE TABLE {tmp}_origem_destino_grupo AS
            SELECT DISTINCT id_origem, id_destino, grupo_origem, grupo_destino, camada_caminho
            FROM {tmp}_caminhos_final;
            
            /*
            CREATE TABLE {tmp}_ligacao_final AS
            SELECT DISTINCT id1, id2, descricao, camada
            FROM  {tmp}_caminhos_final 
            */
            
            DROP TABLE IF exists {tmp}_ligacao;
            CREATE TABLE {tmp}_ligacao AS
            SELECT DISTINCT *
            FROM {tmp}_caminhos_final;   
            
    '''
    con.executescript(query)
#.def camadasRede_caminhos

@timeit
#def camadasRede_json(camadasIds, cam, camada, mensagem, con, tmp, bCaminhos=False):
def camadasRede_json(con, tmp, camadasIds, mensagem,  bCaminhos=False):
    nosaux = []
    ligacoes = []        
    sno = set()
    #if cam: 
    query = f''' --atualiza {tmp}_ids para buscar dados de cnpjs
            DROP TABLE if exists {tmp}_ids;
            CREATE TABLE {tmp}_ids AS
            --se calcular caminhos, não precisa adicionar itens sem ligações do conjunto de origem
            { " "  if bCaminhos else f"SELECT identificador FROM {tmp}_ids_inicial UNION "}
            SELECT t.id1 as identificador
            FROM {tmp}_ligacao t
            UNION
            SELECT t.id2 as identificador
            FROM {tmp}_ligacao t
            
    '''
    con.executescript(query)
    #if camada>0:
    # queryLigacao = f''' SELECT id1 as origem, id2 as destino, descricao as label from {tmp}_ligacao'''
    # dlaux= pd.read_sql(queryLigacao, con)[['origem','destino','label']]
    # dlaux['cor']='silver'
    # dlaux['camada']=0
    # dlaux['tipoDescricao']=''

    # ligacoes.extend(dlaux.to_dict('records')) #yyy usando pandas para dict é mais rápido, mas tem que fazer mais ajustes
    # sno.update(dlaux['origem'].unique())
    # sno.update(dlaux['destino'].unique())

    #ligacoes=[]
    queryLigacao = f''' SELECT id1 as origem, id2 as destino, descricao as label from {tmp}_ligacao'''
    for row in con.execute(queryLigacao): #metade do tempo do que usar pd.read_sql
        ligacoes.append({'origem':row[0], 
                          'destino':row[1],
                          'label':row[2],
                          'cor':'silver',
                          'camada':0,
                          'tipoDescricao':''
                          })
        sno.add(row[0])
        sno.add(row[1])
   
    # queryNos = f'''SELECT  identificador as id
    #             from {tmp}_ids
    #             '''
    # dnoaux = pd.read_sql(queryNos, con)
    # sno.update(dnoaux['id'])
        
    query = f'''Select distinct identificador as id, group_concat(grupo) as grupo from {tmp}_ids_inicial group by identificador'''
    dgrupo = pd.read_sql(query, con)
    dicGrupo = pd.Series(dgrupo.grupo.values, index=dgrupo.id).to_dict()
    
    for n in sno:
        if n not in camadasIds:
            camadasIds[n] = 1 #não está calculando camadas... só distinguindo o que é camada 0 ou não
        if not n.startswith('PJ_'):
            descricao = ''
            if n.startswith('PF_'):
                _, descricao = id2cpfnome(n) #kid[15:] 
            elif n.startswith('PE_'): 
                descricao = '(EMPRESA SÓCIA NO EXTERIOR)'
            no = {'id': n, 'descricao':descricao, 
                    'camada': camadasIds.get(n,1),
                    } #, 
            if n in dicGrupo:
                no['nota'] = dicGrupo[n]
            nosaux.append(copy.deepcopy(no))    
    
    #print('diferença', set(camadasIds).difference(sno))
    #bAdicionaItensQueNaoAparecemNaTabelaDeLigacao = True
    if not bCaminhos: #se não for busca por caminhos, adiciona itens da camada 0
        for n in set(camadasIds).difference(sno): #adiciona itens que não aparecem na tabela de ligacao rede.db (mas pode estar em outras, como links.db)
            #no = {'id': n, 'descricao':'', 'camada': camadasIds.get(n,1)} #, 
            if n[:3] in ('PF_', 'PJ_', 'ID_', 'EN_', 'EM_', 'TE_'):
                continue
            no = {'id': n, 'descricao': '', 
                  'camada': camadasIds.get(n,1), 'tipo':0, 'situacao_ativa': True,
                  #'logradouro': '',
                  #'municipio': '', 'uf': '',  
                  'cod_nat_juridica':''
                  }
            nosaux.append(copy.deepcopy(no))

    # cnpjs = {n.removeprefix('PJ_') for n in camadasIds.keys() if n.startswith('PJ_')} #usar sno ao inves de camadasIds.keys pula ligações
    # if cnpjs:
    #     if cam: #se for camada 0, {tmp}_ids já existe
    #         query = f''' --atualiza {tmp}_ids para buscar dados de cnpjs
    #                 DROP TABLE if exists {tmp}_ids;
    #                 CREATE TABLE {tmp}_ids AS
    #                 SELECT DISTINCT * FROM 
    #                 (   --se calcular caminhos, não precisa adicionar itens sem ligações do conjunto de origem
    #                      { " "  if bCaminhos else f"SELECT identificador FROM {tmp}_ids_inicial UNION "}
    #                     SELECT t.id1 as identificador
    #                     FROM {tmp}_ligacao t
    #                     UNION
    #                     SELECT t.id2 as identificador
    #                     FROM {tmp}_ligacao t
    #                 )
    #         '''
    #         con.executescript(query)
    #     #dadosDosNosCNPJs(cnpjs=cnpjs, nosaux=nosaux, camadasIds=camadasIds, tmp=tmp, con=con)
    #     dadosDosNosCNPJs(nosaux=nosaux, camadasIds=camadasIds, tmp=tmp, con=con, dicGrupo=dicGrupo)

    # cnpjs = {n.removeprefix('PJ_') for n in camadasIds.keys() if n.startswith('PJ_')} #usar sno ao inves de camadasIds.keys pula ligações
    #fazer essa checagem em python demora, faz d

    #dadosDosNosCNPJs(cnpjs=cnpjs, nosaux=nosaux, camadasIds=camadasIds, tmp=tmp, con=con)
    dadosDosNosCNPJs(nosaux=nosaux, camadasIds=camadasIds, tmp=tmp, con=con, dicGrupo=dicGrupo)
    dadosDosNosBaseLocal(nosaux, camadasIds, tmp=tmp, con=con)
    nosaux=ajustaLabelIcone(nosaux)
    if bCaminhos: #adiciona no json tabela de achados
        queryLigacao = f''' SELECT * from {tmp}_origem_destino_grupo'''
        dod= pd.read_sql(queryLigacao, con)    
        
        textoJson={'no': nosaux, 'ligacao':ligacoes, 'mensagem':mensagem, 'origem_destino':dod.to_dict('records')}
    else:
        textoJson={'no': nosaux, 'ligacao':ligacoes, 'mensagem':mensagem} 
    return textoJson
#.def camadasRede_json

#@timeit
def dadosDosNosBaseLocal(nosInOut, camadasIds, tmp=None, con=None):
    if not caminhoDBBaseLocal:
        return 
    dicDados = jsonDadosBaseLocalDic(listaIds=list(camadasIds), tmp=tmp, con=con) 
    nosaux = []
    for n in nosInOut: 
        if n['id'] in dicDados:
            daux = copy.deepcopy(dicDados[n['id']])
            for k,v in daux.items():
                n[k] = v
        nosaux.append(copy.deepcopy(n))
    nosInOut = nosaux
#.def dadosDosNosBaseLocal
         
#@timeit
#def dadosDosNosCNPJs(cnpjs, nosaux, camadasIds, tmp, con):
def dadosDosNosCNPJs(nosaux, camadasIds, tmp, con, dicGrupo=None):

    con.execute("ATTACH DATABASE '" + caminhoDBReceita.replace('\\','/') + "' as " + 'cnpj')
    con.row_factory=sqlite3.Row
    cur = con.cursor()
    query = f'''                
                DROP TABLE if exists {tmp}_cnpjs;
                
                CREATE TABLE {tmp}_cnpjs AS -- a tabela vai conter identificadores PJ_xx camada 0 que não estão na base
                SELECT distinct substr(identificador,4) as cnpj from {tmp}_ids 
                where substr(identificador,1,3)='PJ_';
                
                SELECT tp.cnpj, te.razao_social, tt.nome_fantasia, tt.situacao_cadastral as situacao, tt.matriz_filial,
                tt.tipo_logradouro, tt.logradouro, tt.numero, tt.complemento, tt.bairro,
                ifnull(tm.descricao,tt.nome_cidade_exterior) as municipio, tt.uf as uf, tpais.descricao as pais_,
                te.natureza_juridica as cod_nat_juridica
                from {tmp}_cnpjs tp
                left join cnpj.estabelecimento tt on tt.cnpj = tp.cnpj
                left join cnpj.empresas te on te.cnpj_basico = tt.cnpj_basico --trocar por inner join deixa a consulta lenta...
                left join cnpj.municipio tm on tm.codigo=tt.municipio
                left join cnpj.pais tpais on tpais.codigo=tt.pais
            ''' #pode haver empresas fora da base de teste
    #setCNPJsRecuperados = set()

    for subquery in query.split(';'):
        cur.execute(subquery)

    for k in cur:
        #k = dict(zip(colsname, k1))
        if k['razao_social']: #cnpj na base
            logradouro_complemento =  k['complemento'].strip() #strip(';')]
            if k['uf']!='EX':
                #listaaux.append(k['bairro']) #se for empresa no exterior, o bairro as vezes aparece como municipio
                logradouro_complemento += ('-' + k['bairro'].strip()) if k['bairro'].strip() else ''
                logradouro_complemento = logradouro_complemento.removeprefix('-')
            #listalogradouro = [j.strip() for j in listaaux if j.strip()]
            #listalogradouro = [j.strip() for j in [k['logradouro'].strip(), k['numero'], k['complemento'].strip(';'), k['bairro']] if j.strip()]
            logradouro = ', '.join([k['logradouro'].strip(), k['numero'].strip()] )
            if not logradouro.startswith(k['tipo_logradouro'].strip()):
                logradouro = k['tipo_logradouro'].strip() + ' ' + logradouro
            logradouro = re.sub("\s\s+", " ", logradouro)
            logradouro_complemento = re.sub("\s\s+", " ", logradouro_complemento)
            no = {'id': cnpj2id(k['cnpj']), 
                  'descricao': cpf_cnpj.removeCPFFinal(k['razao_social']), #xxx gambiarra, remove cpf no final de razao social de empresario individual
                  'nome_fantasia': k['nome_fantasia'], 
                  'camada': camadasIds[cnpj2id(k['cnpj'])], 'tipo':0, 'situacao_ativa': int(k['situacao'])==2,
                  'logradouro': logradouro,
                  'logradouro_complemento': logradouro_complemento, #quebrando complemento, para poder usar logradouro no openstreetmap
                  'municipio': k['municipio'], 
                  'uf':k['uf'], 
                  'cod_nat_juridica':k['cod_nat_juridica']
                  }
            if k['uf']=='EX':
                no['municipio']=k['bairro']
                no['pais']=k['pais_']
            if int(k['situacao']) not in (2, 8):
                no['situacao_fiscal'] = gdic.dicSituacaoCadastral.get(k['situacao'],'') 
        else:
            no = {'id': cnpj2id(k['cnpj']), 'descricao': 'NÃO FOI LOCALIZADO NA BASE', 
                  'camada': camadasIds[cnpj2id(k['cnpj'])], 'tipo':0, 'situacao_ativa': True,
                  'logradouro': '',
                  'municipio': '', 'uf': '',  'cod_nat_juridica':''
                  }
        if dicGrupo:
            if no['id'] in dicGrupo:
                no['nota'] = dicGrupo[no['id']]
        nosaux.append(copy.deepcopy(no))
        #setCNPJsRecuperados.add(k['cnpj'])
    #trata caso excepcional com base de teste, cnpj que é sócio não tem registro na tabela empresas
    # if False:
    #     diffCnpj = cnpjs.difference(setCNPJsRecuperados)
    #     for cnpj in diffCnpj:
    #         #no = {'id': cnpj2id(cnpj), 'descricao': dicRazaoSocial.get(cnpj, 'NÃO FOI LOCALIZADO NA BASE'), 
    #         no = {'id': cnpj2id(cnpj), 'descricao': 'NÃO FOI LOCALIZADO NA BASE', 
    #               'camada': camadasIds[cnpj2id(cnpj)], 'tipo':0, 'situacao_ativa': True,
    #               'logradouro': '',
    #               'municipio': '', 'uf': '',  'cod_nat_juridica':''
    #               }
    #         nosaux.append(copy.deepcopy(no))
    #ajusta nos, colocando label
    #nosaux=ajustaLabelIcone(nosaux)
    
    nos = nosaux #nosaux[::-1] #inverte, assim os nos de camada menor serao inseridas depois, ficando na frente
    nosaux = nos.sort(key=lambda n: n['camada'], reverse=True) #inverte ordem, porque os últimos icones vão aparecer na frente. Talvez na prática não seja útil.   
#.def dadosDosNosCNPJs

#@timeit
#def camadaLink(cpfcnpjIn='', conCNPJ=None, camada=1, numeroItens=15, 
def camadaLink(listaIds=None, conCNPJ=None, camada=1, numeroItens=15, 
               valorMinimo=0, valorMaximo=0, grupo='', bjson=True, 
               tipoLink='link'):    
    mensagem = '' #{'lateral':'', 'popup':'', 'confirmar':''}
    #camada = min(camada, 10) #camada alta causa erro no limite da sql, que fica muito grande. A camada de link
    if not listaIds:
        return {'no': [], 'ligacao':[], 'mensagem': mensagem} 
    if tipoLink=='endereco':
        if not caminhoDBEnderecoNormalizado:
            #mensagem['popup'] = 'Não há tabela de enderecos configurada.'
            return {'no': [], 'ligacao':[], 'mensagem': mensagem} 
    con = None
    tabela = ''
    tmp = tabelaTemp()
    camDB = ''
    if tipoLink=='link':
        if not caminhoDBLinks:
            mensagem = 'Não há tabela de links configurada.'
            return {'no': [], 'ligacao':[], 'mensagem': mensagem} 
        camDB = caminhoDBLinks
        #db = 'dlink'
        #con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBLinks}",execution_options=gEngineExecutionOptions)
        tabela = 'dlink.links'
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

    if not camDB: #con:
        print('tipoLink indefinido')
        return {'no': [], 'ligacao':[], 'mensagem':'erro, tipoLink indefinido'}         
    grupo = str(grupo)
    nosaux = []
    #nosids = set()
    ligacoes = []
    setLigacoes = set()
    con, camadasIds, cnpjs, cpfnomes_, tmp = criaTabelasTmpParaCamadas(camDB, 'dlink', listaIds=listaIds, grupo=grupo, prefixo_tabela_temporaria=tmp)
    #https://docs.python.org/2/library/sqlite3.html#using-the-connection-as-a-context-manager 
    con.row_factory = sqlite3.Row #para ver registros do sqlite3 como dicionário
    cur = con.cursor()
    
    cnpjsInicial = copy.deepcopy(cnpjs)
    #dicRazaoSocial = {} #excepcional, se um cnpj que é sócio na tabela de socios não tem cadastro na tabela empresas
    limite = numeroItens #15
    #passo = numeroItens*2 #15
    #cnt1 = collections.Counter() #contadores de links para o id1 e id2
    #cnt2 = collections.Counter()    
    tempoInicio = time.time()
    registrosAnterior = -1
    cntlink = collections.Counter()
    for cam in range(1, camada+1):       
        #no sqlite, o order by é feito após o UNION.
        #ligacoes = [] #tem que reiniciar a cada loop

        #orig_destAnt = ()

        # #tem que mudar o método, teria que fazer uma query para cada entrada
        # res = con.execute(query + ' LIMIT ' + str(limite) if limite else query)
        # colsname = [t[0] for t in res.description]
        # #for k in con.execute(query + ' LIMIT ' + str(limite) if limite else query):
        # for k1 in res.fetchall():
        

        #cur.execute(query + ' LIMIT ' + str(limite) if limite else query)
        if limite:
            cur.execute(query + ' LIMIT :limite', {'limite':limite})
        else:
            cur.execute(query)
        for k in cur:
            
            #k=dict(zip(colsname,k1))
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
            
            if k['id1'] not in camadasIds:
                camadasIds[k['id1']] = cam           
            if k['id2'] not in camadasIds:
                camadasIds[k['id2']] = cam
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
                           "camada":cam} #"label":k['descricao'] + ':' + ajustaValor(k['valor'], bValorInteiro)}
                if tipoLink=='endereco':
                    ligacao['tipoDescricao'] = k['descricao']
                    ligacao['label'] = k['descricao']
                elif tipoLink=='base_local':
                    ligacao['label'] = k['descricao'] 
                    ligacao['tipoDescricao'] = 'base_local'
                else:
                    ligacao['label'] = k['descricao'] + ': ' + ajustaValor(k['valor'], bValorInteiro)
                    ligacao['tipoDescricao'] = k['descricao'] 
                ligacoes.append(copy.deepcopy(ligacao))
                setLigacoes.add((k['id1'], k['id2']))
            else:
                print('####ligacao repetida. A implementar')
        #.for k in con.execute(query):
        listaProximaCamada = [item for item in camadasIds if camadasIds[item]>cam-1]
        dftmptable = pd.DataFrame({'identificador' : listaProximaCamada})
        dftmptable['grupo'] = grupo
        dftmptable['camada'] = dftmptable['identificador'].apply(lambda x: camadasIds[x])
        #con.commit()
        dftmptable.to_sql(f'{tmp}_ids', con=con, if_exists='replace', index=False) #vvv, dtype=dtype_tmp_ids)
        #curioso, esse índice deixa a busca lenta!!!!
        #con.execute('CREATE INDEX ix_tmp_ids_index ON tmp_ids ("identificador")')
        limite = limite * numeroItens * 2
        registros = con.execute(f'select count(*) from {tmp}_ids').fetchone()[0]
        if cam<camada:
            if registros>kLimiteCamada:
                mensagem=f'A camada {camada} não foi alcançada, pois excedeu o limite de itens. Chegou até a camada {cam}.'
                break
            if registros==registrosAnterior and cam>1:
                mensagem = f'A camada {camada} não foi alcançada,  pois não havia mais itens. Chegou na camada {cam-1}.'
                break
            if (time.time()-tempoInicio)>kTempoMaxConsulta:
                #print('xxx', time.time()-tempoInicio)
                mensagem=f'A camada {camada} não foi alcançada, pois excedeu o tempo máximo de consulta. Chegou até a camada {cam}.'
                break
        registrosAnterior = registros
    #.for cam in range(1, camada+1):

    for c in camadasIds:
        if c.startswith('PJ_'):
            cnpjs.add(c[3:])
    # if 0: #conCNPJ:
    #     conCNPJaux =conCNPJ
    # else:
    #     conCNPJaux = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    cnpjs = cnpjs.difference(cnpjsInicial)
    #nos = dadosDosNosCNPJs(conCNPJaux, cnpjs, nosaux, dicRazaoSocial, camadasIds)
    #dadosDosNosCNPJs(cnpjs, nosaux, camadasIds=camadasIds, tmp=tmp, con=con)
    dadosDosNosCNPJs(nosaux, camadasIds=camadasIds, tmp=tmp, con=con)

    for c in camadasIds:
        if c.startswith('PJ_'):
            continue
        if c.startswith('PF_'):
            nome = c[15:] #supõe 'PF_12345678901-nome'
            no = {'id': c, 'descricao':nome, 
                    'camada': camadasIds[c]} #, 

        else: #elif c.startswith('EN_'):
            no = {'id': c, 'descricao':'', 
                    'camada': camadasIds[c]} #, 

        nosaux.append(copy.deepcopy(no))    
    dadosDosNosBaseLocal(nosaux, camadasIds, tmp=tmp, con=con)
    nosaux=ajustaLabelIcone(nosaux)
    textoJson={'no': nosaux, 'ligacao':ligacoes, 'mensagem':mensagem} 
    con.close()
    con = None

    #apagaTabelasTemporarias(tmp, camDB)
    return textoJson
#.def camadaLink

# def apagaLog():
#     con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
#     con.execute('DROP TABLE IF EXISTS log_cnpjs;')
#     con.execute('DROP TABLE IF EXISTS log_cpfnomes;')
#     con = None

def cnae_secundariaF(codigos):
    t = ''
    for c in codigos.split(','):
        if c:
            t += f"{c}-{gdic.dicCnae.get(c,'')}; "
    return t.removesuffix('; ')
#.def cnae_secondariaF
   
def jsonDados(cpfcnpjListain:list, bsocios=False):
    '''pegando apenas dados do primeiro item da lista'''
    cids, cnpjs, cpfnomes, outrosIdentificadores, cpfpjnomes = separaEntrada(cpfcnpjListain)
    #con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}",execution_options=gEngineExecutionOptions)
    #con = sqlite3.connect(caminhoDBReceita, uri=True)    

    if cnpjs:
        #dlista = jsonDadosReceita([list(cnpjs)[0],], bsocios)
        dlista = jsonDadosReceita(list(cnpjs), bsocios)
    else:
        dlista = []

    dicDados = {}
    if caminhoDBBaseLocal:
        dicDados = jsonDadosBaseLocalDic(cids) #cpfcnpjIn=cpfcnpjIn) 
        if dicDados:
            nosaux = []
            for n in dlista: 
                if n['id'] in dicDados:
                    daux = copy.deepcopy(dicDados[n['id']])
                    for q,v in daux.items():
                        n[q] = v
                nosaux.append(copy.deepcopy(n))
            sids = {n['id'] for n in dlista}
            for k, d in dicDados.items(): #pegar informação de base local que eventualmente não esteja em baseReceita
                if k not in sids:
                    if not d.get('id'):
                        d['id'] = k
                    nosaux.append(copy.deepcopy(d))
            dlista = nosaux
    #print('jsonDados-fim: ' + time.ctime())   
    #con = None
    #return dlista[0] if dlista else {} #retornando só primeiro
    return dlista
#.def jsonDados

def jsonDadosReceita(cnpjlista, bsocios=False):
    '''dados'''
    
    if bsocios:
        querySocios = '''
            SELECT t.cnpj, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
            FROM estabelecimento tt       
            left join socios t on tt.cnpj=t.cnpj
            LEFT JOIN empresas te on te.cnpj_basico=tt.cnpj_basico
            LEFT JOIN qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
            LEFT JOIN qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
            left join pais tpais on tpais.codigo=t.pais
            where 
        '''
    
        if len(cnpjlista)==1:
            querySocios += 'tt.cnpj=?'
        else:
            querySocios += 'tt.cnpj in ('
            querySocios += ' ?, '*(len(cnpjlista)-1) + '? )'
        querySocios+= ' ORDER BY tt.cnpj, t.nome_socio '
        ##curioso, se fizer só um select ordenando por t.cnpj, t.nome_socio a consulta fica muito lenta
        ##prestar atenção no query plan, que altera quando tem um fica diferente quanto tem mais elementos
    query = '''
        select t.*, te.*, ifnull(tm.descricao,t.nome_cidade_exterior) as municipio_texto, tpais.descricao as pais_, tsimples.opcao_mei
        from estabelecimento t
        
        left join empresas te on te.cnpj_basico=t.cnpj_basico
        left join municipio tm on tm.codigo=t.municipio
        left join simples tsimples on tsimples.cnpj_basico=t.cnpj_basico
        left join pais tpais on tpais.codigo=t.pais
        where ''' 
    if len(cnpjlista)==1:
        query += 't.cnpj=?'
    else:
        query += 't.cnpj in ('
        query += ' ?, '*(len(cnpjlista)-1) + '? )'
    camposPJ = ['cnpj', 'matriz_filial', 'razao_social', 'nome_fantasia', 'data_inicio_atividades', 'situacao_cadastral', 
				'data_situacao_cadastral', 'motivo_situacao_cadastral', 'natureza_juridica', 'cnae_fiscal', 'cnae_secundaria', 'porte_empresa', 'opcao_mei',
				'endereco', 'municipio', 'uf', 'cep', 'nm_cidade_exterior', 'nome_pais', 'nm_cidade_exterior', 'pais',
				'ddd1', 'telefone1', 'ddd2', 'telefone2', 'ddd_fax', 'fax', 'correio_eletronico', 'capital_social'
				]
    
    #con = sqlalchemy.create_engine(f'sqlite:///{caminhoDBReceita}')
    con = sqlite3.connect(caminhoDBReceita, uri=True)    
    con.row_factory=sqlite3.Row
    cur = con.cursor()    
    
    if bsocios:
        dsocios = collections.defaultdict(list)
        for k in con.execute(querySocios, cnpjlista):
            dsocios[k[0]].append(dict(k))

    #for k in con.execute(query, {'cnpjin':cnpjin}):
    dlista = []
    cur.execute(query, cnpjlista)
    #for k in con.execute(query, cnpjlista):    
    for k in cur:
        d = dict(k)  
        # if da:
        #     da['proximo_cnpj'] = d['cnpj']
        #     break
        d['razao_social'] = cpf_cnpj.removeCPFFinal(d['razao_social'])
        capital = d['capital_social'] 
        capital = f"{capital:,.2f}".replace(',','@').replace('.',',').replace('@','.')
        # listalogradouro = [k.strip() for k in [d['logradouro'].strip(), d['numero'], d['complemento'].strip(';'), d['bairro']] if k.strip()]
        # logradouro = ', '.join(listalogradouro)
        # logradouro = re.sub("\s\s+", " ", logradouro)
        listaaux = [k['logradouro'].strip(), k['numero'], k['complemento'].strip(';')]
        if k['uf']!='EX':
            listaaux.append(k['bairro']) #se for empresa no exterior, o bairro as vezes aparece como municipio
        listalogradouro = [j.strip() for j in listaaux if j.strip()]
        logradouro = ', '.join(listalogradouro)
        logradouro = re.sub("\s\s+", " ", logradouro)
        #d['cnpj'] = f"{d['cnpj']} - {'Matriz' if d['matriz_filial']=='1' else 'Filial'}"       
        d['matriz_filial'] = 'Matriz' if d['matriz_filial']=='1' else 'Filial'
        d['data_inicio_atividades'] = ajustaData(d['data_inicio_atividades'])
        d['situacao_cadastral'] = f"{d['situacao_cadastral']} - {gdic.dicSituacaoCadastral.get(d['situacao_cadastral'],'')}"
        d['data_situacao_cadastral'] = ajustaData(d['data_situacao_cadastral']) 
        if d['motivo_situacao_cadastral']=='0':
            d['motivo_situacao_cadastral'] = ''
        else:
            d['motivo_situacao_cadastral'] = f"{d['motivo_situacao_cadastral']}-{gdic.dicMotivoSituacao.get(d['motivo_situacao_cadastral'],'')}"
        #d['cnae_fiscal'] = f"{d['cnae_fiscal']}-{gdic.dicCnae.get(int(d['cnae_fiscal']),'')}"
        d['cnae_fiscal'] = f"{d['cnae_fiscal']}-{gdic.dicCnae.get(d['cnae_fiscal'],'')}"
        d['cnae_secundaria'] = cnae_secundariaF(d['cnae_fiscal_secundaria'])
        d['porte_empresa'] = f"{d['porte_empresa']}-{gdic.dicPorteEmpresa.get(d['porte_empresa'],'')}"
        d['endereco'] = logradouro if logradouro.startswith(d['tipo_logradouro']) else f"{d['tipo_logradouro']} {logradouro}"
        d['capital_social'] = capital 
        d['municipio'] = d['municipio_texto']
        d['opcao_mei'] = d['opcao_mei'] if  d['opcao_mei']  else ''
        d['uf'] = d['uf'] #d['pais_'] if d['uf']=='EX' else d['uf']
        if k['uf']=='EX':
            d['municipio'] = k['bairro']
            d['pais'] = k['pais_']
        d = {k:v for k,v in d.items() if k in camposPJ}
        d['id'] = 'PJ_'+ d['cnpj']
        d['cnpj_formatado'] = f"{d['cnpj'][:2]}.{d['cnpj'][2:5]}.{d['cnpj'][5:8]}/{d['cnpj'][8:12]}-{d['cnpj'][12:]}"
        
        if d['natureza_juridica'] in ('2135', '4120'): #remove empresario individual, produtor rural
            ts = '#INFORMAÇÃO EDITADA#'
            d['endereco'] = ts
            d['telefone1'] = ts
            d['telefone2'] = ''
            d['fax'] = ''
            d['correio_eletronico'] = ts
            d['cep'] = ts
        d['natureza_juridica'] = f"{d['natureza_juridica']}-{gdic.dicNaturezaJuridica.get(d['natureza_juridica'],'')}"   
        if bsocios:
            d['dados_socios'] = dsocios.get(d['cnpj'])
        dlista.append(copy.deepcopy(d))
        #da = copy.deepcopy(d)
    #print('jsonDados-fim: ' + time.ctime())   
    # if proximo==-1:
    #     da['proximo_cnpj'] = cnpjin
    con = None
    return dlista
#.def jsonDadosReceita

def jsonDadosBaseLocalDic(listaIds=None, tmp=None, con=None):  
#def jsonDadosBaseLocal(cpfcnpjIn=None, listaIds=None):    
    if not caminhoDBBaseLocal:
        return {}
    if not listaIds: #and not cpfcnpjIn:
        return {}
    # con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}",execution_options=gEngineExecutionOptions)
    # dftmptable = pd.DataFrame({'id' : list(listaIds)})
    # tmp = tabelaTemp()
    # dftmptable.to_sql(f'{tmp}_idsj', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
    if  con:
        con.execute("ATTACH DATABASE '" + caminhoDBBaseLocal.replace('\\','/') + "' as dlocal") 
    else:
        con, camadasIds_, cnpjs_, cpfnomes_, tmp = criaTabelasTmpParaCamadas(caminhoDBBaseLocal, 'dlocal', listaIds=listaIds, grupo='', prefixo_tabela_temporaria='')    
    #with contextlib.closing(con) as con1:
    if True:
        query = f'''
            select tj.id, tj.json
            from {tmp}_ids t
            inner join dlocal.dadosjson tj on tj.id = t.identificador
                '''
        #dlista = []
        dicLista = {}
        # res = con.execute(query)
        # colsname = [t[0] for t in res.description]
        # #for k in con.execute(query):
        # for k1 in res.fetchall():
        #     k = dict(zip(colsname, k1))
        con.row_factory = sqlite3.Row #para ver registros do sqlite3 como dicionário
        cur = con.cursor()
        cur.execute(query)
        for k in cur:
            try:
                d = json.loads(k['json']) #dict(k)  
            except:
                d = {k['id']:'erro na base'}
            if k['id'] not in dicLista:
                dicLista[k['id']]=copy.deepcopy(d)
            else: #já havia registro de dados com id, sobrepoe os campos novos
                daux = copy.deepcopy(dicLista[k['id']])
                for q,v in d.items():
                    if q not in daux:
                        daux[q] = v
                    else:
                        if daux[q]==d[q] or (v in daux[q]):
                            #daux[q] = daux[q]
                            pass
                        elif daux[q] in v:
                            daux[q] = v
                        else: 
                            if q in ('imagem','cor'): #checa se o valor do item pode ser mesclado. 
                                daux[q] = v
                            else:
                                daux[q] = daux[q] + '; ' + v
                dicLista[k['id']]=copy.deepcopy(daux)
        cur.close()
    #con.execute(f'Drop table if exists {tmp}_idsj')
    #con = None
    return dicLista
#.def jsonDadosBaseLocalDic

def dados_api_cnpj(cnpj, itensFlag):
    cnpjin = cnpj #request.args.get('cnpj', default = '', type = str)

    qr = qteEmpresas_referenciaF()
    dados = {'base_rfb_cnpj_qtde':qr['cnpj_qtde'], 'base_rfb_data_referencia_da_base':qr['data_referencia']}

    if not cnpjin:
        dados['cnpj'] = ''
        return dados   
    cnpj = cnpjin.removeprefix('PJ_').replace('.','').replace('-','').replace('/','').strip()
    cnpj = cpf_cnpj.validar_cnpj(cnpj)
    dados2 = {}
    if cnpj:
        dados2 = jsonDados(['PJ_'+cnpj,], True) #precisa reincluir PJ_ para fazer busca na base local
        dados.update(dados2)
        dados['cnpj_formatado'] = cpf_cnpj.cnpj_formatado(cnpj)
    if not dados2:
        dados['cnpj'] = cnpjin 
    dado_adicional = {key:dados.get(key) for key in itensFlag if dados.get(key)}
    # for key in itensFlag:
    #     if dados.get(key):
    #         dado_adicional[key] = dados[]
    #         htadicional += "<b>" + key + ": </b> "+ dados.get(key)+ "<br>";
    dados['dado_adicional'] = dado_adicional
    return dados
#.def dados_api_cnpj

def dados_consulta_cnpj(request, render_template, itensFlag):
    cnpjListain = request.args.get('cnpj', default = '', type = str)
    bmobile = any(word in request.headers.get('User-Agent','') for word in ['Mobile','Opera Mini','Android'])
    qr = qteEmpresas_referenciaF()
    dadosReferencia = {'cnpj_qtde':qr['cnpj_qtde'], 'data_referencia':qr['data_referencia'],'desktop': not bmobile}
    dadosReferencia['usuario_local'] = (request.remote_addr ==  '127.0.0.1')
    dadosReferencia['textoH2'] = ''
    listaDados = []
    if not cnpjListain:
        dadosReferencia['cnpj'] = ''
        return render_template('dados_cnpj.html', listaDados = listaDados, dadosReferencia=dadosReferencia) 
    dadosReferencia['cnpj'] = cnpjListain 
    cnpjs = set()
    for cnpj_ in cnpjListain.split(';')[:100]:  #faz corte de 100 elementos
        cnpj = cnpj_.removeprefix('PJ_').replace('.','').replace('-','').replace('/','').strip()
        cnpj = cpf_cnpj.validar_cnpj(cnpj)
        if cnpj:
            cnpjs.add(cnpj)
    
    if cnpjs:
        #dados2 = jsonDados('PJ_'+cnpj, True) #precisa reincluir PJ_ para fazer busca na base local
        #lista = []
        for dados1cnpj_ in jsonDados(cnpjs, True): #precisa reincluir PJ_ para fazer busca na base local
            dados1cnpj = copy.deepcopy(dados1cnpj_)
            dado_adicional = {key:dados1cnpj.get(key) for key in itensFlag if dados1cnpj.get(key)}
            dados1cnpj['dado_adicional'] = copy.deepcopy(dado_adicional)      
            listaDados.append(copy.deepcopy(dados1cnpj))
        #dados['lista'] = lista
        #dados['cnpj_formatado'] = 'cnpj_formatado' #cpf_cnpj.cnpj_formatado(cnpj)
    # if not cnpjs:
    #     dadosReferencia['cnpj'] = cnpjListain 
    return render_template('dados_cnpj.html', listaDados = listaDados, dadosReferencia=dadosReferencia) 
#.def dados_consulta_cnpj

def carregaJSONemBaseLocal(nosLigacoes, comentario=''):
    if not caminhoDBBaseLocal:
        return
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBBaseLocal}") #,execution_options=gEngineExecutionOptions)
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
    dftmptable.to_sql('dadosjson', con=con, if_exists='append', index=False) #, dtype=sqlalchemy.types.VARCHAR)
    dftmptable = pd.DataFrame(listaLigacao, columns = ['id1', 'id2', 'descricao','valor', 'comentario'])
    dftmptable.to_sql('links', con=con, if_exists='append', index=False) #, dtype=sqlalchemy.types.VARCHAR)
    con = None
#.def carregaJSONemBaseLocal

def junta(a, separador, b):
    if a and b:
        return a + separador + b
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
    elif valor>=10*1e9:
        v = '{:.0f}'.format(valor/1e9).replace('.',',') + ' Bi'
    elif valor>=1e9:
        v = '{:.1f}'.format(valor/1e9).replace('.',',') + ' Bi'
    elif valor>=10*1e6:
        v = '{:.0f}'.format(valor/1e6).replace('.',',') + ' Mi'
    elif valor>=1e6:
        v = '{:.1f}'.format(valor/1e6).replace('.',',') + ' Mi'
    elif valor>=10000.0:
        v = '{:.0f}'.format(valor/1000).replace('.',',') + ' mil'
    elif valor>=1000.0:
        v = '{:.1f}'.format(valor/1000).replace('.',',') + ' mil'
    else:
        v = '{:.2f}'.format(valor).replace('.',',')
    return v
#.def ajustaValor
        
def ajustaData(d): #aaaammdd para dd/mm/aaaa
    if d:
        return d[-2:]+'/' + d[4:6] + '/' + d[:4]
    else:
        return ''

def dadosParaExportar(dados):    
    #print('dadosParaExportar-inicio: ' + time.ctime())
    #con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
    sids = set()
    for item in dados['no']:
        sids.add(item['id'])
    listaCpfCnpjs = list(sids)
    con, camadasIds_, cnpjs_, cpfnomes_, tmp = criaTabelasTmpParaCamadas(caminhoDBReceita, 'cnpj', listaIds=listaCpfCnpjs, grupo='', tabelasACriar=['ids', 'cpfpjnomes'])
    querysocios = f'''
                SELECT * FROM -- distinct * from
    				(SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                     t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
                FROM cnpj.socios t
                --INNER JOIN tmp_cnpjs tl ON  tl.cnpj = t.cnpj
                INNER JOIN {tmp}_cpfpjnomes tl ON  tl.cpfpj = t.cnpj
                left join cnpj.estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN cnpj.empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN cnpj.qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join cnpj.pais tpais on tpais.codigo=t.pais
                where tl.nome=''
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao,
                    t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
                FROM cnpj.socios t
                INNER JOIN {tmp}_cpfpjnomes tl ON tl.cpfpj = t.cnpj_cpf_socio
                left join cnpj.estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN cnpj.empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN cnpj.qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join cnpj.pais tpais on tpais.codigo=t.pais
                where tl.nome=''
                UNION
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                    t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_, t.faixa_etaria
                FROM cnpj.socios t
                INNER JOIN {tmp}_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
                left join cnpj.estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN cnpj.empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN cnpj.qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join cnpj.pais tpais on tpais.codigo=t.pais
                where tn.nome<>''
                /* XXX
                UNION --xxx inclui responsavel por socio
                SELECT t.cnpj, te.razao_social, t.cnpj_cpf_socio, t.nome_socio, sq.descricao as cod_qualificacao, 
                    t.data_entrada_sociedade, t.pais, tpais.descricao as pais_, t.representante_legal, t.nome_representante, t.qualificacao_representante_legal, sq2.descricao as qualificacao_representante_legal_,  t.faixa_etaria
                --SELECT t.representante_legal as origem, t.nome_representante as nome_origem, t.cnpj_cpf_socio, t.nome_socio, 'rep-sócio-' || sq.descricao as cod_qualificacao
                FROM cnpj.socios t
                INNER JOIN {tmp}_cpfpjnomes tn ON tn.nome= t.nome_socio AND tn.cpfpj=t.cnpj_cpf_socio
                left join cnpj.estabelecimento tt on tt.cnpj=t.cnpj
                LEFT JOIN cnpj.empresas te on te.cnpj_basico=tt.cnpj_basico
                LEFT JOIN cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
                LEFT JOIN cnpj.qualificacao_socio sq2 ON sq2.codigo=t.qualificacao_representante_legal
                left join cnpj.pais tpais on tpais.codigo=t.pais
                where t.nome_representante<>'' */
            )
                ORDER BY nome_socio
            '''

    queryempresas = f'''
                SELECT te.*, tm.descricao as municipio_, tt.uf as uf_, pais.descricao as pais_, tt.*
                FROM {tmp}_cpfpjnomes tp 
                left join cnpj.estabelecimento tt on tt.cnpj=tp.cpfpj
                LEFT JOIN cnpj.empresas te on te.cnpj_basico=tt.cnpj_basico
                left join cnpj.municipio tm on tm.codigo=tt.municipio
                left join cnpj.pais on pais.codigo=tt.pais
                where tp.nome=''
            '''
    con.row_factory = sqlite3.Row #para ver registros do sqlite3 como dicionário
    cur = con.cursor()
    #from io import BytesIO
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dfe=pd.read_sql_query(queryempresas, con)
        dfe['razao_social'] =  dfe['razao_social'].apply(lambda t: cpf_cnpj.removeCPFFinal(t))
        dfe['capital_social'] = dfe['capital_social'].apply(lambda capital: f"{capital/100:,.2f}".replace(',','@').replace('.',',').replace('@','.') if capital else '')  
        dfe['matriz_filial'] = dfe['matriz_filial'].apply(lambda x:'Matriz' if x=='1' else 'Filial')
        dfe['data_inicio_atividades'] = dfe['data_inicio_atividades'].apply(ajustaData)
        dfe['situacao_cadastral'] = dfe['situacao_cadastral'].apply(lambda x: gdic.dicSituacaoCadastral.get(x,'') if x else '')
        dfe['data_situacao_cadastral'] =  dfe['data_situacao_cadastral'].apply(ajustaData)
        dfe['motivo_situacao_cadastral'] = dfe['motivo_situacao_cadastral'].apply(lambda x: x + '-' + gdic.dicMotivoSituacao.get(x,'') if x else '')
        dfe['natureza_juridica'] = dfe['natureza_juridica'].apply(lambda x: x + '-' + gdic.dicNaturezaJuridica.get(x,'') if x else 11)
        dfe['cnae_fiscal'] = dfe['cnae_fiscal'].apply(lambda x: x +'-'+ gdic.dicCnae.get(x,'') if x else '')
        dfe['cnae_fiscal_secundaria'] = dfe['cnae_fiscal_secundaria'].apply(lambda x: cnae_secundariaF(x))
        dfe['porte_empresa'] = dfe['porte_empresa'].apply(lambda x: x+'-' + gdic.dicPorteEmpresa.get(x,'') if x else '')

        cols = [c for c in dfe.columns if not c.startswith('cnpj')]
        cols.insert(0, 'cnpj')
        dfe[cols].to_excel(writer, merge_cells = False, sheet_name = "Empresas", index=False, freeze_panes=(1,0))
    
        
        #coloca dados em duas colunas
        #dfe.T.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "EmpresasT", index=True, freeze_panes=(0,1))
        lempresas = [['N','coluna','dado']]
        for k, r in dfe.iterrows():
            lempresas.append([k+1, 'CNPJ', r['cnpj']])
            for c,d  in r.items():
                if not c.startswith('cnpj'):
                    lempresas.append([k+1, c, d])
            lempresas.append(['', '',''])    
        pd.DataFrame(lempresas).to_excel(writer, header=False, sheet_name = "Empresas_", index=False, freeze_panes=(1,0))
        
        dfs=pd.read_sql_query(querysocios, con)
        dfs.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "Socios", index=False, freeze_panes=(1,0))
        dfin = pd.DataFrame.from_dict(dados['no']) #,orient='index',  columns=['id', 'descricao', 'nota', 'camada', 'cor', 'posicao', 'pinado', 'imagem', 'logradouro', 'municipio', 'uf', 'cod_nat_juridica', 'situacao_ativa', 'tipo', 'sexo'])
        dfin.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "identificadores", index=False, freeze_panes=(1,0))
    
        ligacoes = []
        for lig in dados['ligacao']:
            ligacoes.append([lig['origem'], lig['destino'], lig['label'], lig['tipoDescricao']])
        dflig = pd.DataFrame(ligacoes, columns=['origem','destino','ligacao','tipo_ligacao'])
        dflig.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "ligacoes", index=False, freeze_panes=(1,0))
     
    #writer.close()
    output.seek(0)
    #apagaTabelasTemporarias(tmp)
    #con.close()
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
            imagem = 'icone-grafo-id.png'
        elif prefixo=='UG':
            imagem = 'icone-grafo-ug.png'
        else:
            imagem = 'icone-grafo-id.png' #caso genérico
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

def mensagemInicial():
    mensagemInicial = '' 
    numeroEmpresas = qteEmpresas_referenciaF()['cnpj_qtde'] #numeroDeEmpresasNaBase()
    if numeroEmpresas:
        tnumeroEmpresas = format(numeroEmpresas,',').replace(',','.')
        if numeroEmpresas>40000000: #no código do template, dois pontos será substituida por .\n
            #mensagemInicial += f'''\nA base tem {tnumeroEmpresas} empresas, data de referência: ''' + qteEmpresas_referenciaF()['data_referencia'] #config.referenciaBD
            mensagemInicial += f'''\nA base cnpj tem {tnumeroEmpresas} empresas. '''+config.referenciaBD
    return mensagemInicial

# @lru_cache(8)
# def numeroDeEmpresasNaBase(): 
#     #pega qtde de registros na tabela _referencia para acelerar o início da rotina
#     if not caminhoDBReceita:
#         return 0
#     con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}", execution_options=gEngineExecutionOptions)
#     try:
#         res = con.execute("select valor from _referencia where referencia='cnpj_qtde'").fetchone()[0]
#         r = int(res)
#     except:
#         r = 0
#     if not r:
#         print('select count(*) as contagem from estabelecimento')
#         r = con.execute('select count(*) as contagem from estabelecimento').fetchone()[0]
#     return r

@lru_cache(8)
def qteEmpresas_referenciaF(): 
    #pega qtde de registros na tabela _referencia para acelerar o início da rotina
    con = sqlalchemy.create_engine(f"sqlite:///{caminhoDBReceita}")
    res = con.execute("select valor from _referencia where referencia='cnpj_qtde'").fetchone()[0]
    cnpj_qtde = int(res)
    data_referencia = con.execute("select valor from _referencia where referencia='CNPJ'").fetchone()[0]
    con = None
    return {'cnpj_qtde':cnpj_qtde, 'data_referencia':data_referencia}
#.def qteEmpresas_referenciaF(): 

if __name__ == '__main__':
    #apagaTabelasTemporarias('tmp_') #apaga todas as tabelas tmp_ 
    pass
