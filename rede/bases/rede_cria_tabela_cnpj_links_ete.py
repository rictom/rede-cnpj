# -*- coding: latin-1 -*- 

"""
rictom
2022-10-21 - alterado, removido filtro de empresas ativas para reduzir tempo de execução.
teste com dask na rotina de endereços, tem desempenho igual ao pandas (um pouco mais lento)

2022-11-24 código alterado para usar sqlite3
"""


import pandas as pd, sqlalchemy, sqlite3
#import dask.dataframe as dd
import re, time, string, unicodedata, sys, os #, py7zr
gstep = 2000000 #quantidade de registros por loop

offsetGlobal='0' #offset do sql, string
rodaSo1bloco=False
caminhobase = '' #alterar caminho
caminhoarquivolog = 'logimportarenderecos.txt' 

camDbSqliteBaseCompleta = r"cnpj.db" #aqui precisa ser a tabela completa

camDBSaida = 'cnpj_links_ete.db'

bMemoria = False #se tiver menos de 16GB de RAM, colocar bMemoria=False #na memoria, 1h11m, no hd, 1h17m

def executaSql(con, sqlseq):
    kt = len(sqlseq.split(';'))
    for k,sql in enumerate(sqlseq.split(';'), 1):
        print('-'*30)
        print(time.ctime(), f' - executando parte: {k}/{kt}')
        print(sql)
        con.execute(sql) 
#.def executaSql
    
def soCaracteres(data):
    '''remove acentuacao e mantém só os caracteres'''
    if data is None:
        return ''
    t=''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.printable)
    return re.sub('\W',' ',t)

dicAbreviaturas = {
        "A":"AREA",
        "AC":"ACESSO",
        "ACA":"ACAMPAMENTO",
        "AD":"ADRO",
        "AE":"AREA ESPECIAL",
        "AER":"AEROPORTO",
        "AL":"ALAMEDA",
        "AR":"AREA",
        "ART":"ARTERIA",
        "AT":"ALTO",
        "ATL":"ATALHO",
        "AV":"AVENIDA",
        "AVEN":"AVENIDA",
        "AVC":"AVENIDA CONTORNO",
        "BAL":"BALNEARIO",
        "BC":"BECO",
        "BEL":"BELVEDERE",
        "BL":"BLOCO",
        "BSQ":"BOSQUE",
        "BVD":"BOULEVARD",
        "BX":"BAIXA",
        #"C":"CAIS",
        "CAL":"CALCADA",
        "CAM":"CAMINHO",
        "CAN":"CANAL",
        "CH":"CHACARA",
        "CHA":"CHAPADAO",
        "CIR":"CIRCULAR",
        "CJ":"CONJUNTO",
        "CMP":"COMPLEXO VIARIO",
        "CND":"CONDOMINIO",
        "COL":"COLONIA",
        "CON":"CONDOMINIO",
        "COR":"CORREDOR",
        "CPO":"CAMPO",
        "CRG":"CORREGO",
        "DSC":"DESCIDA",
        "DSV":"DESVIO",
        "DT":"DISTRITO",
        "ENT":"ENTRADA PARTICULAR",
        "EQ":"ENTREQUADRA",
        "ESC":"ESCADA",
        "ESP":"ESPLANADA",
        "EST":"ESTRADA",
        "ETC":"ESTACAO",
        "ETD":"ESTADIO",
        "ETN":"ESTANCIA",
        "EVD":"ELEVADA",
        "FAV":"FAVELA",
        "FAZ":"FAZENDA",
        "FER":"FERROVIA",
        "FNT":"FONTE",
        "FRA":"FEIRA",
        "FTE":"FORTE",
        #"GAL":"GALERIA",
        "GJA":"GRANJA",
        "HAB":"HABITACIONAL",
        "IA":"ILHA",
        "JD":"JARDIM",
        "LAD":"LADEIRA",
        "LD":"LADEIRA",
        "LG":"LAGO",
        "LGA":"LAGO",
        "LGO":"LARGO",
        "LOT":"LOTEAMENTO",
        "LRG":"LARGO",
        "MNA":"MARINA",
        "MOD":"MODULO",
        "MRO":"MORRO",
        "MTE":"MONTE",
        "NUC":"NUCLEO",
        "OTR":"OUTROS",
        "PAR":"PARALELA",
        "PAS":"PASSARELA",
        "PAT":"PATIO",
        "PC":"PRACA",
        "PCA":"PRACA",
        "PDA":"PARADA",
        "PDO":"PARADOURO",
        "PNT":"PONTA",
        "PQ":"PARQUE",
        "PR":"PRAIA",
        "PRL":"PROLONGAMENTO",
        "PRQ":"PARQUE",
        "PSA":"PASSARELA",
        "PSG":"PASSAGEM",
        "PTE":"PONTE",
        "PTO":"PATIO",
        "Q":"QUADRA",
        "QD":"QUADRA",
        "QTA":"QUINTAS",
        "R":"RUA",
        "RAM":"RAMAL",
        "RDV":"RODOVIA",
        "REC":"RECANTO",
        "RER":"RETIRO",
        "RES":"RESIDENCIAL",
        "RET":"RETA",
        "RMP":"RAMPA",
        "ROD":"RODOVIA",
        "ROT":"ROTULA",
        "RTN":"RETORNO",
        "RTT":"ROTATORIA",
        "SIT":"SITIO",
        "SRV":"SERVIDAO",
        "ST":"SETOR",
        "SUB":"SUBIDA",
        "TCH":"TRINCHEIRA",
        "TER":"TERMINAL",
        "TR":"TRAVESSA",
        "TRV":"TREVO",
        "TV":"TRAVESSA",
        "UNI":"UNIDADE",
        "V":"VIA",
        "VAL":"VALE",
        "VD":"VIADUTO",
        "VER":"VEREDA",
        "VEV":"VIELA",
        "VEX":"VIAEXPRESSA",
        "VIA":"VIA",
        "VL":"VILA",
        "VLA":"VIELA",
        "VLE":"VALE",
        "VRT":"VARIANTE",
        #termos que não estão na tabela logradouro cnpj
        "ALM":"ALMIRANTE",
        "AND":"ANDAR",
        "AP":"APARTAMENTO",
        "APART":"APARTAMENTO",
        "APT":"APARTAMENTO",
        "APTO":"APARTAMENTO",
        "BL":"BLOCO",
        "BRIG":"BRIGADEIRO",
        "CAP":"CAPITAO",
        "CASA":"",
        "CS":"",
        "CEL":"CORONEL",
        "CMTE":"COMANDANTE",
        #"CON":"CONEGO",
        "CONJ":"CONJUNTO",
        "DA":"",
        "DAS":"",
        "DE":"",
        "DEP":"DEPUTADO",
        "DO":"",
        "DOS":"",
        "DR":"DOUTOR",
        "ED":"EDIFICIO",
        "EDF":"EDIFICIO",
        "ENG":"ENGENHEIRO",
        "ETR":"ESTRADA",
        "FDS":"FUNDOS",
        "FR":"FREI",
        "GAL":"GENERAL",
        "GEN":"GENERAL",
        "GLEB":"GLEBA",
        "LIN":"LINHA",
        "LINH":"LINHA",
        "LJ":"LOJA",
        "LT":"LOTE",
        "MAL":"MARECHAL",
        "MIN":"MINISTRO",
        "MJ":"MAJOR",
        "MONS":"MONSENHOR",
        #"OTR":"",
        "OTRS":"",
        "OUTROS":"",
        "PAS":"PASSARELA",
        "PE":"PADRE",
        "POV":"POVOADO",
        "PRAC":"PRACA",
        "PRC":"PRACA",
        "PRES":"PRESIDENTE",
        "PROF":"PROFESSOR",
        "PROFA":"PROFESSORA",
        "PTO":"PATIO",
        #"QD":"QUADRA",
        "QDA":"QUADRA",
        "QDRA":"QUADRA",
        "SARG":"SARGENTO",
        "SEN":"SENADOR",
        "SL":"SALA",
        "SN":"",
        "STA":"SANTA",
        "STO":"SANTO",
        "TEN":"TENENTE",
    }

regex1 = re.compile('([0-9]+)[.]([0-9]+)')
regex2 = re.compile('([A-Z])(\d)')
regex3 = re.compile('(\d)([A-Z])')
tremovenumerosespacos = str.maketrans(string.ascii_letters,string.ascii_letters,string.digits+' ')

def normalizaEndereco(enderecoIn, ignoraEnderecoSoComNumeros = True, ignoraEnderecoSemNumeros = True):
    '''ajusta endereco, removendo ponto de números, removendo s/n, expandindo siglas comuns
       como R, AV. No final, ordena o resultado'''
    #junta números com ponto, por exemplo, 3.222 fica 3222
    if 1:
        enderecoAux = re.sub('([0-9]+)[.]([0-9]+)', '\\1\\2', enderecoIn).upper() #remove ponto de número
        enderecoAux = re.sub('([A-Z])(\d)', '\\1 \\2', enderecoAux) #separa letra de dígito
        enderecoAux = re.sub('(\d)([A-Z])', '\\1 \\2', enderecoAux) #separa digito de letra
    else: #fazendo compile fica mais lento!
        enderecoAux = regex1.sub('\\1\\2', enderecoIn).upper() #remove ponto de número
        enderecoAux = regex2.sub('\\1 \\2', enderecoAux) #separa letra de dígito
        enderecoAux = regex3.sub('\\1 \\2', enderecoAux) #separa digito de letra        
    
    #enderecoAux = ' ' + enderecoAux.upper() + ' '
    enderecoAux = " %s " %(soCaracteres(enderecoAux))
    enderecoAux = enderecoAux.replace(' S N ',' ')
    # remove zeros e zeros à esquerda
    #enderecoAux = re.sub(' 0+([^ ]*) ', ' \\1 ', enderecoAux)
    lendereco = enderecoAux.split()
    if len(lendereco)==0:
        return ''
    #if lendereco[0]=='R': lendereco[0]='RUA'
    #elif lendereco[0]=='Q': lendereco[0]='QUADRA'
    if lendereco[0]=='LOC':
        lendereco[0]=''
    # remove palavras duplicadas
    palavras = set()
    #letras = set()
    numeros = []
    #tremovenumerosespacos = str.maketrans(string.ascii_letters,string.ascii_letters,string.digits+' ')
    for k,pedaco in enumerate(lendereco):
        pedacoAjustado = pedaco
        if pedaco in dicAbreviaturas:
            if len(pedaco)>1 or k<=1: #alguns casos como R ou Q só se for no começo do nome
                pedacoAjustado = dicAbreviaturas[pedaco]
        if pedacoAjustado: # !='':
            if pedacoAjustado.isdigit():
                #pedacoAjustado = re.sub('0+([^ ]*)', '\\1', pedacoAjustado) #remove zero à esquerda (dá erro)
                pedacoAjustado = pedacoAjustado.lstrip('0') #remove zero à esquerda
                if pedacoAjustado: # != '':
                    numeros.append(pedacoAjustado)
            else:
                palavras.add(pedacoAjustado)
    palavrasOrdenadas = sorted(list(palavras)) #<---------------------Ordenação, pode ser removido
    if ignoraEnderecoSemNumeros: #há muitos endereços sem número
        if not numeros:
            return ''
    if ignoraEnderecoSoComNumeros and not palavrasOrdenadas:
        return ''
    palavrasOrdenadas.extend(numeros)
    endereco = ' '.join(palavrasOrdenadas)
    return endereco
    if ignoraEnderecoSoComNumeros:
        if endereco.translate(tremovenumerosespacos) == '': #remove numeros e espaço
        #if endereco.replace(' ','').isdigit():
            return ''
        else:
            return endereco
    else:
        return endereco
#.def NormalizaEndereco

# def baixa_enderecos_cnpj_Dask(bpergunta=True):
#     '''rotina em dask leva o mesmo tempo que usando em pandas'''
#     #rodaSo1bloco = True
#     print(time.ctime(), 'INICIANDO baixa_enderecos_cnpj-------------------------')
#     #conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}") #, execution_options={"sqlite_raw_colnames": True})
#     conBaseCompleta = sqlite3.connect(camDbSqliteBaseCompleta)
#     query = '''
#                 create table endereco_aux AS
#                 SELECT t.cnpj, cast(t.cnpj_basico as int) as cnpj_basico,
#                 situacao_cadastral as situacao,
#                 --tipo_logradouro, logradouro, numero, complemento, bairro,
#                 (logradouro || ' ' || numero || ' ' || complemento) as logradouroNumeroComplemento,
#                 ifnull(tm.descricao,'') as municipio, t.uf
#                 FROM estabelecimento t 
#                 left join municipio tm on tm.codigo=t.municipio
#             ''' #pode haver empresas fora da base de teste            
#     print(time.ctime(), 'criando tabela endereco_aux')    
#     conBaseCompleta.execute('DROP TABLE IF EXISTS endereco_aux')    
#     conBaseCompleta.execute(query)
#     print(time.ctime(), 'criando tabela endereco_aux. Fim.') 

#     conBaseCompleta.commit()
#     conBaseCompleta = None

#     print(time.ctime(), 'dask enderecos')
#     pend = dd.read_sql_table('endereco_aux', f"sqlite:///{camDbSqliteBaseCompleta}", 
#                              index_col='cnpj_basico')
#     pend['endereco'] = pend['logradouroNumeroComplemento'].apply(normalizaEndereco, meta=('logradouroNumeroComplemento', 'object')) +  '-' + pend['municipio'] + '-'+pend['uf']
#     dftmptable = pend[['cnpj','endereco','situacao']]
#     dftmptable.to_sql('endereco', f"sqlite:///{camDBSaida}", if_exists='append', dtype=sqlalchemy.types.String)
#     #conBaseCompleta.execute('DROP TABLE IF EXISTS endereco_aux')  
#     #conBaseCompleta.commit()
#     conBaseCompleta.close()
#     print(time.ctime(), 'ROTINA TERMINOU ')
# #.def baixa_enderecos_cnpj_Dask():

def baixa_enderecos_cnpj(bpergunta, conBaseCompleta, conEnderecoNormalizado): #usando pandas
    print(time.ctime(), 'INICIANDO baixa_enderecos_cnpj-------------------------')
    queryBase = '''
                SELECT t.rowid, t.cnpj, 
                situacao_cadastral as situacao,
                --tipo_logradouro, logradouro, numero, complemento, bairro,
                (logradouro || ' ' || numero || ' ' || complemento) as logradouroNumeroComplemento,
                ifnull(tm.descricao,t.nome_cidade_exterior) as municipio, 
                IIF(t.uf<>'EX', t.uf , tpais.descricao ) as uf -- case when t.uf<>'EX' then t.uf else tpais.descricao end as uf
                FROM estabelecimento t 
                left join municipio tm on tm.codigo=t.municipio
                left join pais tpais on tpais.codigo=t.pais
            ''' #pode haver empresas fora da base de teste            
    inicio = 0
    kregistros = gstep

    numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    ultimo_rowid = 0
    
    while True:
        print(inicio, time.ctime())
        #query = queryBase + f' LIMIT {inicio},{kregistros}' 
        if ultimo_rowid==0:
            query = queryBase + f' ORDER BY t.rowid LIMIT {kregistros}' 
        else:
            query = queryBase + f' WHERE t.rowid> {ultimo_rowid} ORDER BY t.rowid LIMIT {kregistros}' 
        pend = pd.read_sql(query, conBaseCompleta, index_col=None)
        if not pend.shape[0]:
            break
        pend['endereco_aux'] = pend['logradouroNumeroComplemento'].apply(normalizaEndereco) 
        pend['endereco'] = pend['endereco_aux'] +  '-' + pend['municipio'] + '-'+pend['uf']
        ultimo_rowid = pend['rowid'].max()
        pend2 = pend[pend['endereco_aux'] != '' ]
        dftmptable = pend2[['cnpj','endereco','situacao']]
        dftmptable.to_sql('endereco', conEnderecoNormalizado, if_exists='append')#, dtype=sqlalchemy.types.String)
        
        inicio += kregistros
        if inicio>numeroDeRegistros:
            break
        if rodaSo1bloco:
            break 
    #conBaseCompleta.commit()
    conEnderecoNormalizado.commit()            
    #conBaseCompleta.close()
    #conEnderecoNormalizado.close()
    print(time.ctime(), 'ROTINA TERMINOU ')
#.def baixa_enderecos_telefones_cnpj():

def ajustaTelefone(telefoneIn):
    #if printDebug: print datetime.now(), 'Telefone '+telefoneIn+ '  segunda_parte   '+telefoneIn[-7:]
    if telefoneIn is None or telefoneIn=='' or telefoneIn == '0 0' or \
       telefoneIn[-7:] in ('0000000','1111111','2222222','3333333','4444444','5555555','6666666','7777777','8888888','9999999'):
        return ''
    telefoneIn = ' '.join(telefoneIn.split()).strip() #remove espaços duplicados
    if ' ' in telefoneIn:
        pos = telefoneIn.find(' ')
        ddd,t = telefoneIn[:pos], re.sub(' ','',telefoneIn[pos:]) #telefoneIn.split(' ')
        if len(ddd)>2:
            ddd = ddd[-2:]
        if len(t)<4: #muito curto, deve estar errado
            return ''
        return ddd + ' ' + t
    #else: #as vezes o cnpj retorna o telefone junto com o ddd - nesse caso não faz ajuste nenhum
    elif len(telefoneIn)<9: #muito curto, deve estar errado
        return ''
    else:
        return telefoneIn
    
def baixa_telefone_cnpj(bpergunta, conBaseCompleta, conTelefone):
    print(time.ctime(), 'INICIANDO baixa_telefone_cnpj-------------------------')
    queryBase = '''
                SELECT ROWID, cnpj, situacao_cadastral as situacao, ddd1, telefone1, ddd2, telefone2, ddd_fax, fax --, email
                FROM estabelecimento t
                -- where situacao='02' -- só ativas -- <>'08'ignora baixa
            ''' #pode haver empresas fora da base de teste
            
    inicio = 0
    kregistros = gstep   

    ultimo_rowid = 0
    
    numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    conTelefone.execute('DROP table if exists telefone')
    while True:
        print(inicio, time.ctime())
        #query = queryBase + f' LIMIT {inicio},{kregistros}' 
        if ultimo_rowid==0:
            query = queryBase + f' ORDER BY rowid LIMIT {kregistros}' 
        else:
            query = queryBase + f' WHERE rowid> {ultimo_rowid} ORDER BY rowid LIMIT {kregistros}' 
        lista = []
        #result = conBaseCompleta.execute(query ) #quando ocorre erro, o curso.execute retorna um texto
        for k in conBaseCompleta.execute(query):
           #cnpj = k['cnpj'] 
           situacao = k['situacao']
           t1 = ajustaTelefone(k['ddd1']+ ' ' + k['telefone1'])
           t2 = ajustaTelefone(k['ddd2']+ ' ' + k['telefone2'])
           t3 = ajustaTelefone(k['ddd_fax']+ ' ' + k['fax'])
           ultimo_rowid = max(ultimo_rowid, k['rowid'])
           settel = set([t1, t2, t3])
           for tel in settel:
               if tel:
                   lista.append([k['cnpj'], tel, situacao])
        if lista:
            dftmptable = pd.DataFrame(lista, columns=['cnpj','telefone', 'situacao'])
            dftmptable.to_sql('telefone', conTelefone, if_exists='append', index=False)#, dtype=sqlalchemy.types.String)

        inicio += kregistros
        if inicio>numeroDeRegistros:
            break
        if rodaSo1bloco:
            break
    conTelefone.commit()
    print(time.ctime(), 'ROTINA TERMINOU ')
#.def def baixa_telefone_cnpj():

def ajusta_email(emailin):
    #import re
    #regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
    #if(re.search(regex,email)): 
        #valid email
    if emailin.startswith("'"):
        emailin = emailin[1:]
    if emailin.endswith("'"):
         emailin=emailin[:-1]
    if '@' not in emailin:
        return ''
    return emailin.strip().lower()

def baixa_email_cnpj(bpergunta, conBaseCompleta, conTelefone):
    print(time.ctime(), 'INICIANDO baixa_email_cnpj-------------------------')
    queryBase = '''
                SELECT ROWID, cnpj, situacao_cadastral as situacao, -- ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, num_fax --,
                correio_eletronico as email
                FROM estabelecimento t
                -- where situacao='02' -- só ativas -- <>'08'ignora baixa
            ''' #pode haver empresas fora da base de teste
            
    inicio = 0
    kregistros = gstep
    ultimo_rowid = 0

    numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    conTelefone.execute('DROP table if exists email')
    while True:
        print(inicio, time.ctime())
        #query = queryBase + f' LIMIT {inicio},{kregistros}' 
        if ultimo_rowid==0:
            query = queryBase + f' ORDER BY rowid LIMIT {kregistros}' 
        else:
            query = queryBase + f' WHERE rowid> {ultimo_rowid} ORDER BY rowid LIMIT {kregistros}' 
        lista = []
        #result = conBaseCompleta.execute(query ) #quando ocorre erro, o curso.execute retorna um texto
        for k in conBaseCompleta.execute(query):
           cnpj = k['cnpj'] 
           situacao = k['situacao']
           email = ajusta_email(k['email'])
           if email:
               lista.append([cnpj, email, situacao])
           ultimo_rowid = max(ultimo_rowid, k['rowid'])
        if lista:
            dftmptable = pd.DataFrame(lista, columns=['cnpj','email', 'situacao'])
            dftmptable.to_sql('email', conTelefone, if_exists='append', index=False)# , dtype=sqlalchemy.types.String)

        inicio += kregistros
        if inicio>numeroDeRegistros:
            break
        if rodaSo1bloco:
            break
    #conBaseCompleta.commit()
    conTelefone.commit()
    #conBaseCompleta.close()
    #conTelefone.close()
    print(time.ctime(), 'ROTINA TERMINOU ')
#.def def baixa_email_cnpj():
    
def agrupa_cnpj_por_tipo(tipo, con):
    camDB = camDBSaida #"ete.db"
    #2022-10-21 filtro será ignorado, para reduzir tempo de execução
    if tipo=='endereco':
        #con = sqlalchemy.create_engine(f"sqlite:///{camDB}", execution_options={"sqlite_raw_colnames": True})
        #con = sqlite3.connect(camDB)
        nomeid = 'cnpj'
        coluna = 'endereco'
        filtro = 'ativas'
        filtroSQL = "situacao='02'"
        tabela = 'endereco'
        prefixo = 'EN_'
        prefixoNomeid = 'PJ_'
    elif tipo=='telefone':
        #con = sqlalchemy.create_engine(f"sqlite:///{camDB}", execution_options={"sqlite_raw_colnames": True})
        #con = sqlite3.connect(camDB)
        nomeid = 'cnpj'
        coluna = 'telefone'
        filtro = 'ativas'
        filtroSQL = "situacao='02'"
        tabela = 'telefone'
        prefixo = 'TE_'
        prefixoNomeid = 'PJ_'
    elif tipo=='email':
        #con = sqlalchemy.create_engine(f"sqlite:///{camDB}", execution_options={"sqlite_raw_colnames": True})
        #con = sqlite3.connect(camDB)
        nomeid = 'cnpj'
        coluna = 'email'
        filtro = 'ativas'
        filtroSQL = "situacao='02'"
        tabela = 'email'
        prefixo = 'EM_'
        prefixoNomeid = 'PJ_'
    else:
        print('tipo não definido')
        return
    
    sqlseq = f'''
        CREATE INDEX idx_{tabela}_{coluna} ON {tabela} ({coluna});

        create table {tabela}_contagem AS 
        select {coluna}, count({coluna}) as contagem
        from {tabela}
        group by {coluna}
        having count({coluna})>1;

        CREATE UNIQUE INDEX "idx_{tabela}_contagem" ON "{tabela}_contagem" ("{coluna}"); 

        create table link_{tabela} AS
        select "{prefixoNomeid}" || t.{nomeid} as id1, "{prefixo}"|| t.{coluna} as id2, '{coluna}' as descricao, tc.contagem as valor
        from {tabela}_contagem tc
        inner join {tabela} t on tc.{coluna}=t.{coluna};      
    '''
    
    sql_removido = '''        
        
        CREATE table {tabela}_contagens AS
        select t.{nomeid}, t.{coluna}, tc.contagem
        from {tabela} t
        inner join {tabela}_contagem tc on tc.{coluna}=t.{coluna};
    
        -- CREATE INDEX "idx_{tabela}_contagens" ON "{tabela}_contagens" (	"{coluna}");
        -- CREATE INDEX "idx_{tabela}_contagens_{nomeid}" ON "{tabela}_contagens" ("{nomeid}");
        
        create table link_{tabela} AS
        select "{prefixoNomeid}" || {nomeid} as id1, "{prefixo}"|| {coluna} as id2, '{coluna}' as descricao, contagem as valor
        from {tabela}_contagens;
        
        -- CREATE INDEX "ix_link_{tabela}_id1" ON "link_{tabela}" ("id1");
        -- CREATE INDEX "ix_link_{tabela}_id2" ON "link_{tabela}" ("id2");
    '''

    executaSql(con, sqlseq)
    con.commit()
    #con.close()
    print(time.ctime(), ' fim sqlseq')


def juntaTabelasETE1(con):
    sqlseq =     '''
    create table link_ete as
    select id1, id2, 'end' as descricao, valor
    from link_endereco;
    insert into link_ete 
    select id1, id2, 'tel' as descricao, valor
    from link_telefone;
    insert into link_ete 
    select id1, id2, 'email' as descricao, valor
    from link_email;
    CREATE INDEX "ix_link_ete_id1" ON "link_ete" ("id1");
    CREATE INDEX "ix_link_ete_id2" ON "link_ete" ("id2");
    drop table link_endereco;
    drop table link_telefone;
    drop table link_email;
    drop table email;
    drop table email_contagem;
    drop table email_contagens;
    drop table endereco;
    drop table endereco_contagem;
    drop table endereco_contagens;
    drop table telefone;
    drop table telefone_contagem;
    drop table telefone_contagens;
    '''
    print(time.ctime(), 'juntaTabelasETE: INICIANDO -------------------------')
    #conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}", execution_options={"sqlite_raw_colnames": True})
    
    #con = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
    #con = sqlite3.connect(camDBSaida)
    #conETE.execute(sqlseq)
    ktotal = len(sqlseq.split(';'))
    for k,sql in enumerate(sqlseq.split(';')):
        print('-'*30)
        print(time.ctime(), f'-executando parte:{k}/{ktotal}')
        print(sql)
        con.execute(sql)
    print(time.ctime(), ' fim sqlseq')
    print(time.ctime(), 'juntaTabelasETE: Fim. ')
    con.commit()
    #con.close()
#.def juntaTabelasETE1():

def juntaTabelasETE(con, camDBSaida):
    
    con.execute("ATTACH DATABASE '" + camDBSaida.replace('\\','/') + "' as dbFinal") 
    sqlseq =     '''
    create table dbFinal.link_ete as
    select id1, id2, 'end' as descricao, valor
    from link_endereco;
    insert into dbFinal.link_ete 
    select id1, id2, 'tel' as descricao, valor
    from link_telefone;
    insert into dbFinal.link_ete 
    select id1, id2, 'email' as descricao, valor
    from link_email;
    '''
    
    sql_removido = '''
    --CREATE INDEX "ix_link_ete_id1" ON "dbFinal.link_ete" ("id1");
    --CREATE INDEX "ix_link_ete_id2" ON "dbFinal.link_ete" ("id2");
    drop table link_endereco;
    drop table link_telefone;
    drop table link_email;
    drop table email;
    drop table email_contagem;
    drop table email_contagens;
    drop table endereco;
    drop table endereco_contagem;
    drop table endereco_contagens;
    drop table telefone;
    drop table telefone_contagem;
    drop table telefone_contagens;
    '''
    print(time.ctime(), 'juntaTabelasETE: INICIANDO -------------------------')
    #conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}", execution_options={"sqlite_raw_colnames": True})
    
    #con = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
    #con = sqlite3.connect(camDBSaida)
    #conETE.execute(sqlseq)

    executaSql(con, sqlseq)
    con.commit()
    print(time.ctime(), ' fim sqlseq')
    conFinal = sqlite3.connect(camDBSaida)
    print(time.ctime(), 'criando indices em link_ete')
    conFinal.execute('CREATE INDEX "ix_link_ete_id1" ON "link_ete" ("id1")')
    conFinal.execute('CREATE INDEX "ix_link_ete_id2" ON "link_ete" ("id2")')
    conFinal.commit()
    print(time.ctime(), 'juntaTabelasETE: Fim. ')
    #con.close()
    # print(time.ctime(), 'Aplicando VACUUM --------------------------------')
    # conEnderecoNormalizado.execute('VACUUM')
    # print(time.ctime(), 'Aplicando VACUUM-FIM-------------------------------')
    # bckengine = sqlite3.connect(camDBSaida, detect_types=sqlite3.PARSE_DECLTYPES, uri=True)
    # with bckengine: #isso faz commit
    #     conEnderecoNormalizado.backup(bckengine)
    # bckengine.close()
#.def juntaTabelasETE():
    

# def removeTabelasTemporarias(bpergunta=True): #usando sqlite3, não dá para usar esta função
#     con = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
#     insp = sqlalchemy.inspect(con)
#     nomes = insp.get_table_names()
#     tabelasAApagar = [n for n in nomes if n!='link_ete']
#     if bpergunta:
#         print(f'removeTabelasTemporarias. O script vai fazer alterações no banco de dados, APAGANDO as tabelas {tabelasAApagar}. Prossegue?(y,n)')
#         if input()!='y':
#             exit()
#     for t in tabelasAApagar:
#         print(time.ctime(), f'apagando tabela {t}')
#         con.execute(f'Drop table {t};')

#     print('removeTabelasTemporarias. FIM ', time.ctime())
#     print(time.ctime(), 'Aplicando VACUUM --------------------------------')
#     con.execute('VACUUM')
#     print(time.ctime(), 'Aplicando VACUUM-FIM-------------------------------')
#     con.commit()
#     con = None
    
# #.def removeTabelasTemporarias():    


# def compacta(cam):
#     if not cam:
#         raise Exception('especifique nome de arquivo para compactar')
#     print(time.ctime(), 'compactando... ')
#     #import py7zr
#     with py7zr.SevenZipFile(cam + '.7z', 'w') as archive:
#         archive.writeall(cam, os.path.split(cam)[1])
#     print(time.ctime(), 'compactando... Fim')
# #.def compacta

def retiraPontuacao(x):
    return(x.translate(str.maketrans('', '', string.punctuation)))

def leArquivoEnderecos():
    '''normaliza enderecos a partir de arquivo excel, com colunas id, endereco, municipio, uf '''
    df = pd.read_excel('endereco.xlsx', dtype=str, na_filter=False)
    df['id2'] = 'EN_' + df['endereco'].apply(normalizaEndereco) + '-' + df['municipio'].apply(lambda x:x.upper().replace('-',' ')) + '-' + df['uf'].apply(lambda x: x.upper())    
    df['id1'] = df['id']
    df['descricao']='end'
    df['valor']=0
    dend = df[['id1','id2','descricao','valor']]
    #df['valor']=0
    if False: #salva no arquivo sqlite 
        con = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
        dend.to_sql('link_ete', con, if_exists='append', dtype=sqlalchemy.types.String, index=False)
    else: #salva arquivo base_dados_modelo
        dend['comentario']='arquivo excel'
        con = sqlalchemy.create_engine("sqlite:///base_dados_modelo.db", execution_options={"sqlite_raw_colnames": True})
        dend.to_sql('links', con, if_exists='append', dtype=sqlalchemy.types.String, index=False)
    
if __name__ == "__main__":
    if not os.path.exists(camDbSqliteBaseCompleta):
        print(f'O arquivo {camDbSqliteBaseCompleta} com a base de cnpj não foi localizado. Corrija a variável camDbSqliteBaseCompleta')
        sys.exit(0)
    if os.path.exists(camDBSaida):
        print(f'O arquivo {camDBSaida} já existe. Apague primeiro e tente novamente.')
        sys.exit(0)
    print('O script vai fazer alterações no banco de dados. A execução leva cerca de 3.5 horas. Prossegue?(y,n)')
    if input()!='y':
        exit()
    #baixa_enderecos_cnpj_Dask(False)
    conBaseCompleta = sqlite3.connect(camDbSqliteBaseCompleta)
    conBaseCompleta.row_factory=sqlite3.Row

    if bMemoria:
        conEnderecoNormalizado = sqlite3.connect(':memory:')
    else:
        if os.path.exists(camDBSaida+'.tmp.db'):
            os.remove(camDBSaida+'.tmp.db')
        conEnderecoNormalizado = sqlite3.connect(camDBSaida+'.tmp.db')  
        
    baixa_enderecos_cnpj(False, conBaseCompleta, conEnderecoNormalizado)
    baixa_telefone_cnpj(False, conBaseCompleta, conEnderecoNormalizado)
    baixa_email_cnpj(False, conBaseCompleta, conEnderecoNormalizado)
    agrupa_cnpj_por_tipo('endereco', conEnderecoNormalizado)
    agrupa_cnpj_por_tipo('telefone', conEnderecoNormalizado)
    agrupa_cnpj_por_tipo('email', conEnderecoNormalizado)
    print(time.ctime(), 'salvando tabela')
    juntaTabelasETE(conEnderecoNormalizado, camDBSaida)

    conEnderecoNormalizado.close()
    if not bMemoria: #apaga arquivo temporario
        os.remove(camDBSaida+'.tmp.db')
    print(time.ctime(), 'salvando tabela-fim')
    ##compacta(camDBSaida) #isso está muito devagar
    print(f'O aquivo {camDBSaida} foi gerado')
    print(time.ctime(), 'FIM do script!!!!!!!!!!!!!!!!!')
    r = input('Pressione Enter')
    
    
