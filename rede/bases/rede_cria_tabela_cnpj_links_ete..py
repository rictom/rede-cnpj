# -*- coding: utf-8-*- 

"""
Created on set/2020
@author: github rictom/rede-cnpj
"""

import pandas as pd, sqlalchemy, dask.dataframe as dd
import re, time, string, unicodedata, sys, os
gstep = 1000000 #quantidade de registros por loop

offsetGlobal='0' #offset do sql, string
rodaSo1bloco=False
caminhobase = '' #alterar caminho
caminhoarquivolog = 'logimportarenderecos.txt' 

camDbSqliteBaseCompleta = "cnpj.db" #aqui precisa ser a tabela completa

camDBSaida = 'cnpj_links_ete.db'

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

def normalizaEndereco(enderecoIn, ignoraEnderecoSoComNumeros = True, ignoraEnderecoSemNumeros = True):
    '''ajusta endereco, removendo ponto de números, removendo s/n, expandindo siglas comuns
       como R, AV. No final, ordena o resultado'''
    #junta números com ponto, por exemplo, 3.222 fica 3222
    enderecoAux = re.sub('([0-9]+)[.]([0-9]+)', '\\1\\2', enderecoIn).upper() #remove ponto de número
    enderecoAux = re.sub('([A-Z])(\d)', '\\1 \\2', enderecoAux) #separa letra de dígito
    enderecoAux = re.sub('(\d)([A-Z])', '\\1 \\2', enderecoAux) #separa digito de letra
    enderecoAux = ' ' + enderecoAux.upper() + ' '
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
    tremovenumerosespacos = str.maketrans(string.ascii_letters,string.ascii_letters,string.digits+' ')
    for k,pedaco in enumerate(lendereco):
        pedacoAjustado = pedaco
        if pedaco in dicAbreviaturas:
            if len(pedaco)>1 or k<=1: #alguns casos como R ou Q só se for no começo do nome
                pedacoAjustado = dicAbreviaturas[pedaco]
        if pedacoAjustado !='':
            if pedacoAjustado.isdigit():
                #pedacoAjustado = re.sub('0+([^ ]*)', '\\1', pedacoAjustado) #remove zero à esquerda (dá erro)
                pedacoAjustado = pedacoAjustado.lstrip('0') #remove zero à esquerda
                if pedacoAjustado != '':
                    numeros.append(pedacoAjustado)
            else:
                palavras.add(pedacoAjustado)
    palavrasOrdenadas = sorted(list(palavras)) #<---------------------Ordenação, pode ser removido
    if ignoraEnderecoSemNumeros: #há muitos endereços sem número
        if not numeros:
            return ''
    palavrasOrdenadas.extend(numeros)
    endereco = ' '.join(palavrasOrdenadas)
    if ignoraEnderecoSoComNumeros:
        if endereco.translate(tremovenumerosespacos) == '': #remove numeros e espaço
            return ''
        else:
            return endereco
    else:
        return endereco
#.def NormalizaEndereco

def baixa_enderecos_cnpj_Dask(bpergunta=True):
    #rodaSo1bloco = True
    print('INICIANDO baixa_enderecos_cnpj-------------------------', time.ctime())
    conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}") #, execution_options={"sqlite_raw_colnames": True})
    #conEnderecoNormalizado = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
    query = '''
                create table endereco_aux AS
                SELECT t.cnpj, cast(t.cnpj_basico as int) as cnpj_basico,
                situacao_cadastral as situacao,
                --tipo_logradouro, logradouro, numero, complemento, bairro,
                (logradouro || ' ' || numero || ' ' || complemento) as logradouroNumeroComplemento,
                ifnull(tm.descricao,'') as municipio, t.uf
                FROM estabelecimento t 
                left join municipio tm on tm.codigo=t.municipio
                limit 3000000
                
            ''' #pode haver empresas fora da base de teste            
    print(time.ctime(), 'criando tabela endereco_aux')    
    conBaseCompleta.execute('DROP TABLE IF EXISTS endereco_aux')    
    conBaseCompleta.execute(query)
    print(time.ctime(), 'criando tabela endereco_aux. Fim.') 
    #inicio = 0
    #kregistros = gstep
    #numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    conBaseCompleta = None

    print(time.ctime(), 'dask enderecos')
    pend = dd.read_sql_table('endereco_aux', f"sqlite:///{camDbSqliteBaseCompleta}", 
                             index_col='cnpj_basico')
    pend['endereco'] = pend['logradouroNumeroComplemento'].apply(normalizaEndereco, meta=('logradouroNumeroComplemento', 'object')) +  '-' + pend['municipio'] + '-'+pend['uf']
    dftmptable = pend[['cnpj','endereco','situacao']]
    dftmptable.to_sql('endereco', f"sqlite:///{camDBSaida}", if_exists='append', dtype=sqlalchemy.types.String)
    print('ROTINA TERMINOU ' + time.ctime())
#.def baixa_enderecos_cnpj_Dask():

def baixa_enderecos_cnpj(bpergunta=True): #usando pandas
    #rodaSo1bloco = True
    print('INICIANDO baixa_enderecos_cnpj-------------------------', time.ctime())
    conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}") #, execution_options={"sqlite_raw_colnames": True})
    
    conEnderecoNormalizado = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
    queryBase = '''
                SELECT t.cnpj, 
                situacao_cadastral as situacao,
                --tipo_logradouro, logradouro, numero, complemento, bairro,
                (logradouro || ' ' || numero || ' ' || complemento) as logradouroNumeroComplemento,
                ifnull(tm.descricao,'') as municipio, t.uf
                FROM estabelecimento t 
                left join municipio tm on tm.codigo=t.municipio
            ''' #pode haver empresas fora da base de teste            
    inicio = 0
    kregistros = gstep
    query = queryBase + f' LIMIT {inicio},{kregistros}'

    numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    
    while True:
        print(inicio, time.ctime())
        query = queryBase + f' LIMIT {inicio},{kregistros}' 

        pend = pd.read_sql(query, conBaseCompleta, index_col=None)
        pend['endereco_aux'] = pend['logradouroNumeroComplemento'].apply(normalizaEndereco) 
        pend['endereco'] = pend['endereco_aux'] +  '-' + pend['municipio'] + '-'+pend['uf']
        pend2 = pend[pend['endereco_aux'] != '' ]
        dftmptable = pend2[['cnpj','endereco','situacao']]
        dftmptable.to_sql('endereco', conEnderecoNormalizado, if_exists='append', dtype=sqlalchemy.types.String)

        inicio += kregistros
        if inicio>numeroDeRegistros:
            break
        if rodaSo1bloco:
            break      
    print('ROTINA TERMINOU ' + time.ctime())
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
    
def baixa_telefone_cnpj(bpergunta=True):
    rodaSo1bloco = False #True
    print('INICIANDO baixa_telefone_cnpj-------------------------', time.ctime())
    conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}", execution_options={"sqlite_raw_colnames": True})
    
    queryBase = '''
                SELECT cnpj, situacao_cadastral as situacao, ddd1, telefone1, ddd2, telefone2, ddd_fax, fax --, email
                FROM estabelecimento t
                -- where situacao='02' -- só ativas -- <>'08'ignora baixa
            ''' #pode haver empresas fora da base de teste
            
    inicio = 0
    kregistros = gstep   

    conTelefone = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})

    query = queryBase + f' LIMIT {inicio},{kregistros}'

    numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    conTelefone.execute('DROP table if exists telefone')
    while True:
        print(inicio, time.ctime())
        query = queryBase + f' LIMIT {inicio},{kregistros}' 
        lista = []
        #result = conBaseCompleta.execute(query ) #quando ocorre erro, o curso.execute retorna um texto
        for k in conBaseCompleta.execute(query):
           #cnpj = k['cnpj'] 
           situacao = k['situacao']
           t1 = ajustaTelefone(k['ddd1']+ ' ' + k['telefone1'])
           t2 = ajustaTelefone(k['ddd2']+ ' ' + k['telefone2'])
           t3 = ajustaTelefone(k['ddd_fax']+ ' ' + k['fax'])
           settel = set([t1, t2, t3])
           for tel in settel:
               if tel:
                   lista.append([k['cnpj'], tel, situacao])
        dftmptable = pd.DataFrame(lista, columns=['cnpj','telefone', 'situacao'])
        dftmptable.to_sql('telefone', conTelefone, if_exists='append', index=False, dtype=sqlalchemy.types.String)

        inicio += kregistros
        if inicio>numeroDeRegistros:
            break
        if rodaSo1bloco:
            break
       
    conBaseCompleta = None
    conTelefone = None
    print('ROTINA TERMINOU ' + time.ctime())
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

def baixa_email_cnpj(bpergunta=True):
    rodaSo1bloco = False
    print('INICIANDO baixa_email_cnpj-------------------------', time.ctime())
    conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}", execution_options={"sqlite_raw_colnames": True})
    conTelefone = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})

    queryBase = '''
                SELECT cnpj, situacao_cadastral as situacao, -- ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax, num_fax --,
                correio_eletronico as email
                FROM estabelecimento t
                -- where situacao='02' -- só ativas -- <>'08'ignora baixa
            ''' #pode haver empresas fora da base de teste
            
    inicio = 0
    kregistros = gstep
    query = queryBase + f' LIMIT {inicio},{kregistros}'

    numeroDeRegistros = conBaseCompleta.execute('select count(*) from estabelecimento').fetchall()[0][0]
    conTelefone.execute('DROP table if exists email')
    while True:
        print(inicio, time.ctime())
        query = queryBase + f' LIMIT {inicio},{kregistros}' 
        lista = []
        #result = conBaseCompleta.execute(query ) #quando ocorre erro, o curso.execute retorna um texto
        for k in conBaseCompleta.execute(query):
           cnpj = k['cnpj'] 
           situacao = k['situacao']
           email = ajusta_email(k['email'])
           if email:
               lista.append([cnpj, email, situacao])
        dftmptable = pd.DataFrame(lista, columns=['cnpj','email', 'situacao'])
        dftmptable.to_sql('email', conTelefone, if_exists='append', index=False, dtype=sqlalchemy.types.String)

        inicio += kregistros
        if inicio>numeroDeRegistros:
            break
        if rodaSo1bloco:
            break
       
    conBaseCompleta = None
    conTelefone = None
    print('ROTINA TERMINOU ' + time.ctime())
#.def def baixa_email_cnpj():
    
def agrupa_cnpj_por_tipo(tipo):
    camDB = camDBSaida #"ete.db"
    if tipo=='endereco':
        con = sqlalchemy.create_engine(f"sqlite:///{camDB}", execution_options={"sqlite_raw_colnames": True})
        nomeid = 'cnpj'
        coluna = 'endereco'
        filtro = 'ativas'
        filtroSQL = "situacao='02'"
        tabela = 'endereco'
        prefixo = 'EN_'
        prefixoNomeid = 'PJ_'
    elif tipo=='telefone':
        con = sqlalchemy.create_engine(f"sqlite:///{camDB}", execution_options={"sqlite_raw_colnames": True})
        nomeid = 'cnpj'
        coluna = 'telefone'
        filtro = 'ativas'
        filtroSQL = "situacao='02'"
        tabela = 'telefone'
        prefixo = 'TE_'
        prefixoNomeid = 'PJ_'
    elif tipo=='email':
        con = sqlalchemy.create_engine(f"sqlite:///{camDB}", execution_options={"sqlite_raw_colnames": True})
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
    
    sqlseq = f'''CREATE INDEX idx_{tabela}_{coluna} ON {tabela} (
     	{coluna});
        CREATE INDEX idx_{tabela}_{nomeid} ON {tabela} (
         	{nomeid} );

        create table {tabela}_contagem AS 
        select {coluna}, count({coluna}) as contagem
        from {tabela}
        group by {coluna}
        having count({coluna})>1;

        create table {tabela}_contagem_{filtro} AS 
        select {coluna}, count({coluna}) as contagem
        from {tabela}
        where {filtroSQL}
        group by {coluna}
        having count({coluna})>1;

        CREATE UNIQUE INDEX "idx_{tabela}_contagem" ON "{tabela}_contagem" ("{coluna}"); 

        CREATE UNIQUE INDEX "idx_{tabela}_contagem_{filtro}" ON "{tabela}_contagem_{filtro}" (
         	"{coluna}");
    
        CREATE table {tabela}_contagens AS
        select t.{nomeid}, t.{coluna}, tc.contagem, IFNULL(tcf.contagem,0) as contagem_{filtro}
        from {tabela} t
        inner join {tabela}_contagem tc on tc.{coluna}=t.{coluna}
        left join {tabela}_contagem_{filtro} tcf on tcf.{coluna}=t.{coluna};
    
        CREATE INDEX "idx_{tabela}_contagens" ON "{tabela}_contagens" (	"{coluna}");
        CREATE INDEX "idx_{tabela}_contagens_{nomeid}" ON "{tabela}_contagens" ("{nomeid}");
        
        create table link_{tabela} AS
        select "{prefixoNomeid}" || {nomeid} as id1, "{prefixo}"|| {coluna} as id2, '{coluna}' as descricao, contagem_{filtro} as valor
        from {tabela}_contagens;
        
        CREATE INDEX "ix_link_{tabela}_id1" ON "link_{tabela}" ("id1");
        CREATE INDEX "ix_link_{tabela}_id2" ON "link_{tabela}" ("id2");
    '''
    for k,sql in enumerate(sqlseq.split(';')):
        print('-'*30)
        print(time.ctime(), '-executando parte:', k)
        print(sql)
        con.execute(sql)
    print(time.ctime(), ' fim sqlseq')


def juntaTabelasETE():
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
    '''
    print('juntaTabelasETE: INICIANDO -------------------------', time.ctime())
    #conBaseCompleta = sqlalchemy.create_engine(f"sqlite:///{camDbSqliteBaseCompleta}", execution_options={"sqlite_raw_colnames": True})
    
    con = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
    #conETE.execute(sqlseq)
    for k,sql in enumerate(sqlseq.split(';')):
        print('-'*30)
        print(time.ctime(), '-executando parte:', k)
        print(sql)
        con.execute(sql)
    print(time.ctime(), ' fim sqlseq')
    print('juntaTabelasETE: Fim. ', time.ctime())
#.def juntaTabelasETE():

def removeTabelasTemporarias():
    con = sqlalchemy.create_engine(f"sqlite:///{camDBSaida}", execution_options={"sqlite_raw_colnames": True})
    insp = sqlalchemy.inspect(con)
    nomes = insp.get_table_names()
    tabelasAApagar = [n for n in nomes if n!='link_ete']
    #print(f'removeTabelasTemporarias. O script vai fazer alterações no banco de dados, APAGANDO as tabelas {tabelasAApagar}. Prossegue?(y,n)')
    #if input()!='y':
    #    exit()
    for t in tabelasAApagar:
        print(f'apagando tabela {t}', time.ctime())
        con.execute(f'Drop table {t};')
    con = None
    print('removeTabelasTemporarias. FIM ', time.ctime())
#.def removeTabelasTemporarias():    
    
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
        print(f'O arquivo {camDbSqliteBaseCompleta} com a base de cnpj não foi localizado.')
        resp = input('Pressione Enter.')
        sys.exit(0)
    if os.path.exists(camDBSaida):
        print(f'O arquivo {camDBSaida} já existe. Apague primeiro e tente novamente.')
        resp = input('Pressione Enter.')
        sys.exit(0)
    print('O script vai fazer alterações no banco de dados. Prossegue?(y,n)')
    if input()!='y':
        exit()
    #baixa_enderecos_cnpj_Dask(False)
    baixa_enderecos_cnpj(False)
    baixa_telefone_cnpj(False)
    baixa_email_cnpj(False)
    agrupa_cnpj_por_tipo('endereco')
    agrupa_cnpj_por_tipo('telefone')
    agrupa_cnpj_por_tipo('email')
    juntaTabelasETE()
    removeTabelasTemporarias()
    ##compacta(camDBSaida) #isso está muito devagar
    print(f'O arquivo {camDBSaida} foi gerado.')
    print(time.ctime(), 'FIM do script!!!!!!!!!!')
    resp = input('Pressione Enter.')
    
