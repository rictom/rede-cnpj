# -*- coding: utf-8 -*-
"""
Created on set/2020
json a partir da tabela sqlite

@author: tomita
"""

#import pymysql as mysqllib #tem que definir autocommit=True
import time, copy, json, re, string, unicodedata

#if not easygui.ynbox('Isso vai carregar os arquivos da pasta filtrado para o servidor xampp. Deseja prosseguir?'):
#    exit()
import pandas as pd, sqlalchemy
#import sqlite3, 
#camDbSqlite = r"D:\cgu\receita-cnpj\dados_abertos_cnpj-2020-jul-csv\CNPJ_full.db"
import sys, configparser
config = configparser.ConfigParser()
config.read('rede.ini')
try:
    camDbSqlite = config['rede']['caminhoDBSqlite']
except:
    #print('o arquivo sqlite não foi localizado. Veja o arquivo de configuracao rede.ini')
    sys.exit('o arquivo sqlite não foi localizado. Veja o caminho da base no arquivo de configuracao rede.ini está correto.')

dfaux = pd.read_csv(r"tabelas\tabela-de-qualificacao-do-socio-representante.csv", sep=';')
dicQualificacao_socio = pd.Series(dfaux.descricao.values,index=dfaux.codigo).to_dict()
dfaux = pd.read_csv(r"tabelas\DominiosMotivoSituaoCadastral.csv", sep=';', encoding='latin1')
dicMotivoSituacao = pd.Series(dfaux['Descrição'].values, index=dfaux['Código']).to_dict()
dfaux = pd.read_excel(r"tabelas\cnae.xlsx", sheet_name='codigo-grupo-classe-descr')
dicCnae = pd.Series(dfaux['descricao'].values, index=dfaux['codigo']).to_dict()
dicSituacaoCadastral = {'01':'Nula', '02':'Ativa', '03':'Suspensa', '04':'Inapta', '08':'Baixada'}
dicPorteEmpresa = {'00':'Não informado', '01':'Micro empresa', '03':'Empresa de pequeno porte', '05':'Demais (Médio ou Grande porte)'}
dfaux = pd.read_csv(r"tabelas\natureza_juridica.csv", sep=';', encoding='utf8', dtype=str)
dicNaturezaJuridica = pd.Series(dfaux['natureza_juridica'].values, index=dfaux['codigo']).to_dict()


# dfaux = pd.read_excel(r"D:\cgu\receita-cnpj\tabelas auxiliares\tabela natureza.xlsx")
# dfaux.columns = [i.strip() for i in dfaux.columns]
# dicNatureza = pd.Series(dfaux['Natureza Jurídica'].values, index=dfaux['Código']).to_dict()

dfaux=None

def buscaPorNome(nome):
    #remove acentos
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})
    nome = ''.join(x for x in unicodedata.normalize('NFKD', nome) if x in string.printable)
    cjs, cps = set(), set()
    query = f'''
                SELECT cnpj_cpf_socio, nome_socio
                FROM socios 
                where nome_socio="{nome.upper()}"
            '''
    #data=pd.read_sql_query(query, con)
    #print(data)
    for r in con.execute(query):
        if len(r.cnpj_cpf_socio)==14:
            cjs.add(r.cnpj_cpf_socio)
        elif len(r.cnpj_cpf_socio)==11:
            cps.add((r.cnpj_cpf_socio, r.nome_socio))
    # pra fazer busca por razao_social, a coluna não está indexada
    query = f'''
                SELECT cnpj
                FROM empresas 
                where razao_social="{nome.upper()}"
            '''        
    for r in con.execute(query):
        cjs.add(r.cnpj)     
    return cjs, cps

def separaEntrada(cpfcnpjIn='', listaCpfCnpjs=None):
    cnpjs = set()
    cpfnomes = set()
    if cpfcnpjIn:
        lista = cpfcnpjIn.split(';')
        lista = [i.strip() for i in lista]
    else:
        lista = listaCpfCnpjs
    for i in lista:
        if i.startswith('PJ_'):
            cnpjs.add(i[3:])
        elif i.startswith('PF_'):
            cpfcnpjnome = cpfcnpjIn[3:]
            cpf = cpfcnpjnome[:11]
            nome = cpfcnpjnome[12:]
            cpfnomes.add((cpf,nome))  
        else:
            soDigitos = ''.join(re.findall('\d', str(i)))
            if len(soDigitos)==14:
                cnpjs.add(soDigitos)
            elif len(soDigitos)==11:
                pass #fazer verificação por CPF??
            elif not soDigitos:
                cnpjsaux, cpfnomesaux = buscaPorNome(i)
                cnpjs.update(cnpjsaux)
                cpfnomes.update(cpfnomesaux)  
    return cnpjs, cpfnomes

def ajustaLabelIcone(nosaux):
    nos = []
    for no in nosaux:
        prefixo =no['id'].split('_')[0]
        no['tipo'] = prefixo
        if prefixo=='PF':    
            no['label'] =  no['id'].replace('-','\n',1)
        elif prefixo=='PJ':
            no['label'] =  no['id'] + '\n' + no.get('descricao','')
        else:
            no['label'] = no['id']
        if prefixo=='PF':
            no['sexo'] = provavelSexo(no.get('id',''))
            if no['sexo']==1:
                imagem = 'icone-grafo-masculino.png'
            elif no['sexo']==2:
                imagem = 'icone-grafo-feminino.png'
            else:
                imagem = 'icone-grafo-desconhecido.png'
        elif prefixo=='END':
            imagem = 'icone-grafo-endereco.png'
        else:
            imagem = 'icone-grafo-empresa.png'
        no['imagem'] = '/rede/static/imagem/' + imagem
        nos.append(copy.deepcopy(no))
    return nos 
    
def jsonRede(cpfcnpjIn, camada=1):    
    #print('INICIANDO-------------------------')
    print('jsonRede-inicio: ' + time.ctime())
    
    #con=sqlite3.connect(camDbSqlite)
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})

    nosaux = []
    nosids = set()
    ligacoes = []
    cnpjs, cpfnomes = separaEntrada(cpfcnpjIn)
    camadasIds = {cnpj:0 for cnpj in cnpjs}
    for cpf,nome in cpfnomes:
        camadasIds[(cpf, nome)] = 0;
    for cam in range(camada):       
        dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
        dftmptable.to_sql('tmp_cnpjs', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
    
        dftmptable = pd.DataFrame(list(cpfnomes), columns=['cpf', 'nome'])
        dftmptable.to_sql('tmp_cpfnomes', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
        query = '''
                    SELECT t.cnpj, cnpj_cpf_socio, nome_socio, tipo_socio, cod_qualificacao
                    FROM socios t
                    INNER JOIN tmp_cnpjs tl
                    ON  tl.cnpj = t.cnpj
                    UNION
                    SELECT t.cnpj, cnpj_cpf_socio, nome_socio, tipo_socio, cod_qualificacao
                    FROM socios t
                    INNER JOIN tmp_cnpjs tl
                    ON tl.cnpj = t.cnpj_cpf_socio
                    UNION
                    SELECT t.cnpj, cnpj_cpf_socio, nome_socio, tipo_socio, cod_qualificacao
                    FROM socios t
                    INNER JOIN tmp_cpfnomes tn ON tn.nome= t.nome_socio AND tn.cpf=t.cnpj_cpf_socio
                    -- ORDER by cnpj, cnpj_cpf_socio, nome_socio 
                    --ordem desta forma esta lenta, 
                '''
        #no sqlite, o order by é feito após o UNION.
        #data=pd.read_sql_query(query, con)
        ligacoes = [] #tem que reiniciar a cada loop
        cnpjs=set()
        cpfnomes = set()
        orig_destAnt = ()
        for k in con.execute(query):
            if k['cnpj'] not in cnpjs:
                cnpjs.add(k['cnpj'])
            if (k['cnpj']) not in camadasIds:
                camadasIds[k['cnpj']] = cam+1
            if len(k['cnpj_cpf_socio'])==14:
                cnpj = k['cnpj_cpf_socio']
                destino = 'PJ_'+ cnpj
                if cnpj not in cnpjs:
                    cnpjs.add(cnpj)
                if cnpj not in camadasIds:
                    camadasIds[cnpj] = cam+1
            else:
                destino = 'PF_'+k['cnpj_cpf_socio']+'-'+k['nome_socio']
                cpfnome = (k['cnpj_cpf_socio'], k['nome_socio'])
                if cpfnome not in camadasIds:
                    camadasIds[cpfnome] = cam+1
                if destino not in nosids: #verificar repetição??
                    no = {'id': destino, 'descricao':k['nome_socio'], 
                          'camada': camadasIds[cpfnome], 
                          'situacao_ativa': True, 
                          #'empresa_situacao': 0, 'empresa_matriz': 1, 'empresa_cod_natureza': 0, 
                          'logradouro':'',
                          'municipio': '', 'uf': ''} #, 'm1': 0, 'm2': 0, 'm3': 0, 'm4': 0, 'm5': 0, 'm6': 0, 'm7': 0, 'm8': 0, 'm9': 0, 'm10': 0, 'm11': 0}     
                    nosids.add(destino)
                    nosaux.append(copy.deepcopy(no)) 
                    cpfnomes.add(cpfnome)
            #neste caso, não deve haver ligação repetida, mas é necessário colocar uma verificação se for ligações generalizadas
            if orig_destAnt == ('PJ_'+k['cnpj'], destino):
                print('XXXXXXXXXXXXXX repetiu ligacao', orig_destAnt)
            orig_destAnt = ('PJ_'+k['cnpj'], destino)
            ligacao = {"origem":'PJ_'+k['cnpj'], "destino":destino, 
                       "cor":"gray", "camada":cam+1, "tipoDescricao":'sócio',"label":dicQualificacao_socio.get(int(k['cod_qualificacao']),'')}
            ligacoes.append(copy.deepcopy(ligacao))
    
    #con1 = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable.to_sql('tmp_cnpjs', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
    query = '''
                SELECT t.cnpj, razao_social, situacao, 
                tipo_logradouro, logradouro, numero, complemento, bairro,
                municipio, uf
                FROM empresas t
                INNER JOIN tmp_cnpjs tp on tp.cnpj=t.cnpj
            '''
    #data=pd.read_sql_query(query, con)
    for k in con.execute(query):
        no = {'id': 'PJ_'+k['cnpj'], 'descricao': k['razao_social'], 
              'camada': camadasIds[k['cnpj']], 'tipo':0, 'situacao_ativa': k['situacao']=='02',
              #'empresa_situacao': dicMotivoSituacao.get(int( k['situacao']),''),
              #'empresa_matriz': k['matriz_filial'], 
              #'empresa_cod_natureza': k['cod_nat_juridica'], 
              'logradouro': f'''{k['tipo_logradouro']} {', '.join([k['logradouro'], k['numero'], k['complemento'], k['bairro']])}''',
              'municipio': k['municipio'], 'uf': k['uf'] 
              }
              #,'m1': 0, 'm2': 0, 'm3': 0, 'm4': 0, 'm5': 0, 'm6': 0, 'm7': 0, 'm8': 0, 'm9': 0, 'm10': 0, 'm11': 0
        nosaux.append(copy.deepcopy(no))
    
    con = None
    
    #ajusta nos, colocando label
    nosaux=ajustaLabelIcone(nosaux)
    nos = nosaux #nosaux[::-1] #inverte, assim os nos de camada menor serao inseridas depois, ficando na frente
    nos.sort(key=lambda n: n['camada'], reverse=True) #inverte ordem, porque os últimos icones vão aparecer na frente. Talvez na prática não seja útil.
    textoJson={'no': nos, 'ligacao':ligacoes} 

    print('jsonRede-fim: ' + time.ctime())
    return textoJson

def jsonDados(cpfcnpjIn):    
    #print('INICIANDO-------------------------')
    print('jsonDados-inicio: ' + time.ctime())
    
    #con=sqlite3.connect(camDbSqlite)
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})

    cnpjs, cpfnomes = separaEntrada(cpfcnpjIn)
    
    #con1 = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable.to_sql('tmp_cnpjs1', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
    query = '''
                SELECT *
                FROM empresas t
                INNER JOIN tmp_cnpjs1 tp on tp.cnpj=t.cnpj
            '''
    #data=pd.read_sql_query(query, con)

    
    dados = ""
    for k in con.execute(query):
        d = dict(k)  
        # for k in z:
        #     print(f"<b>{k}:</b> {{d['{k}']}} <br>")
        capital = d['capital_social']/100
        capital = f"{capital:,.2f}".replace(',','@').replace('.',',').replace('@','.')
        dados = f'''<b>CNPJ:</b> {d['cnpj']} - {'Matriz' if d['matriz_filial']=='1' else 'Filial'}<br>
<b>Razão Social:</b> {d['razao_social']} <br>
<b>Nome Fantasia:</b> {d['nome_fantasia']} <br>
<b>Data início atividades:</b> {ajustaData(d['data_inicio_ativ'])} <br>
<b>Situação:</b> {d['situacao']} - {dicSituacaoCadastral.get(d['situacao'],'')}  <b>Data Situação:</b> {ajustaData(d['data_situacao'])} <br>
<b>Motivo situação:</b> {d['motivo_situacao']}-{dicMotivoSituacao.get(int(d['motivo_situacao']),'')} <br>
<b>Natureza jurídica:</b> {d['cod_nat_juridica']}-{dicNaturezaJuridica.get(d['cod_nat_juridica'],'')}<br>
<b>CNAE:</b> {d['cnae_fiscal']}-{dicCnae.get(int(d['cnae_fiscal']),'')} <br>
<b>Porte empresa:</b> {d['porte']}-{dicPorteEmpresa.get(d['porte'],'')} <br>
<b>Opção MEI:</b> {d['opc_mei']} <br>
<b>Endereço:</b> {d['tipo_logradouro']} {', '.join([d['logradouro'], d['numero'], d['complemento'], d['bairro']])} <br>
<b>Municipio:</b> {d['municipio']}/{d['uf']} - <b>CEP:</b>{d['cep']} <br>
<b>Endereço Exterior:</b> {d['nm_cidade_exterior']} <b>País:</b> {d['nome_pais']} <br>
<b>Telefone:</b> {d['ddd_1']} {d['telefone_1']}  {d['ddd_2']} {d['telefone_2']} <br>
<b>Fax:</b> {d['ddd_fax']} {d['num_fax']} <br>
<b>Email:</b> {d['email']} <br>
<b>Capital Social:</b> R$ {capital} <br>
'''    
#dados.append(copy.deepcopy(no))
        break #só pega primeiro
    con = None

    print('jsonDados-fim: ' + time.ctime())
    return dados

def ajustaData(d): #aaaammdd
    return d[-2:]+'/' + d[4:6] + '/' + d[:4]

def dadosParaExportar(listaCpfCnpjs):    
    #print('INICIANDO-------------------------')
    print('jsonDados-inicio: ' + time.ctime())
    
    #con=sqlite3.connect(camDbSqlite)
    con = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})

    cnpjs, cpfnomes = separaEntrada(listaCpfCnpjs=listaCpfCnpjs)
    print(cnpjs)
    print(cpfnomes)
    #con1 = sqlalchemy.create_engine(f"sqlite:///{camDbSqlite}", execution_options={"sqlite_raw_colnames": True})
    dftmptable = pd.DataFrame(list(cpfnomes), columns=['cpf', 'nome'])
    dftmptable.to_sql('tmp_cpfnomes', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
    dftmptable = pd.DataFrame({'cnpj' : list(cnpjs)})
    dftmptable.to_sql('tmp_cnpjs', con=con, if_exists='replace', index=False, dtype=sqlalchemy.types.VARCHAR)
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
    dfin = pd.DataFrame(listaCpfCnpjs, columns=['cpfcnpj'])
    dfin.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "lista", index=False)
    dfe=pd.read_sql_query(queryempresas, con)
    dfe['capital_social'] = dfe['capital_social'].apply(lambda capital: f"{capital/100:,.2f}".replace(',','@').replace('.',',').replace('@','.'))
    
    dfe['matriz_filial'] = dfe['matriz_filial'].apply(lambda x:'Matriz' if x=='1' else 'Filial')
    dfe['data_inicio_ativ'] = dfe['data_inicio_ativ'].apply(ajustaData)
    dfe['situacao'] = dfe['situacao'].apply(lambda x: dicSituacaoCadastral.get(x,''))
                                            
    dfe['data_situacao'] =  dfe['data_situacao'].apply(ajustaData)
    dfe['motivo_situacao'] = dfe['motivo_situacao'].apply(lambda x: x + '-' + dicMotivoSituacao.get(int(x),''))
    dfe['cod_nat_juridica'] = dfe['cod_nat_juridica'].apply(lambda x: x + '-' + dicNaturezaJuridica.get(x,''))
    dfe['cnae_fiscal'] = dfe['cnae_fiscal'].apply(lambda x: x+'-'+dicCnae.get(int(x),''))
    
    dfe['porte'] = dfe['porte'].apply(lambda x: x+'-'+dicPorteEmpresa.get(x,''))
    
    dfe.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "Empresas", index=False)

    dfs=pd.read_sql_query(querysocios, con)
    dfs['cod_qualificacao'] =  dfs['cod_qualificacao'].apply(lambda x:x + '-' + dicQualificacao_socio.get(int(x),''))
    dfs.to_excel(writer, startrow = 0, merge_cells = False, sheet_name = "Socios", index=False)

    writer.close()
    output.seek(0)
    con = None
    return output

    #https://github.com/jmcarpenter2/swifter
    #dfe['data_inicio_ativ'] = dfe['data_inicio_ativ'].swifter.apply(lambda x: )


def provavelSexo(nome):
    carac = nome.split(' ')[0][-1].upper()
    if carac=='O':
        sexo = 1
    elif carac=='A':
        sexo = 2
    else:
        sexo = 0
    return sexo


