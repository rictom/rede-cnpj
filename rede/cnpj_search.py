# -*- coding: utf-8 -*-
"""
Created on Sat Dec 18 09:55:05 2021

@author: ricar
cria cnpj_search.db para fazer busca por MATCH, por parte do nome da Razão Social ou nome do sócio
poderia indexar direto em cnpj.db, está separado para reduzir o tamanho do cnpj.db
"""

import sqlalchemy, time

camDbSqliteBaseCompleta = r"cnpj.db" #aqui precisa ser a tabela completa

# com bCriaSearchSeparado = False, faz indexação full text no proprio cnpj.db.
# alterar no arquivo de configuração rede.ini
# a linha base_receita_fulltext = cnpj.db
#
# com bCriaSearchSeparado = True, faz indexação full text em cnpj_search.db
# alterar no arquivo de configuração rede.ini
# a linha base_receita_fulltext = cnpj_search.db

bCriaSearchEmCNPJDB = True
if not bCriaSearchEmCNPJDB: #cria cnpj_search.db
    camDBSaida = 'cnpj_search.db'
    
    engine = sqlalchemy.create_engine(f'sqlite:///{camDBSaida}')
    
    #https://github.com/sqlalchemy/sqlalchemy/issues/4311
    
    @sqlalchemy.event.listens_for(engine, "connect")
    def connect(dbapi_conn, rec):
        dbapi_conn.execute(f'ATTACH DATABASE "{camDbSqliteBaseCompleta}" AS "cnpj"')
        
    
    sqlseq = '''
    CREATE virtual TABLE empresas_search using fts5 (razao_social);
    
    insert into empresas_search
    Select razao_social
    from cnpj.empresas;
    
    CREATE virtual TABLE socios_search using fts5 (nome_socio);
    
    insert into socios_search
    select nome_socio
    from cnpj.socios;'''
    
    print(time.ctime(), f'Inicio - criando {camDBSaida}')
    for k,sql in enumerate(sqlseq.split(';')):
        if not sql.replace('\n','').strip():
            continue
        print('-'*30)
        print(time.ctime(), '-executando parte:', k)
        print(sql)
        engine.execute(sql)
    print(time.ctime(), ' fim sqlseq')
    
    print(time.ctime(), f'Inicio - compactando {camDBSaida}')
#    import py7zr
#    with py7zr.SevenZipFile(camDBSaida + ".7z", 'w') as archive:
#        archive.writeall(camDBSaida)
    print('em rede.ini, coloque base_receita_fulltext = cnpj_search.db')
else: #cria indice full text em cnpj.db
    engine = sqlalchemy.create_engine(f'sqlite:///{camDbSqliteBaseCompleta}')
    sqlMatchEmcnpjdb = '''
    ---- criar tabelas de busca no cnpj.db 
    CREATE virtual TABLE empresas_search using fts5 (razao_social);
    
    
    insert into empresas_search
    Select razao_social
    from empresas;
    
    CREATE virtual TABLE socios_search using fts5 (nome_socio);
    
    insert into socios_search
    select nome_socio
    from socios;
    '''
    print(time.ctime(), f'Inicio - indexando full text {camDbSqliteBaseCompleta}')
    for k,sql in enumerate(sqlMatchEmcnpjdb.split(';')):
        if not sql.replace('\n','').strip():
            continue
        print('-'*30)
        print(time.ctime(), '-executando parte:', k)
        print(sql)
        engine.execute(sql)
    print(time.ctime(), ' fim sqlseq')

    print('em rede.ini, coloque base_receita_fulltext = cnpj.db')
#.if 

print(time.ctime(), 'Fim. ')
               