# -*- coding: utf-8 -*-
""" 
Created 2022/10/01
@author: rictom
https://github.com/rictom/rede-cnpj

Este script deve ser rodado para modificar a base sqlite cnpj.db para ser usado pelo projeto rede-cnpj.
A partir da versão 0.8.9 (outubro/2002), a rede-cnpj utiliza a tabela 'ligacao' auxiliar para permitir consultas mais rápidas para a geração dos gráficos (fica cerca de 3x mais rápido)
Esta rotina:
- cria tabela ligacao para uso na rede-cnpj
- cria indexação full text para buscar parte do nome de sócio, razão social e nome fantasia.
O arquivo cnpj.db deve estar na mesma pasta que este script. 
"""

#import sqlalchemy
import time, sys, sqlite3, os, psutil

#camDbSqliteBaseCompleta = r"cnpj.db" 
camDBcnpj = "dados-publicos/cnpj.db" 
camDBrede = 'dados-publicos/rede.db'
camDBrede_search = 'dados-publicos/rede_search.db'

bMemoria = False #se tiver menos de 16GB, use bMemoria=False

if bMemoria:
    if psutil.virtual_memory().total < 17005105152: #16GB
        print('ATENÇÃO. Como este script tenta gerar a tabela primeiro na memória RAM, há um requisito minimo de 16GB. Por isso este script possivelmente dará erro.')
        sys.exit()

hdd = psutil.disk_usage('/')
if hdd.free/(2**30) < 20:
    print(f'ATENÇÃO. Este script vai criar arquivos do tamanho de aproximadamente 20G, mas há somente {hdd.free/(2**30)} livres no HD. Libere espaço e tente novamente.')
    sys.exit()

if os.path.exists(camDBrede):
    print('o arquivo ' + camDBrede + ' já existe. Apague-o primeiro.')
    sys.exit(0)
if os.path.exists(camDBrede_search):
    print('o arquivo ' + camDBrede_search + ' já existe. Apague-o primeiro.')
    sys.exit(0)

    
resp = input(f'Este script vai criar ou alterar a base {camDBrede}. Leva cerca de 1 ou 2hs. Deseja prosseguir (y,n)?')
if resp.lower()!='y' and resp.lower()!='s':
    sys.exit()



sql_ligacao= '''
-- cria tabela de ligação (necessário a partir de versão 0.8.9 (outubro/2022)
drop table if exists ligacao
;
drop table if exists ligacao1
;
-- PJ->PJ vinculo sócio pessoa juridica
create table ligacao1 AS
select  'PJ_'||t.cnpj_cpf_socio as origem, 'PJ_'||t.cnpj as destino, sq.descricao as tipo, 'socios' as base
from cnpj.socios t
left join cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
where length(t.cnpj_cpf_socio)=14 --t.nome_socio=''
;
-- PF->PJ vinculo de sócio pessoa física
insert into ligacao1
select  'PF_'||t.cnpj_cpf_socio||'-'||t.nome_socio as origem, 'PJ_'||t.cnpj as destino, sq.descricao as tipo, 'socios' as base
from cnpj.socios t
left join cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
where length(t.cnpj_cpf_socio)=11 AND t.nome_socio<>''
;
-- PE->PJ empresa sócia no exterior 
insert into ligacao1
select 'PE_'||t.nome_socio as origem, 'PJ_'||t.cnpj as destino,  sq.descricao as tipo, 'socios' as base
from cnpj.socios t
left join cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_socio
where length(t.cnpj_cpf_socio)<>14 and length(t.cnpj_cpf_socio)<>11 and
t.cnpj_cpf_socio=''
;
-- PF>PE representante legal de empresa socia no exterior
insert into ligacao1
select  'PF_'||t.representante_legal||'-'||t.nome_representante as origem, 'PE_'||t.nome_socio as destino, 'rep-sócio-'||sq.descricao as tipo, 'socios' as base
from cnpj.socios t
left join cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_representante_legal
where length(t.cnpj_cpf_socio)<>14 and length(t.cnpj_cpf_socio)<>11 and
t.cnpj_cpf_socio='' and t.representante_legal<>'***000000**'
;
-- PF->PJ representante legal PJ->PJ
insert into ligacao1
select 'PF_'||t.representante_legal||'-'||t.nome_representante as origem, 'PJ_'||t.cnpj_cpf_socio as destino, 'rep-sócio-'||sq.descricao as tipo, 'socios' as base
from cnpj.socios t
left join cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_representante_legal
where length(t.cnpj_cpf_socio)=14 and t.representante_legal<>'***000000**' --t.nome_socio=''
;
-- PF->PF representante legal de sócio PF
insert into ligacao1
select  'PF_'||t.representante_legal||'-'||t.nome_representante as origem, 'PF_'||t.cnpj_cpf_socio||'-'||t.nome_socio as destino, 'rep-sócio-'||sq.descricao as tipo, 'socios' as base
from cnpj.socios t
left join cnpj.qualificacao_socio sq ON sq.codigo=t.qualificacao_representante_legal
where length(t.cnpj_cpf_socio)=11 and t.representante_legal<>'***000000**' --t.nome_socio=''
;
--tabela filiais
create table tfilial AS 
select cnpj, cnpj_basico
from estabelecimento t
where  t.matriz_filial = '2' -- is '1' 
;
CREATE INDEX idx_filiais ON tfilial (cnpj_basico)
;
-- PJ filial-> PJ matriz
insert into ligacao1
select 'PJ_'||tf.cnpj as origem, 'PJ_'||t.cnpj as destino, 'filial' as tipo, 'estabelecimento' as base
from tfilial tf
left join cnpj.estabelecimento t on t.cnpj_basico=tf.cnpj_basico 
where  t.matriz_filial = '1' -- is '1'
;
DROP TABLE IF EXISTS tfilial 
;
/*
-- PJ->PJ filial->matriz, versao anterior, lenta por causa do self join
insert into ligacao1
select 'PJ_'||tf.cnpj as origem, 'PJ_'||t.cnpj as destino, 'filial' as tipo, 'estabelecimento' as base
from cnpj.estabelecimento t
left join cnpj.estabelecimento tf on tf.cnpj_basico=t.cnpj_basico 
where  t.matriz_filial = '1' -- is '1' 
and tf.matriz_filial = '2' -- is '2' --tf.cnpj<>t.cnpj 
*/
;

-----------------------------------
--- cria tabela de ligacao
----------------------------------

CREATE TABLE ligacao AS
SELECT  origem as id1, destino as id2, tipo as descricao, base as comentario from ligacao1 group by origem, destino, tipo, base
--testar... parece que group by é mais rápido que distinct
--SELECT DISTINCT origem as id1, destino as id2, tipo as descricao, base as comentario  from ligacao1
;
 --para ficar no padrao das outras tabelas de ligacao
 
DROP TABLE IF EXISTS ligacao1
;
CREATE  INDEX idx_ligacao_origem ON ligacao (id1)
;
CREATE  INDEX idx_ligacao_destino ON ligacao (id2)
;
'''

#engine = sqlalchemy.create_engine(f'sqlite:///{camDbSqliteBaseCompleta}')

if bMemoria:
    engine = sqlite3.connect(':memory:')
else:
    engine = sqlite3.connect(camDBrede)
engine.execute("ATTACH DATABASE '" + camDBcnpj.replace('\\','/') + "' as cnpj")

def executaSequencia(camDB, sqlsequencia):    
    print(time.ctime(), f'Inicio - criando tabela {camDB}')
    ktotal = len(sqlsequencia.split(';'))
    for k,sql in enumerate(sqlsequencia.split(';'), 1):
        if not sql.replace('\n','').strip():
            continue
        print('-'*30)
        print(time.ctime(), '-executando parte:', f'{k}/{ktotal}')
        print(sql)
        engine.execute(sql)
    print(time.ctime(), 'commit')
    #engine.execute("DETACH  DATABASE cnpj") #apareceu mensagem database locked
    engine.commit()
    #engine.close()
    print(time.ctime(), ' fim sqlseq')
    
    print(time.ctime(), 'salvando tabela')
    if bMemoria:
        bckengine = sqlite3.connect(camDB, detect_types=sqlite3.PARSE_DECLTYPES, uri=True)
        with bckengine: #isso faz commit
            engine.backup(bckengine)
        bckengine.close()
    engine.close()
#.def executaSequencia

# cria tabela rede.db
executaSequencia(camDBrede, sqlsequencia=sql_ligacao)


sql_search= '''
----------------------------------------------
------indexa full text pela tabela de ligação (substitui versão anterior que fazia por colunas da tabela empresas, estabelecimentos e socios)
-----------------------------------------------

DROP TABLE if exists id_search;
CREATE virtual TABLE id_search using fts5 (id_descricao);

insert into id_search
--select distinct id_descricao
select id_descricao
from ( 
select 'PJ_' || te.cnpj ||'-' || t.razao_social  as id_descricao
from cnpj.estabelecimento te 
left join cnpj.empresas t on t.cnpj_basico=te.cnpj_basico
where te.matriz_filial is '1'
UNION ALL
select 'PJ_' || te.cnpj ||'-' || te.nome_fantasia  as id_descricao 
from cnpj.estabelecimento te 
-- where trim(te.nome_fantasia) <>'' --incluir este where faz que ignore cnpj filial sem nome fantasia, o que faz falta na hora de busca filiais por cnpj básico
UNION ALL
select  id1  as id_descricao
from rede.ligacao
where substr(id1,1,3)<>'PJ_'
UNION ALL
select  id2 as id_descricao
from  rede.ligacao
where substr(id2,1,3)<>'PJ_'
) as tunion
group by id_descricao --talvez group by seja mais rápido que distinct
;

'''

#incluir na mão id_search para a tabela links. Abrir rede.db no dbbrowser, anexar links.db e  rodar o sql abaixo
parte_tabela_links = '''
--inserir tabela links para busca em id_search
insert into id_search
select distinct id_descricao
from ( 
select  t.id1  as id_descricao
from links.links t
where substr(t.id1,1,3)<>'PJ_'
UNION
select  t.id2 as id_descricao
from links.links t
where substr(t.id2,1,3)<>'PJ_'
) as tunion
group by id_descricao --talvez group by seja mais rápido que distinct
;
'''

#engine = sqlalchemy.create_engine(f'sqlite:///{camDbSqliteBaseCompleta}')
#engine = sqlite3.connect(camDBrede)
if bMemoria:
    engine = sqlite3.connect(':memory:')
    
else:
    engine = sqlite3.connect(camDBrede_search)
engine.execute("ATTACH DATABASE '" + camDBrede + "' as rede")
engine.execute("ATTACH DATABASE '" + camDBcnpj.replace('\\','/') + "' as cnpj")

# cria tabela rede_search.db
executaSequencia(camDBrede_search, sqlsequencia=sql_search)


'''
#https://stackoverflow.com/questions/5831548/how-to-save-my-in-memory-database-to-hard-disk
conn = sqlite3.connect('file:existing_db.db?mode=memory',detect_types=sqlite3.PARSE_DECLTYPES,uri=True)
bckup = sqlite3.connect('file:backup.db',detect_types=sqlite3.PARSE_DECLTYPES,uri=True)
with bckup:
    conn.backup(bckup)
bckup.close()
conn.close()
'''

print(f'Os arquivos {camDBrede} e {camDBrede_search} foram gerados.')
print(time.ctime(), 'Fim!!!!!!!!!! ')
resp = input('Pressione Enter.')
               