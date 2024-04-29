# -*- coding: utf-8 -*-
"""
Created on set/2020
@author: github rictom/rede-cnpj
https://github.com/rictom/rede-cnpj

"""
#http://pythonclub.com.br/what-the-flask-pt-1-introducao-ao-desenvolvimento-web-com-python.html
from flask import Flask, request, render_template, send_from_directory, send_file, Response, abort
from requests.utils import unquote
#https://medium.com/analytics-vidhya/how-to-rate-limit-routes-in-flask-61c6c791961b
import flask_limiter #pip install Flask-Limiter
from flask_limiter.util import get_remote_address
from werkzeug.utils import secure_filename
import os, sys, json, secrets, io, glob, pathlib, unicodedata, string, importlib, time
from functools import lru_cache
import rede_config as config
import pandas as pd
from datetime import datetime

#from flask import jsonify
from orjson import dumps as jsonify #orjson é muito mais rápido que jsonify

nome_modulo_relacionamento = config.config['BASE'].get('modulo_relacionamento', 'rede_sqlite_cnpj').strip()
print(f'Carregando {nome_modulo_relacionamento}')
rede_relacionamentos = importlib.import_module(nome_modulo_relacionamento)
print(f'Utilizando {nome_modulo_relacionamento} como rede_relacionamentos.')

# sys.path.append('busca') #pasta com rotinas de busca
# sys.path.append('i2') #rotina do i2 chart reader
# import rede_google, mapa, rede_i2
#import rede_i2

# from busca import rede_google, mapa
# from i2 import rede_i2

from modulos.busca import rede_google, mapa
from modulos.i2 import rede_i2

try: #define alguma atividade quando é chamado por /rede/envia_json, função serve_envia_json_acao
    import rede_acao
except:
    pass

#rede_relacionamentos.gtabelaTempComPrefixo=False

app = Flask("rede")
#app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False #pretty print torna jsonify lento
#app.config['JSON_SORT_KEYS'] = False
app.config['MAX_CONTENT_PATH'] = 100000000
app.config['UPLOAD_FOLDER'] = 'arquivos'

kExtensaoDeArquivosPermitidos = ['.xls','.xlsx','.txt','.docx','.doc','.pdf', '.ppt', '.pptx', '.csv','.html','.htm','.jpg','.jpeg','.png', '.svg', '.anx', '.anb']

limiter = flask_limiter.Limiter(app=app, key_func=get_remote_address) #, default_limits=["200 per day", "50 per hour"])

limiter_padrao = config.config['ETC'].get('limiter_padrao', '20/minute').strip() 
limiter_dados = config.config['ETC'].get('limiter_dados', limiter_padrao).strip() 
limiter_google = config.config['ETC'].get('limiter_google', '4/minute').strip() 
limiter_arquivos = config.config['ETC'].get('limiter_arquivos', '2/minute').strip() 
bConsultaGoogle = config.config['ETC'].getboolean('busca_google',False)
bConsultaChaves = config.config['ETC'].getboolean('busca_chaves',False)
ggeocode_max  = config.config['ETC'].getint('geocode_max', 15) 
api_key_validas = [k.strip() for k in config.config['API'].get('api_keys', '').split(',')]
#https://blog.cambridgespark.com/python-context-manager-3d53a4d6f017
gp = {}
gp['camadaMaxima'] = 10
gp['itensFlag'] = ['situacao_fiscal', 'pep', 'ceis', 'cepim', 'cnep', 'acordo_leniência', 'ceaf', 'pgfn-fgts', 'pgfn-sida','pgfn-prev', 'servidor_siape']

#como é usada a tabela tmp_cnpjs no sqlite para todas as consultas, se houver requisições simultâneas ocorre colisão. 
#o lock faz esperar terminar as requisições por ordem.
#no linux, quando se usa nginx e uwsgi, usar lock do uwsgi, senão lock do threading (funciona no linux quando só tem 1 worker)
import contextlib
try:
    import uwsgi #supondo que quando tem uwsgi instalado, está usando linux e nginx
    gUwsgiLock=True
    #rlock = contextlib.nullcontext() #funciona no python3.7
    gLock =  contextlib.suppress() #python <3.7 #context manager que não faz nada
    print('usando gUwsgiLock=True e gLock = contexlib.suppress()')
except:
    import threading
    gUwsgiLock=False
    gLock = threading.Lock() #prevenir erros de requisições seguidas. No servidor faz o esperado colocando só um thread no rede.wsgi.ini


#sem usar lock, erro sqlite "database schema has changed"
'''para remover o lock, necessita de um esquema para gerenciar tabelas temporárias.
   foi colocado prefixos aleatórios às tabelas temporárias, mas em ambiente com thread provavelmente vai dar erro'''
if False: #bloqueia em rede_sqlite_cnpj.py
    #True= não usa bloqueio, rede_relacionamentos.gtabelaTempComPrefixo deve ser =True, para adicionar prefixo nas tabelas temporárias
    #False= usa bloqueio
    #gLock = contextlib.suppress() 
    #gLock = contextlib.nullcontext() #somente python>=3.7
    gLock =  contextlib.suppress()
    gUwsgiLock = False

# @app.route("/")
# def raiz():
#     return redirect("/rede/", code = 302)

@app.route("/rede/",  methods=['GET','POST'])
@app.route("/rede/grafico/<int:camada>/<cpfcnpj>")
@app.route("/rede/grafico_no_servidor/<idArquivoServidor>")
@limiter.limit(limiter_padrao)
def serve_html_pagina(cpfcnpj='', camada=0, idArquivoServidor=''):
    mensagemInicial = ''
    #inserirDefault = ''
    listaEntrada = ''
    listaJson = ''
    camada = min(gp['camadaMaxima'], camada)
    bbMenuInserirInicial = config.par.bMenuInserirInicial
    #janelaPai = ''
    
    idArquivoServidor = idArquivoServidor if idArquivoServidor else config.par.idArquivoServidor
    if idArquivoServidor:
        idArquivoServidor = secure_filename(idArquivoServidor)
    listaImagens = imagensNaPastaF(True)
    if config.par.arquivoEntrada:
        #if os.path.exists(config.par.listaEntrada): checado em config
        extensao = os.path.splitext(config.par.arquivoEntrada)[1].lower()
        if extensao in ['.py','.js']:
            listaEntrada = open(config.par.arquivoEntrada, encoding=config.par.encodingArquivo).read()
            if extensao=='.py': #configura para lista hierarquica
                listaEntrada = '_>p\n' + listaEntrada
            elif extensao=='.js':
                listaEntrada = '_>j\n' + listaEntrada
        elif extensao=='.json':
            listaJson = json.loads(open(config.par.arquivoEntrada, encoding=config.par.encodingArquivo).read())
        elif extensao in ['.csv','.txt']:
            df = pd.read_csv(config.par.arquivoEntrada, sep=config.par.separador, dtype=str, header=None, keep_default_na=False, encoding=config.par.encodingArquivo, skip_blank_lines=False)
        elif extensao in ['.xlsx','xls']:
            #df = pd.read_excel(config.par.arquivoEntrada, sheet_name=config.par.excel_sheet_name, header= config.par.excel_header, dtype=str, keep_default_na=False)
            df = pd.read_excel(config.par.arquivoEntrada, sheet_name=config.par.excel_sheet_name, header= None, dtype=str, keep_default_na=False)
        else:
            print('arquivo em extensão não reconhecida, deve ser csv, txt ou json:' + config.par.arquivoEntrada)
            sys.exit(0)
        if extensao in ['.csv', '.txt', '.xlsx', 'xls']:
            listaEntrada = ''
            for linha in df.values:
                listaEntrada += '\t'.join([i.replace('\t',' ') for i in linha]) + '\n' #isso deve ser um caso particular, tab dentro de valor troca para espaço.
            #print(listaEntrada)
            df = None            
    elif not cpfcnpj and not idArquivoServidor: #define cpfcnpj inicial, só para debugar.
        #cpfcnpj = config.par.cpfcnpjInicial
        mensagemInicial = config.config['INICIO'].get('mensagem_advertencia','').replace('\\n','\n').strip()
        if  mensagemInicial:
            mensagemInicial += '\n' + rede_relacionamentos.mensagemInicial()
            #inserirDefault = 'TESTE'     
        # else:
        #     config.par.bMenuInserirInicial = False
    
    if config.par.tipo_lista:
        if config.par.tipo_lista.startswith('_>'):
            listaEntrada = config.par.tipo_lista + '\n' + listaEntrada 
        else:
            listaEntrada = config.par.tipo_lista + listaEntrada

    if request.method == 'POST': #atende chamadas javascript da função openWindowWithPost 
        try:
            dadosPost = request.form.get('data')
            jsonaux = json.loads(dadosPost)
            #print(jsonaux)
            if isinstance(jsonaux, dict):
                #listaJson = jsonaux
                #listaJson = jsonaux.get('json', jsonaux) #temporário para manter compatibilidade reversa
                listaJson = jsonaux.get('json', '') #temporário para manter compatibilidade reversa
                listaEntrada = jsonaux.get('entradas', '')
                #janelaPai = jsonaux.get('janelaPai', '') #problemas com message, não funcionou
            elif isinstance(jsonaux, str):
                listaEntrada = jsonaux
            else:
                abort(404, 'Situação não prevista, request POST diferente de dict ou de texto.')
                return
        except:
            abort(404, 'Erro no processamente do POST')
            return
        mensagemInicial=''
        bbMenuInserirInicial = False
            
    paramsInicial = {'cpfcnpj':cpfcnpj, 
                     'camada':camada,
                     'mensagem':mensagemInicial,
                     'bMenuInserirInicial': bbMenuInserirInicial, #config.par.bMenuInserirInicial,
                     'inserirDefault':'', #'TESTE',
                     'idArquivoServidor':idArquivoServidor,
                     'lista':listaEntrada,
                     'json':listaJson,
                     'listaImagens':listaImagens,
                     'bBaseReceita': 1 if config.config['BASE'].get('base_receita','') else 0,
                     'bBaseFullTextSearch': 1 if config.config['BASE'].get('base_receita_fulltext','') else 0,
                     'bBaseLocal': 1 if config.config['BASE'].get('base_local','') else 0,
                     'btextoEmbaixoIcone':config.par.btextoEmbaixoIcone,
                     'referenciaBD':config.referenciaBD,
                     'referenciaBDCurto':config.referenciaBD.split(',')[0],
                     'geocode_max':ggeocode_max,
                     'bbusca_chaves': config.config['ETC'].getboolean('busca_chaves', False),
                     'mobile':any(word in request.headers.get('User-Agent','') for word in ['Mobile','Opera Mini','Android']),
                     'chrome':'Chrome' in request.headers.get('User-Agent',''),
                     'firefox':'Firefox' in request.headers.get('User-Agent',''),
                     'usuarioLocal': usuarioLocal(),
                     #'janelaPai': janelaPai,
                     'itensFlag':gp['itensFlag'], #['situacao_fiscal', 'pep', 'ceis', 'cepim', 'cnep', 'acordo_leniência', 'ceaf', 'pgfn-fgts', 'pgfn-sida','pgfn-prev'];
                     'bgrafico_no_servidor': ('/rede/grafico_no_servidor/' + idArquivoServidor) == request.path 
 }
    #print(paramsInicial)
    config.par.idArquivoServidor='' #apagar para a segunda chamada da url não dar o mesmo resultado.
    config.par.arquivoEntrada=''
    config.par.cpfcnpjInicial=''
    return render_template('rede_template.html', parametros=paramsInicial)
#.def serve_html_pagina

#@lru_cache #isto pode dar inconsistência com parametros via post??
#@app.route('/rede/grafojson/cnpj/<int:camada>/<cpfcnpj>', methods=['GET','POST'])
@app.route('/rede/grafojson/<tipo>/<int:camada>/<cpfcnpj>', methods=['POST',]) #methods=['GET','POST'])
@limiter.limit(limiter_padrao)
def serve_rede_json_cnpj(tipo, camada=1, cpfcnpj=''):
    # if request.remote_addr in ('xxx'):
    #     return jsonify({'acesso':'problema no acesso. favor não utilizar como api de forma intensiva, pois isso pode causar bloqueio para outros ips.'})        
    camada = min(gp['camadaMaxima'], int(camada))
        
    criterioCaminhos = ''
    if tipo=='cnpj':
        pass
    elif tipo.startswith('caminhos'):
        criterioCaminhos = tipo.removeprefix('caminhos-')
    else:
        abort(404, description='Recurso não encontrado')
    #cpfcnpj = cpfcnpj.upper().strip() #upper dá inconsistência com email, que está minusculo na base
    listaIds = []
    if request.method == 'POST':
        listaIds = request.get_json()
    elif request.method == 'GET': #este caso não está sendo usado, só pra debug
        cpfcnpj = cpfcnpj.strip()
        listaIds = [cpfcnpj,]

    r = None
    try:
        if gUwsgiLock:
            uwsgi.lock()
        with gLock:
            if not criterioCaminhos:
                noLig = rede_relacionamentos.camadasRede(camada=abs(camada), listaIds=listaIds, grupo = '',  bjson=True)
            elif criterioCaminhos:
                noLig = rede_relacionamentos.camadasRede(camada=abs(camada), grupo=listaIds, criterioCaminhos = criterioCaminhos, bjson=True)
            r = jsonify(noLig)
    except Exception as e:
        print("ERROR : "+str(e))
    finally:
        if gUwsgiLock:
            uwsgi.unlock()
    return r
#.def serve_rede_json_cnpj

@app.route('/rede/grafojson/links/<int:camada>/<int:numeroItens>/<int:valorMinimo>/<int:valorMaximo>/<cpfcnpj>',  methods=['GET','POST'])
@limiter.limit(limiter_padrao)
def serve_rede_json_links(cpfcnpj='', camada=1, numeroItens=15, valorMinimo=0, valorMaximo=0):
    camada = min(gp['camadaMaxima'], int(camada))
    if request.method == 'GET':
        cpfcnpj = cpfcnpj.strip()
        listaIds = [cpfcnpj,]
    else:
        listaIds = request.get_json()
    r = None
    try:
        if gUwsgiLock:
            uwsgi.lock()
        with gLock:
            r = jsonify(rede_relacionamentos.camadaLink(listaIds=listaIds, camada=camada, numeroItens=numeroItens, valorMinimo=valorMinimo, valorMaximo=valorMaximo, tipoLink='link'))
    finally:
        if gUwsgiLock:
            uwsgi.unlock()        
    return r
#.def serve_rede_json_links

#@lru_cache #isto pode dar inconsistência com parametros via post??
@app.route('/rede/dadosjson/<cpfcnpj>', methods=['GET', 'POST']) # methods=['POST']) 
@limiter.limit(limiter_dados)
def serve_dados_detalhes(cpfcnpj):
    if request.method == 'GET':
        idin = cpfcnpj
    else:
        idin = request.get_json()['idin']
    try: 
        if gUwsgiLock:
            uwsgi.lock()
        with gLock:
            r = rede_relacionamentos.jsonDados([idin,])
            if r:
                return jsonify(r[0])
            return jsonify({})
    finally:
        if gUwsgiLock:
            uwsgi.unlock()    
#.def serve_dados_detalhes

@app.route('/rede/consulta_cnpj/', methods=['GET', 'POST']) #precisa manter com / no final para manter compatibilidade com robots
@limiter.limit(limiter_dados)
def serve_dados_html():
    try: 
        if gUwsgiLock:
            uwsgi.lock()
        with gLock:
            return rede_relacionamentos.dados_consulta_cnpj(request, render_template, gp['itensFlag'])
    finally:
        if gUwsgiLock:
            uwsgi.unlock()    
#.def serve_dados_detalhes

if config.config['API'].getboolean('api_cnpj', False):
    @app.route('/rede/api/<tipo>/<cnpj>', methods=['GET', 'POST']) 
    @limiter.limit(limiter_dados)
    def serve_dados_api(tipo, cnpj):
        try: 
            if gUwsgiLock:
                uwsgi.lock()
            with gLock:
                return jsonify(rede_relacionamentos.dados_api_cnpj(cnpj, gp['itensFlag']))
        finally:
            if gUwsgiLock:
                uwsgi.unlock()    
    #.def serve_dados_detalhes
    
if config.config['API'].getboolean('api_caminhos', False):
    @app.route('/rede/api/caminhos', methods=['GET', 'POST']) 
    @limiter.limit(limiter_dados)
    def serve_api_caminhos():
        try: 

            if gUwsgiLock:
                uwsgi.lock()
            with gLock:
                try:
                    dados = request.get_json(force=True)
                    
                except:
                    return abort(400, description='Não encontrou json na requisição')
                
                if (dados.get('api_key','') not in api_key_validas) or not dados.get('api_key'):
                    return abort(401,  description='Chave inválida')
                #print(dados)
                #return jsonify(dados)
                camada = min(gp['camadaMaxima'], int(dados['camada']))
                return jsonify(rede_relacionamentos.camadasRede(camada=camada, grupo= dados['grupo'], listaIds=None, criterioCaminhos=dados['criterioCaminhos']))
        finally:
            if gUwsgiLock:
                uwsgi.unlock()    
    #.def serve_api_caminhos

#https://www.techcoil.com/blog/serve-static-files-python-3-flask/

# rotina antiga, para servir imagens - nginx não estava servindo imagens por erro de permissão na pasta
# em windows, o flask (por padrão) já serve esses arquivos em static e as imagens. 
# no linux, configurar o nginx para servir imagens. Configurar a pasta imagem com a permissão chmod 755 
# static_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'static')
# @app.route('/rede/static/<path:arquivopath>') #, methods=['GET'])
# def serve_dir_directory_index(arquivopath):
#     print('servindo static: ' + arquivopath)
#     return send_from_directory(static_file_dir, arquivopath)

#local_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'arquivos')
local_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), config.par.pasta_arquivos)

@app.route('/rede/arquivos_json/<arquivopath>', methods=['GET','POST','DELETE'])
@limiter.limit(limiter_padrao)
def serve_arquivos_json(arquivopath):
    #paramApagar = request.args.get('apagar', default = '', type = str)
    filename = secure_filename(arquivopath)
    #parametros = request.get_json()
    extensao = os.path.splitext(filename)[1]
    if not extensao:
        filename += '.json'
        extensao = '.json'
    #if extensao =='.json':
    if extensao in ('.json', '.csv', '.xlsx', '.pdf'):
        #if filename.startswith('temporario') or parametros.get('apagar','')=='sim': #se for temporário, apaga depois de copiar dados para stream
        if filename.startswith('_temporario') or request.method=='DELETE': #se for temporário, apaga depois de copiar dados para stream
            return_data = io.BytesIO()
            caminho = os.path.join(local_file_dir,filename)
            if not os.path.exists(caminho):
                return Response("Arquivo não localizado", status=400)
            with open(caminho, 'rb') as fo:
                return_data.write(fo.read())
            return_data.seek(0)
            if filename not in ['rede_cnpj_diagrama.json',] and extensao=='.json':
                os.remove(caminho)
            return send_file(return_data, mimetype='application/json', download_name=arquivopath)            
        else:
            return send_from_directory(local_file_dir, filename)
    else:
        return Response("Solicitação não autorizada", status=400)
#.def serve_arquivos_json

@app.route('/rede/arquivos_json_upload/<nomeArquivo>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_arquivos_json_upload(nomeArquivo):
    '''
        2024-03-09 permite salvar arquivo se estiver no padrao nome.secrets_tokenhex().json
    '''
    cam = ''
    tamanhoToken = 10
    nomeArquivo = unquote(nomeArquivo)
    filename = secure_filename(nomeArquivo)
    if len(request.get_json())>100000:
        #return jsonify({'mensagem':{'lateral':'', 'popup':'O arquivo é muito grande e não foi salvo', 'confirmar':''}})
        return jsonify({'mensagem':'O arquivo é muito grande e não foi salvo'})
    nosLigacoes = request.get_json()
    breescreve = (request.args.get('reescreve', default='', type=str)=='S')
    if breescreve:
        if not usuarioLocal():
            #verifica se nome já tem hex_digits
            pnome = filename.split('.')
            if len(pnome)>=3 and pnome[-1]=='json' and len(pnome[-2])==2*tamanhoToken and set(pnome[-2]).issubset(string.hexdigits):
                breescreve = True
            else:
                breescreve = False
                return jsonify({'mensagem': 'O arquivo ' + filename + ' não foi encontrado no servidor.'})
        if breescreve:     
                #nome tem token_hex, pode ser nome de arquivo válido
                camTeste = os.path.join(local_file_dir, filename)  
                if os.path.exists(camTeste):
                    cam = camTeste 
                else:
                    return jsonify({'mensagem': 'O arquivo ' + filename + ' não foi encontrado no servidor.'})
    if not cam:
        if usuarioLocal():
            cam = nomeArquivoNovo(os.path.join(local_file_dir, filename + '.json'))
            filename = os.path.split(cam)[1]
        else:
            filename += '.'+secrets.token_hex(tamanhoToken) + '.json'
            cam = os.path.join(local_file_dir, filename)  
    with open(cam, 'w') as outfile:
        json.dump(nosLigacoes, outfile)
    return jsonify({'nomeArquivoServidor':filename})
#.def serve_arquivos_json_upload

@app.route('/rede/arquivo_upload/', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_arquivos_upload():
    if not config.par.bArquivosDownload:
        return jsonify({'nomeArquivoServidor':'', 'mensagem':'salvamento de arquivo não autorizado'})
        #return Response("Solicitação não autorizada", status=400)
    f = request.files['arquivo']
    if pathlib.Path(f.filename).suffix not in kExtensaoDeArquivosPermitidos:
        return jsonify({'mensagem':'extensão de arquivo não permitido'})
    filename = secure_filename(f.filename)
    if not usuarioLocal():
        #filename += '.'+secrets.token_hex(10) + '.json'
        #cam = os.path.join(local_file_dir, filename)           
        return jsonify({'nomeArquivoServidor':'', 'mensagem':'salvamento de arquivo não autorizado'})
    else:
        cam = nomeArquivoNovo(os.path.join(local_file_dir, filename))
        filename = os.path.split(cam)[1]
    f.save(cam)
    return jsonify({'nomeArquivoServidor':filename})
#.def serve_arquivos_upload

@app.route('/rede/json_para_base/<comentario>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_arquivos_json_upload_para_base(comentario=''):
    comentario = unquote(comentario)
    if not usuarioLocal():
        return jsonify({'mensagem':'Opção apenas disponível para usuário local'})
    if not config.config['BASE'].get('base_local',''):
        return jsonify({'mensagem':'Base sqlite local não foi configurada'})
        
    nosLigacoes = request.get_json()
    rede_relacionamentos.carregaJSONemBaseLocal(nosLigacoes, comentario)
    return jsonify({'retorno':'ok'})
#.def serve_arquivos_json_upload_para_base

@app.route('/rede/envia_json/<acao>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_envia_json_acao(acao=''):
    if not usuarioLocal():
        return jsonify({'mensagem':'Opção apenas disponível para usuário local'})
    nosLigacoes = request.get_json()
    #print(nosLigacoes)
    try:
        r = rede_acao.rede_acao(acao, nosLigacoes)
        return jsonify({'retorno':'ok', 'mensagem':r})
    except:
        return jsonify({'mensagem':'Servidor não foi configurada para esta ação'})
#.def serve_envia_json_acao

# @app.route('/rede/arquivos_download/<path:arquivopath>') #, methods=['GET'])
# def serve_arquivos_download(arquivopath):
#     if not config.par.bArquivosDownload:
#         return Response("Solicitação não autorizada", status=400)
#     pedacos = os.path.split(arquivopath)  
#     #print(f'{arquivopath=}')
#     #print(f'{pedacos=}')
#     if not pedacos[0]:
#         return send_from_directory(local_file_dir, pedacos[1]) #, as_attachment=True)
#     if not usuarioLocal():
#         return Response("Solicitação não autorizada", status=400)
#     else:
#         return send_file(arquivopath) #, as_attachment=True)

      
@app.route('/rede/dadosemarquivo/<formato>', methods = ['POST'])
@limiter.limit(limiter_arquivos)
def serve_dadosEmArquivo(formato='xlsx'):
    #dados = json.loads(request.form['dadosJSON']) #formato anterior
    try: 
        if gUwsgiLock:
            uwsgi.lock()
        with gLock:
            dados = request.form.get('data')
            try:
                dados = json.loads(dados)
            except Exception as err:
                print('erro em dadosemarquivo:', err)
                return 
            if formato=='xlsx':
                return send_file(rede_relacionamentos.dadosParaExportar(dados), 
                                 download_name="rede_cnpj-"+time.strftime("%Y-%m-%d_%Hh%Mm")+".xlsx", as_attachment=True)
            elif formato=='anx':
                try:
                    return send_file(rede_i2.jsonParai2(dados), 
                                     download_name="rede_cnpj-"+time.strftime("%Y-%m-%d_%Hh%Mm")+".anx", as_attachment=True)
                except Exception as err:
                    print('erro na exportacao i2: ', err)
                #return send_file('folium.html', download_name="mapa.html", as_attachment=False)
    finally:
        if gUwsgiLock:
            uwsgi.unlock() 
#.def serve_dadosEmArquivo

@app.route('/rede/mapa', methods = ['POST'])
@limiter.limit(limiter_padrao)
def serve_mapa():
    dados = request.form.get('data')
    try:
        dados = json.loads(dados)['no']
    except Exception as err:
        print('erro em mapa:', err)
        return 
    #mostraTooltip = not(request.args.get('mobile', default = 0, type = int) and True)
    mobile = any(word in request.headers.get('User-Agent') for word in ['Mobile','Opera Mini','Android'])
    outputStream = mapa.geraMapa(dados, ggeocode_max, mostraTooltip=not mobile)
    return send_file(outputStream, download_name="mapa.html", as_attachment=False)
#.def serve_mapa

# #formdowload substituido por outro método, cria form direto na página
# @app.route('/rede/formdownload.html', methods = ['GET','POST'])
# @limiter.limit(limiter_padrao)
# def serve_form_download(): #formato='pdf'):
#     return '''
#         <html>
#           <head></head>
#           <body>
#             <form id='formDownload' action="" method="POST">
#               <textarea name="dadosJSON"></textarea>
#             </form>
#           </body>
#         </html>
#     '''
# #.def serve_form_download
    
#@app.route('/rede/abrir_arquivo/', methods = ['POST'])
@app.route('/rede/abrir_arquivo', methods = ['POST'])
@limiter.limit(limiter_padrao)
#def serve_abrirArquivoLocal(nomeArquivo=''):
def serve_abrirArquivoLocal():
    if not config.par.bArquivosDownload:
        return Response("Solicitação não autorizada", status=400)
   # print('remote addr', request.remote_addr)
    #print('host url', request.host_url)
    lista = request.get_json()
    #print(lista)
    nomeArquivo = lista[0]
    #print(f'{nomeArquivo=}')
    if not usuarioLocal():
        print(f'serve_abrirArquivoLocal: {nomeArquivo}')
        print('operação negada.', f'{request.remote_addr}')
        return jsonify({'retorno':False, 'mensagem':'Operação não autorizada,'})
    #arquivoParaAbrir = nomeArquivo #secure_filename(nomeArquivo) 
    #if '/' not in nomeArquivo: #windows usa \
    nomeSplit = os.path.split(nomeArquivo)
    if not nomeSplit[0]: #sem caminho inteiro
        nomeArquivo = os.path.join(local_file_dir, nomeArquivo)
    extensao = os.path.splitext(nomeArquivo)[1].lower()
    print(f'serve_abrirArquivoLocal: {nomeArquivo}')
    if not os.path.exists(nomeArquivo):
        if nomeSplit[0]:
            return jsonify({'retorno':False, 'mensagem':'Arquivo não localizado,'})
        else:
            return jsonify({'retorno':False, 'mensagem':'Não foi localizado na pasta arquivos do projeto.'})
    if (extensao in kExtensaoDeArquivosPermitidos) and os.path.exists(nomeArquivo):
        os.startfile(nomeArquivo)
        #return HttpResponse(json.dumps({'retorno':True}), content_type="application/json")
        return jsonify({'retorno':True, 'mensagem':'Arquivo aberto,'})
    else:
        return jsonify({'retorno':False, 'mensagem':'Extensão de arquivo não autorizada,'})
#.def serve_abrirArquivoLocal

if bConsultaChaves: 
    @app.route('/rede/busca_google', methods=['GET', 'POST'])
    @limiter.limit(limiter_google)
    async def serve_busca_google_chave():
        pagina = request.args.get('pag', default = 1, type = int)
        texto_busca = request.args.get('q', default = '', type = str)
        n_palavras_chave = request.args.get('palavras_chave', default = 0, type = int)    
        link = unquote(request.args.get('link', default = '', type = str))

        if not texto_busca and not link:
            return jsonify({'no':[], 'ligacao':[], 'mensagem':'Sem texto'})
        try:
        #if True:
            if texto_busca:
                #r = await rede_google.json_busca(texto_busca, pagina, n_palavras_chave)
                r = await rede_google.json_google_chaves(texto_busca, pagina, n_palavras_chave)
            elif link:
                if link.startswith('LI_'):
                    link = link[3:]
                    r = await rede_google.json_busca_palavras_urls([link], n_palavras_chave)
                elif link.startswith('AR_'):
                    caminhoDoc = caminhoArquivoLocal(link[3:])
                    r = rede_google.json_busca_palavras_doc(link, caminhoDoc,  n_palavras_chave)
                    #await asyncio.sleep(0.01)
            return r #jsonify({'retorno':'ok', 'mensagem':{'popup':r}})
        except Exception as err:
            print('erro em serve_busca_google', err)
            return jsonify({'no':[], 'ligacao':[], 'mensagem':'Servidor não foi configurado para esta ação-A'})
    #.def serve_busca_google assíncrono com chaves
elif bConsultaGoogle: #se for só consulta google, sem chaves, faz sem asyncio
    @app.route('/rede/busca_google', methods=['GET', 'POST'])
    @limiter.limit(limiter_google)
    def serve_busca_google():
        # if not bConsultaGoogle:# and not usuarioLocal():
        #     return jsonify({'no':[], 'ligacao':[], 'mensagem':{'lateral':'', 'popup':'Servidor não foi configurado para busca-B', 'confirmar':''}})
        pagina = request.args.get('pag', default = 1, type = int)
        texto_busca = request.args.get('q', default = '', type = str)
        n_palavras_chave = request.args.get('palavras_chave', default = 0, type = int)    
        if n_palavras_chave:
            return jsonify({'no':[], 'ligacao':[], 'mensagem':'Servidor não foi configurado para extrair as chaves. Baixe o projeto no github e rode a Rede localmente com essa opção ativada.'})
        if not texto_busca:
            return jsonify({'no':[], 'ligacao':[], 'mensagem':'Sem texto'})
        try:
            r = rede_google.json_google_chaves_sincrono(texto_busca, pagina, n_palavras_chave=0)
            return r 
        except:
            return jsonify({'no':[], 'ligacao':[], 'mensagem':'Servidor não foi configurado para esta ação-C'})
    #.def serve_busca_google síncrono sem chaves

def caminhoArquivoLocal(nomeArquivo):
    nomeSplit = os.path.split(nomeArquivo)
    if not nomeSplit[0]: #sem caminho inteiro
        nomeArquivo = os.path.join(local_file_dir, nomeArquivo)
    extensao = os.path.splitext(nomeArquivo)[1].lower()
    if not os.path.exists(nomeArquivo):
        return ''
    if (extensao in kExtensaoDeArquivosPermitidos) and os.path.exists(nomeArquivo):
        return nomeArquivo
#.def caminhoArquivoLocal

def usuarioLocal():
    return request.remote_addr ==  '127.0.0.1'

def nomeArquivoNovo(nome):
    k=1
    pedacos = os.path.splitext(nome)
    novonome = nome
    while True:
        if not os.path.exists(novonome):
            return novonome
        novonome = pedacos[0] + f"{k:04d}" +  pedacos[1]
        k += 1
        if k>100:
            print('algo errado em nomeArquivoNovo')
            break
    return nome
#.def nomeArquivoNovo

def removeAcentos(data):
  #import unicodedata, string
  if data is None:
    return ''
  return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.printable)
#.def removeAcentos

@lru_cache(8)
def imagensNaPastaF(bRetornaLista=True):
    dic = {}
    for item in glob.glob('static/imagem/**/*.png', recursive=True):
        if '/nao_usado/' not in item.replace("\\","/"):
            dic[os.path.split(item)[1]] = item.replace("\\","/")
    if bRetornaLista:
        return sorted(list(dic.keys()))
    else:
        return dic
#.def imagensNaPastaF
   
if __name__ == '__main__':
    import webbrowser, platform
    if platform.system()=='Windows': 
        webbrowser.open(f'http://127.0.0.1:{config.par.porta_flask}/rede', new=0, autoraise=True) 
    app.run(host='0.0.0.0',debug=True, use_reloader=False, port=config.par.porta_flask)
            #ssl_context=('certificado/rede_selfsigned.crf', 'certificado/rede_selfsigned.key'))
    try: #fecha splash na versão .exe
        import pyi_splash
        pyi_splash.update_text('UI Loaded ...')
        pyi_splash.close()
    except:
        pass