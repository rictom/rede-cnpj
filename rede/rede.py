# -*- coding: utf-8 -*-
"""
Created on set/2020
@author: github rictom/rede-cnpj
https://github.com/rictom/rede-cnpj

"""
#http://pythonclub.com.br/what-the-flask-pt-1-introducao-ao-desenvolvimento-web-com-python.html
from flask import Flask, request, render_template, send_from_directory, send_file, jsonify, Response, redirect
from requests.utils import unquote
#https://medium.com/analytics-vidhya/how-to-rate-limit-routes-in-flask-61c6c791961b
import flask_limiter #pip install Flask-Limiter
from flask_limiter.util import get_remote_address
from werkzeug.utils import secure_filename
import os, sys, json, secrets, io, glob
from functools import lru_cache
import rede_config as config
import pandas as pd

try:
    import rede_sqlite_cnpj as rede_relacionamentos
    print('utilizando rede_sqlite como rede_relacionamentos')
except:
    try:
        import rede_sql as rede_relacionamentos
        print('utilizando rede_sql como rede_relacionamentos')
    except:
        raise Exception('não há módulo de relacionamentos!!!')
    
try: #define alguma atividade quando é chamado por /rede/envia_json, função serve_envia_json_acao
    import rede_acao
except:
    pass

app = Flask("rede")
limiter = flask_limiter.Limiter(app, key_func=get_remote_address) #, default_limits=["200 per day", "50 per hour"])
limiter_padrao = config.config['ETC'].get('limiter_padrao', '20/minute').strip() 
limiter_dados = config.config['ETC'].get('limiter_dados', limiter_padrao).strip() 

#https://blog.cambridgespark.com/python-context-manager-3d53a4d6f017
gp = {}
gp['camadaMaxima'] = 10

#como é usada a tabela tmp_cnpjs no sqlite para todas as consultas, se houver requisições simultâneas ocorre colisão. 
#o lock faz esperar terminar as requisições por ordem.
#no linux, quando se usa nginx e uwsgi, usar lock do uwsgi, senão lock do threading (funciona no linux quando só tem 1 worker)
import contextlib
try:
    import uwsgi #supondo que quando tem uwsgi instalado, está usando linux e nginx
    gUwsgiLock=True
    #rlock = contextlib.nullcontext() #funciona no python3.7
    gLock =  contextlib.suppress() #python <3.7 #context manager que não faz nada
except:
    from threading import Lock
    gUwsgiLock=False
    gLock = Lock() #prevenir erros de requisições seguidas. No servidor faz o esperado colocando só um thread no rede.wsgi.ini

#sem usar lock, erro sqlite "database schema has changed"
'''para remover o lock, necessita de um esquema para gerenciar tabelas temporárias.
   foi colocado prefixos aleatórios às tabelas temporárias, mas em ambiente com thread provavelmente vai dar erro'''
if False:
    #gLock = contextlib.suppress() 
    gLock = contextlib.nullcontext()
    gUwsgiLock = False

# @app.route("/")
# def raiz():
#     return redirect("/rede/", code = 302)

@app.route("/rede/")
@app.route("/rede/grafico/<int:camada>/<cpfcnpj>")
@app.route("/rede/grafico_no_servidor/<idArquivoServidor>")
@limiter.limit(limiter_padrao)
def serve_html_pagina(cpfcnpj='', camada=0, idArquivoServidor=''):
    mensagemInicial = ''
    #inserirDefault = ''
    listaEntrada = ''
    listaJson = ''
    #camada = config.par.camadaInicial if config.par.camadaInicial else camada
    camada = camada if camada else config.par.camadaInicial
    camada = min(gp['camadaMaxima'], camada)
    #print(list(request.args.keys()))
    #print(request.args.get('mensagem_inicial'))
    # if par.idArquivoServidor:
    #     idArquivoServidor =  par.idArquivoServidor
    #idArquivoServidor = config.par.idArquivoServidor if config.par.idArquivoServidor else idArquivoServidor
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
                listaEntrada += '\t'.join([i.replace('\t',' ') for i in linha]) + '\n'       
            #print(listaEntrada)
            df = None            
    elif not cpfcnpj and not idArquivoServidor: #define cpfcnpj inicial, só para debugar.
        cpfcnpj = config.par.cpfcnpjInicial

        if  config.par.bExibeMensagemInicial:
            mensagemInicial = config.config['INICIO'].get('mensagem_advertencia','').replace('\\n','\n')
            mensagemInicial += rede_relacionamentos.mensagemInicial()
            #inserirDefault = 'TESTE'     
        # else:
        #     config.par.bMenuInserirInicial = False
    
    if config.par.tipo_lista:
        if config.par.tipo_lista.startswith('_>'):
            listaEntrada = config.par.tipo_lista + '\n' + listaEntrada 
        else:
            listaEntrada = config.par.tipo_lista + listaEntrada
            
    paramsInicial = {'cpfcnpj':cpfcnpj, 
                     'camada':camada,
                     'mensagem':mensagemInicial,
                     'bMenuInserirInicial': config.par.bMenuInserirInicial,
                     'inserirDefault':'TESTE',
                     'idArquivoServidor':idArquivoServidor,
                     'lista':listaEntrada,
                     'json':listaJson,
                     'listaImagens':listaImagens,
                     'bBaseReceita': 1 if config.config['BASE'].get('base_receita','') else 0,
                     'bBaseFullTextSearch': 1 if config.config['BASE'].get('base_receita_fulltext','') else 0,
                     'bBaseLocal': 1 if config.config['BASE'].get('base_local','') else 0,
                     'btextoEmbaixoIcone':config.par.btextoEmbaixoIcone,
                     'referenciaBD':config.referenciaBD,
                     'referenciaBDCurto':config.referenciaBD.split(',')[0]}
    #print(paramsInicial) 
    config.par.idArquivoServidor='' #apagar para a segunda chamada da url não dar o mesmo resultado.
    config.par.arquivoEntrada=''
    config.par.cpfcnpjInicial=''
    return render_template('rede_template.html', parametros=paramsInicial)
#.def serve_html_pagina

@app.route('/rede/grafojson/cnpj/<int:camada>/<cpfcnpj>',  methods=['GET', 'POST']) #methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_rede_json_cnpj(cpfcnpj, camada=1):
    # if request.remote_addr in ('xxx'):
    #     return jsonify({'acesso':'problema no acesso. favor não utilizar como api de forma intensiva, pois isso pode causar bloqueio para outros ips.'})        
    camada = min(gp['camadaMaxima'], int(camada))
    #cpfcnpj = cpfcnpj.upper().strip() #upper dá inconsistência com email, que está minusculo na base
    listaIds = []
    if request.method == 'GET':
        cpfcnpj = cpfcnpj.strip()
        listaIds = [cpfcnpj,]
    else:
        listaIds = request.get_json()
    r = None
    if gUwsgiLock:
        uwsgi.lock()
    try:
        with gLock:
            noLig = rede_relacionamentos.camadasRede(listaIds=listaIds, camada=camada, grupo='', bjson=True)
            # if listaIds:
            #     cpfcnpj=''
            # if not cpfcnpj:
            #     noLig = rede_relacionamentos.camadasRede(listaIds=listaIds, camada=camada, grupo='', bjson=True)
            # elif cpfcnpj.startswith('PJ_') or cpfcnpj.startswith('PF_'):
            #     noLig = rede_relacionamentos.camadasRede(listaIds=[cpfcnpj,], camada=camada, grupo='', bjson=True )
            # elif cpfcnpj.startswith('EN_') or cpfcnpj.startswith('EM_') or cpfcnpj.startswith('TE_'):
            #     noLig = rede_relacionamentos.camadaLink(listaIds=[cpfcnpj,], camada=camada, tipoLink='endereco')
            # elif cpfcnpj.startswith('ID_'): #ver se o upper é necessário 
            #     noLig = rede_relacionamentos.camadaLink(listaIds=[cpfcnpj.upper(),], camada=camada, tipoLink='base_local')
            # else:
            #     noLig = rede_relacionamentos.camadasRede(listaIds=[cpfcnpj,], camada=camada)
            r = jsonify(noLig)
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
    if gUwsgiLock:
        uwsgi.lock()
    try:
        with gLock:
            # if listaIds:
            #     cpfcnpj=''
            #r = jsonify(rede_relacionamentos.camadaLink(cpfcnpjIn=cpfcnpj, listaIds=listaIds, camada=camada, numeroItens=numeroItens, valorMinimo=valorMinimo, valorMaximo=valorMaximo, tipoLink='link'))
            r = jsonify(rede_relacionamentos.camadaLink(listaIds=listaIds, camada=camada, numeroItens=numeroItens, valorMinimo=valorMinimo, valorMaximo=valorMaximo, tipoLink='link'))
    finally:
        if gUwsgiLock:
            uwsgi.unlock()        
    return r
#.def serve_rede_json_links

@app.route('/rede/dadosjson/<cpfcnpj>', methods=['GET', 'POST']) # methods=['POST'])
@limiter.limit(limiter_dados)
def serve_dados_detalhes(cpfcnpj):
    if gUwsgiLock:
        uwsgi.lock()
    try: 
        with gLock:
            r = rede_relacionamentos.jsonDados([cpfcnpj,])
            return jsonify(r)
    finally:
        if gUwsgiLock:
            uwsgi.unlock()    

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

@app.route('/rede/arquivos_json/<arquivopath>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_arquivos_json(arquivopath):
    filename = secure_filename(arquivopath)
    extensao = os.path.splitext(filename)[1]
    if not extensao:
        filename += '.json'
        extensao = '.json'
    if extensao =='.json':
        if filename.startswith('temporario'): #se for temporário, apaga depois de copiar dados para stream
            return_data = io.BytesIO()
            caminho = os.path.join(local_file_dir,filename)
            if not os.path.exists(caminho):
                return Response("Arquivo não localizado", status=400)
            with open(caminho, 'rb') as fo:
                return_data.write(fo.read())
            return_data.seek(0)
            os.remove(caminho)
            return send_file(return_data, mimetype='application/json', attachment_filename=arquivopath)            
        else:
            return send_from_directory(local_file_dir, filename)
    else:
        return Response("Solicitação não autorizada", status=400)

@app.route('/rede/arquivos_json_upload/<nomeArquivo>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_arquivos_json_upload(nomeArquivo):
    nomeArquivo = unquote(nomeArquivo)
    filename = secure_filename(nomeArquivo)
    if len(request.get_json())>100000:
        return jsonify({'mensagem':{'lateral':'', 'popup':'O arquivo é muito grande e não foi salvo', 'confirmar':''}})
    nosLigacoes = request.get_json()
    if usuarioLocal():
        cam = nomeArquivoNovo(os.path.join(local_file_dir, filename + '.json'))
        filename = os.path.split(cam)[1]
    else:
        filename += '.'+secrets.token_hex(10) + '.json'
        cam = os.path.join(local_file_dir, filename)  
    with open(cam, 'w') as outfile:
        json.dump(nosLigacoes, outfile)
    return jsonify({'nomeArquivoServidor':filename})

@app.route('/rede/abre_json/', methods=['POST']) #usando post, não precisa criar arquivo temporário para abrir nova aba com itens selecionados
@limiter.limit(limiter_padrao)
def serve_post_json(): #xxx
    listaJson = request.form.get('data')
    try:
        listaJson = json.loads(listaJson)
    except:
        listaJson = {}
    paramsInicial = {'cpfcnpj':'', 
                     'camada':0,
                     'mensagem':'',
                     'bMenuInserirInicial': config.par.bMenuInserirInicial,
                     'inserirDefault':'',
                     'idArquivoServidor':'',
                     'lista':'',
                     'json':listaJson,
                     'listaImagens': imagensNaPastaF(True),
                     'bBaseReceita': 1 if config.config['BASE'].get('base_receita','') else 0,
                     'bBaseFullTextSearch': 1 if config.config['BASE'].get('base_receita_fulltext','') else 0,
                     'bBaseLocal': 1 if config.config['BASE'].get('base_local','') else 0,
                     'btextoEmbaixoIcone':config.par.btextoEmbaixoIcone,
                     'referenciaBD':config.referenciaBD,
                     'referenciaBDCurto':config.referenciaBD.split(',')[0]}
    return render_template('rede_template.html', parametros=paramsInicial)

@app.route('/rede/json_para_base/<comentario>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_arquivos_json_upload_para_base(comentario=''):
    comentario = unquote(comentario)
    if not usuarioLocal():
        return jsonify({'mensagem':{'lateral':'', 'popup':'Opção apenas disponível para usuário local', 'confirmar':''}})
    if not config.config['BASE'].get('base_local',''):
        return jsonify({'mensagem':{'lateral':'', 'popup':'Base sqlite local não foi configurada', 'confirmar':''}})
        
    nosLigacoes = request.get_json()
    rede_relacionamentos.carregaJSONemBaseLocal(nosLigacoes, comentario)
    return jsonify({'retorno':'ok'})

@app.route('/rede/envia_json/<acao>', methods=['POST'])
@limiter.limit(limiter_padrao)
def serve_envia_json_acao(acao=''):
    if not usuarioLocal():
        return jsonify({'mensagem':{'lateral':'', 'popup':'Opção apenas disponível para usuário local', 'confirmar':''}})
    nosLigacoes = request.get_json()
    #print(nosLigacoes)
    try:
        r = rede_acao.rede_acao(acao, nosLigacoes)
        return jsonify({'retorno':'ok', 'mensagem':{'popup':r}})
    except:
        return jsonify({'mensagem':{'lateral':'', 'popup':'Servidor não foi configurada para esta ação', 'confirmar':''}})

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

      
@app.route('/rede/dadosemarquivo/<formato>', methods = ['GET', 'POST'])
@limiter.limit(limiter_padrao)
def serve_dadosEmArquivo(formato='xlsx'):
    dados = json.loads(request.form['dadosJSON'])
    return send_file(rede_relacionamentos.dadosParaExportar(dados), attachment_filename="rede_dados_cnpj.xlsx", as_attachment=True)

@app.route('/rede/formdownload.html', methods = ['GET','POST'])
@limiter.limit(limiter_padrao)
def serve_form_download(): #formato='pdf'):
    return '''
        <html>
          <head></head>
          <body>
            <form id='formDownload' action="" method="POST">
              <textarea name="dadosJSON"></textarea>
            </form>
          </body>
        </html>
    '''
    
@app.route('/rede/abrir_arquivo/', methods = ['POST'])
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
    if (extensao in ['.xls','.xlsx','.txt','.docx','.doc','.pdf', '.ppt', '.pptx', '.csv','.html','.htm','.jpg','.jpeg','.png', '.svg']) and os.path.exists(nomeArquivo):
        os.startfile(nomeArquivo)
        #return HttpResponse(json.dumps({'retorno':True}), content_type="application/json")
        return jsonify({'retorno':True, 'mensagem':'Arquivo aberto,'})
    else:
        return jsonify({'retorno':False, 'mensagem':'Extensão de arquivo não autorizada,'})
#.def serve_abrirArquivoLocal

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
  import unicodedata, string
  if data is None:
    return ''
  return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.printable)

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
        
if __name__ == '__main__':
    import webbrowser
    webbrowser.open(f'http://127.0.0.1:{config.par.porta_flask}/rede', new=0, autoraise=True) 
    app.run(host='0.0.0.0',debug=True, use_reloader=False, port=config.par.porta_flask)
            #ssl_context=('certificado/rede_selfsigned.crf', 'certificado/rede_selfsigned.key'))