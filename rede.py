# -*- coding: utf-8 -*-
"""
Created on set/2020

@author: tomita

http://pythonclub.com.br/what-the-flask-pt-1-introducao-ao-desenvolvimento-web-com-python.html

"""

from flask import Flask, request, render_template, send_from_directory, send_file, jsonify
import json
import os
#import busca_google, 
import rede_relacionamentos
app = Flask("rede-flask")
gp = None
'''
import pysos #data persistence
nomes_bloqueados = pysos.List('pysos_nomes_bloqueados')
urls_bloqueados = pysos.List('pysos_urls_bloqueados')
'''

import configparser
config = configparser.ConfigParser()
config.read('rede.ini')
try:
    cpfcnpjInicial = config['rede']['cpfcnpjInicial']
    camadaInicial = config['rede']['camadaInicial']
except:
    #print('o arquivo sqlite não foi localizado. Veja o arquivo de configuracao rede.ini')
    cpfcnpjInicial=''
    camadaInicial=1


@app.route("/rede/")
@app.route("/rede/grafico/<cpfcnpj>/<int:camada>")
def html_pagina(cpfcnpj='', camada=camadaInicial):
    #return render_template('hello.html',nome=nome)
    #return render_template('webglDOMLabels_ms_os.html')
    print('htmlpagina',cpfcnpj, camada)
    if not cpfcnpj: #define cpfcnpj inicial, só para debugar.
        cpfcnpj=cpfcnpjInicial 
    #camada = 1
    #noLigacao = json.dumps(rede_relacionamentos.jsonRede(cpfcnpjIn=cpfcnpj, camada=camada))
    return render_template('rede_template.html', cpfcnpjInicial=cpfcnpj, camadaInicial=camada)

@app.route("/rede/dados_janela/<cpfcnpj>")
def html_dados(cpfcnpj=''):
    dados = rede_relacionamentos.jsonDados(cpfcnpj)
    templ = '''
        <!DOCTYPE html>
         <head>
         <title>%s</title>
        </head>
        <html>
        <body  >
        %s
        </body>
        </html> '''
    return templ %(cpfcnpj, dados)
    #onload="function(){window.sizeToContent();};"

@app.route('/rede/grafojson/<cpfcnpj>/<int:camada>')
def serve_rede_json(cpfcnpj, camada):
    print('pedido json:', cpfcnpj)
    if not camada:
        camada=1
    else:
        camada = int(camada)
    return jsonify(rede_relacionamentos.jsonRede(cpfcnpj, camada))
    #return jsonify({'no':[], 'ligacao':[]})
    #return send_from_directory(static_file_dir, arquivopath)

@app.route('/rede/dadosdetalhes/<cpfcnpj>')
def serve_dados_detalhes(cpfcnpj):
    print('pedido json:', cpfcnpj)
    return jsonify(rede_relacionamentos.jsonDados(cpfcnpj))
    #return jsonify({'no':[], 'ligacao':[]})
    #return send_from_directory(static_file_dir, arquivopath)

# @app.route('/favicon.ico/')
# def serve_favicon():
#     return send_from_directory(static_file_dir, 'img/favicon.png')
 
    
# /rede/macro_tcesppagamento/PJ_01305448000186/ligacao/?lista_nos=PJ_01305448000186

# @app.route('/rede/macro_tcesppagamento/<cpfcnpj>/ligacao/')    
# def serve_tcesp_json(cpfcnpj):
#     print('pedido endereco:', cpfcnpj)
#     if cpfcnpj.startswith('PJ_'):
#         cpfcnpjaux = cpfcnpj[3:]
#         return jsonify(rede_relacionamentos.jsonTCESP(cpfcnpjaux))
#     else:
#         return jsonify({})
    
# @app.route('/rede/macro_endnormalizado/<cpfcnpj>/ligacao/')    
# def serve_endereco_json(cpfcnpj):
#     print('pedido endereco:', cpfcnpj)
#     if cpfcnpj.startswith('PJ_'):
#         cpfcnpjaux = cpfcnpj # [3:]
#         return jsonify(rede_relacionamentos.jsonEndereco(cpfcnpjaux))
#     else:
#         return jsonify({})

#https://www.techcoil.com/blog/serve-static-files-python-3-flask/

static_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'static')
 
@app.route('/rede/static/<path:arquivopath>') #, methods=['GET'])
def serve_dir_directory_index(arquivopath):
    return send_from_directory(static_file_dir, arquivopath)

@app.route('/rede/dadosemarquivo/<formato>', methods = ['GET', 'POST'])
def serve_dadosEmArquivo(formato='xlsx'):
    print('serve_dadosEmArquivo')
    lista = json.loads(request.form['dadosJSON'])
    print(lista)
    return send_file(rede_relacionamentos.dadosParaExportar(lista), attachment_filename="rede_dados_cnpj.xlsx", as_attachment=True)
    #return send_from_directory(static_file_dir, 'pastaqualquer.xlsx', as_attachment=True)

@app.route('/rede/formdownload.html', methods = ['GET','POST'])
def serve_form_download(): #formato='pdf'):
    print('serve_form_download')
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

from requests.utils import unquote

def removeAcentos(data):
  import unicodedata, string
  if data is None:
    return ''
#  if isinstance(data,str):
 #   data = unicode(data,'latin-1','ignore')
  return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.printable)

'''
@app.route("/rede/consulta_google/")
def consulta_google():
    #responde /rede/consulta_google/?param=zz
    global gparam
#    if config.CONFIG['AMBIENTE'] != config.AMBIENTE_DESENVOLVIMENTO:
#        return redirect('/rede/', permanent=True) 
    import json, copy
    from requests.utils import quote, unquote
    request.encoding = 'latin1'    
    #request.encoding = 'utf8'
    param = request.args.get('param', '').replace(' ', '+')
    url =  request.args.get('url', '')
    nome = request.args.get('nome', '')
    cpfcnpj = request.args.get('id', '') #.replace(' ', '+')
    block = request.args.get('block', '') #.replace(' ', '+')
    arquivoParaAbrir = request.args.get('abrirArquivo','')
    #print '-'*20 + '/nParametros'
    #print(param, nome, cpfcnpj, url)    
    print(param, nome, cpfcnpj, url, arquivoParaAbrir)
    param, nome, cpfcnpj, url, arquivoParaAbrir = unquote(param), nome.upper(), cpfcnpj, unquote(url), unquote(arquivoParaAbrir)
    print(param, nome, cpfcnpj, url, arquivoParaAbrir)
    if arquivoParaAbrir: #abre arquivo local, só desenvolvimento
        if os.path.exists(arquivoParaAbrir):
            os.startfile(arquivoParaAbrir)
            #return HttpResponse(json.dumps({'retorno':True}), content_type="application/json")
            return jsonify({'retorno':True})
        else:
            return jsonify({'retorno':False})
    #print 'consulta ', param
    #import urllib2
    #if block:
    #    block = '&block=' + block
    if not url:
        #response = urllib2.urlopen('http://127.0.0.1:5000/busca_param?param=' + removeAcentos(param)+  block)
        #response = urllib2.urlopen('http://127.0.0.1:5000/busca_param?param=' + quote(param.encode('latin1'))+  block)
        dados = page_busca(nome, block)
    else:
        #response = urllib2.urlopen('http://127.0.0.1:5000/busca_url?url=' + quote(url, safe='') + block)
        #response = urllib2.urlopen('http://127.0.0.1:5000/busca_url?url=' + quote(url.encode('latin1'))) 
        dados = page_url(url)
    if block:
        #return HttpResponse(json.dumps({'retorno':'bloqueado'}), content_type="application/json")
        return jsonify({'retorno':'bloqueado'})
    #jsont = json.loads(response.read())
    jsont = dados
    #print jsont
    #{'no': nos, 'ligacao':ligacoes})
    #liga as notícias ao nó de referência
    ligacoes = jsont['ligacao']
    nos = []
    for n in jsont['no']:
        if n['tipo']=='DOC' :
            ligacao = {"origem":cpfcnpj, "destino":n['id'], "cor":"Yellow","camada":1, "tipoDescricao":{"0":'doc'}}
            ligacoes.append(copy.deepcopy(ligacao))
            nos.append(copy.deepcopy(n))
        elif n['tipo']=='IDE': #evita repetição de nó, já que esse já era o elemento de busca
            #print 'verificando nome ' + n['id'][1:]
            if removeAcentos(n['id'][1:].upper())!=nome:
                nos.append(copy.deepcopy(n))
            #else:
                #print 'no removido xxxxxxxxxxxxxxxxxxx',nome
    # TODO código para identificar cpf/cnpj
    #return HttpResponse(json.dumps({'no':nos, 'ligacao':ligacoes}), content_type="application/json")
    return jsonify({'no':nos, 'ligacao':ligacoes})

def page_busca(nome, block=None):
    #nome = request.args.get('param', default = '', type = str)
    #print(nome)
    #nome = unquote(nome)
    print('processando resposta para consulta:', nome)
    #block = request.args.get('block', default = '', type = str)
    dados = {} #json.dumps({})
    if block == '':
        dados = busca_google.jsonMacrosGrafoGoogle(nome)
    elif block == 'yes':
        #coloca na lista de bloqueados
        nomeaux=removeAcentos(nome.upper())
        if nomeaux not in set(nomes_bloqueados):
            nomes_bloqueados.append(nomeaux)
        #dados = json.dumps({'retorno':'bloqueado'})
        dados = {'retorno':'bloqueado'}
    elif block == 'no':
        #remove da lista de bloqueados
        pass
    #dados = json.dumps(u"flask json-" + nome)
    #response = app.response_class(response=u'texto de resposta '+ nome, mimetype='text/plain')
    #response = app.response_class(response=dados, mimetype='application/json', status=200)
    #return response
    return dados

#@app.route("/rede/busca_url/")
def page_url(url):
    url = request.args.get('url', default = '', type = str)
    print(url)
    dados = {} #json.dumps({})
    url = unquote(url)
    if url.startswith('"') and url.endswith('"'):
        url = url[1:-1]
    print('processando resposta para url:') #, url)
    block = request.args.get('block', default = '', type = str)
    if block=='':
        dados = busca_google.jsonMacrosUrl(url)
    elif block=='yes':
        urlaux = url[:8] + url[8:].split('/')[0]
        if urlaux not in set(nomes_bloqueados):
            urls_bloqueados.append(urlaux)
        #dados = json.dumps({'retorno':'bloqueado'})
        dados = {'retorno':'bloqueado'}
    else:
        pass

    #response = app.response_class(response=dados, mimetype='application/json', status=200)
    #return response
    return dados
'''
#@app.route("/busca_abrirArquivo/")
#def page_abreArquivo():
#    url = request.args.get('abrirArquivo', default = '', type = str)
#    print(url)
#    dados = json.dumps({})
#    url = unquote(url)
#    if url.startswith('"') and url.endswith('"'):
#        url = url[1:-1]
#    print('processando resposta para url:') #, url)
#    block = request.args.get('block', default = '', type = str)
#    if block=='':
#        dados = busca_google.jsonMacrosUrl(url)
#    elif block=='yes':
#        urlaux = url[:8] + url[8:].split('/')[0]
#        if urlaux not in set(nomes_bloqueados):
#            urls_bloqueados.append(urlaux)
#        dados = json.dumps({'retorno':'bloqueado'})
#    else:
#        pass
#
#    response = app.response_class(response=dados, mimetype='application/json', status=200)
#    return response

#@app.route('/arquivo_local/')
#def send_pdf():
#    from flask import send_from_directory, current_app as app
#    from requests.utils import unquote
#    url = request.args.get('url', default = '', type = str)
#    print(url)
#    url = unquote(url)
#    return send_from_directory(app.config['UPLOAD_FOLDER'], url)

'''
#@app.route('/rede/pj/<path:arquivopath>') 
#@app.route('/rede/pf/<path:arquivopath>')
@app.route('/rede/<path:arquivopath>')
def serve_dir_directory_index1(arquivopath):
    print('pedido:', arquivopath)
    dados = macros_login.macros_dados(arquivopath)
    if arquivopath.endswith('/json/'):
        return jsonify(dados.json())
    else:
        return dados.content
    #return jsonify({'no':[], 'ligacao':[]})
    #return send_from_directory(static_file_dir, arquivopath)
'''     
if __name__ == '__main__':
    import webbrowser
    webbrowser.open('http://127.0.0.1:5000/rede', new=0, autoraise=True)
    #app.run(debug=True, use_reloader=True)    
    app.run(debug=True, use_reloader=False)