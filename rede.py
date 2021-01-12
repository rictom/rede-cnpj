# -*- coding: utf-8 -*-
"""
Created on set/2020
@author: github rictom/rede-cnpj
"""
#http://pythonclub.com.br/what-the-flask-pt-1-introducao-ao-desenvolvimento-web-com-python.html
from flask import Flask, request, render_template, send_from_directory, send_file, jsonify, Response
from werkzeug.utils import secure_filename
import os, sys, json, secrets
import config, rede_relacionamentos
#from requests.utils import unquote
app = Flask("rede")
gp = {}

gp['numeroDeEmpresasNaBase'] = rede_relacionamentos.numeroDeEmpresasNaBase()
camadaMaxima = 15

@app.route("/rede/")
@app.route("/rede/grafico/<int:camada>/<cpfcnpj>")
@app.route("/rede/grafico_no_servidor/<idArquivoServidor>")
def html_pagina(cpfcnpj='', camada=0, idArquivoServidor=''):
    mensagemInicial = ''
    inserirDefault = ''
    listaEntrada = ''
    listaJson = ''
    #camada = config.par.camadaInicial if config.par.camadaInicial else camada
    camada = camada if camada else config.par.camadaInicial
    camada = min(camadaMaxima, camada)
    #print(list(request.args.keys()))
    #print(request.args.get('mensagem_inicial'))
    # if par.idArquivoServidor:
    #     idArquivoServidor =  par.idArquivoServidor
    #idArquivoServidor = config.par.idArquivoServidor if config.par.idArquivoServidor else idArquivoServidor
    idArquivoServidor = idArquivoServidor if idArquivoServidor else config.par.idArquivoServidor
    if idArquivoServidor:
        idArquivoServidor = secure_filename(idArquivoServidor)
    bBaseFullTextSearch = 1 if config.config['BASE'].get('base_receita_fulltext','') else 0
    listaImagens = rede_relacionamentos.imagensNaPastaF(True)
    if config.par.arquivoEntrada:
        #if os.path.exists(config.par.listaEntrada): checado em config
        extensao = os.path.splitext(config.par.arquivoEntrada)[1].lower()
        if extensao=='.csv' or extensao=='.txt':
            listaEntrada = open(config.par.arquivoEntrada, encoding=config.par.encodingArquivo).read()
        elif extensao=='.json':
            listaJson = json.loads(open(config.par.arquivoEntrada, encoding=config.par.encodingArquivo).read())
        else:
            print('arquivo em extensão não reconhecida, deve ser csv, txt ou json:' + config.par.arquivoEntrada)
            sys.exit(0)
    elif not cpfcnpj and not idArquivoServidor: #define cpfcnpj inicial, só para debugar.
        cpfcnpj = config.par.cpfcnpjInicial
        numeroEmpresas = gp['numeroDeEmpresasNaBase']
        tnumeroEmpresas = format(numeroEmpresas,',').replace(',','.')
        if  config.par.bExibeMensagemInicial:
            if numeroEmpresas>40000000: #no código do template, dois pontos será substituida por .\n
                mensagemInicial = f'''LEIA ANTES DE PROSSEGUIR.\n\nTodos os dados exibidos são públicos, provenientes da página de dados públicos da Secretaria da Receita Federal.\nO autor não se responsibiliza pela utilização desses dados, pelo mau uso das informações ou incorreções.\nA base tem {tnumeroEmpresas} empresas.\n''' + config.referenciaBD
            else:
                mensagemInicial = f"A base sqlite de TESTE tem {tnumeroEmpresas} empresas fictícias.\nPara inserir um novo elemento digite TESTE (CNPJ REAL NÃO SERÁ LOCALIZADO)"
                inserirDefault =' TESTE'        

            
    paramsInicial = {'cpfcnpj':cpfcnpj, 
                     'camada':camada,
                     'mensagem':mensagemInicial,
                     'bMenuInserirInicial': config.par.bMenuInserirInicial,
                     'inserirDefault':inserirDefault,
                     'idArquivoServidor':idArquivoServidor,
                     'lista':listaEntrada,
                     'json':listaJson,
                     'listaImagens':listaImagens,
                      'bBaseFullTextSearch': bBaseFullTextSearch }
    config.par.idArquivoServidor='' #apagar para a segunda chamada da url não dar o mesmo resultado.
    config.par.arquivoEntrada=''
    config.par.cpfcnpjInicial=''
    return render_template('rede_template.html', parametros=paramsInicial)
    # return render_template('rede_template.html', cpfcnpjInicial=cpfcnpj, camadaInicial=camada, 
    #                        mensagemInicial=mensagemInicial, inserirDefault=inserirDefault, idArquivoServidor=idArquivoServidor,
    #                        bBaseFullTextSearch = bBaseFullTextSearch, listaImagens=listaImagens)

@app.route('/rede/grafojson/cnpj/<int:camada>/<cpfcnpj>',  methods=['GET','POST'])
def serve_rede_json_cnpj(cpfcnpj, camada=1):
    camada = min(camadaMaxima, int(camada))
    listaIds = request.get_json()
    if listaIds:
        cpfcnpj=''
    if not cpfcnpj:
        return jsonify(rede_relacionamentos.camadasRede(cpfcnpjIn=cpfcnpj,  listaIds=listaIds, camada=camada, grupo='', bjson=True)) 
    elif cpfcnpj.startswith('PJ_') or cpfcnpj.startswith('PF_'):
        return jsonify(rede_relacionamentos.camadasRede(cpfcnpjIn=cpfcnpj, camada=camada, grupo='', bjson=True )) 
    elif cpfcnpj.startswith('EN_') or cpfcnpj.startswith('EM_') or cpfcnpj.startswith('TE_'):
        return jsonify(rede_relacionamentos.camadaLink(cpfcnpjIn=cpfcnpj,   listaIds=listaIds, camada=camada, tipoLink='endereco'))
    return  jsonify(rede_relacionamentos.camadasRede(cpfcnpj, camada=camada))

@app.route('/rede/grafojson/links/<int:camada>/<int:numeroItens>/<int:valorMinimo>/<int:valorMaximo>/<cpfcnpj>',  methods=['GET','POST'])
def serve_rede_json_links(cpfcnpj='', camada=1, numeroItens=15, valorMinimo=0, valorMaximo=0):
    camada = min(camadaMaxima, int(camada))
    listaIds = request.get_json()
    if listaIds:
        cpfcnpj=''
    return jsonify(rede_relacionamentos.camadaLink(cpfcnpjIn=cpfcnpj, listaIds=listaIds, camada=camada, numeroItens=numeroItens, valorMinimo=valorMinimo, valorMaximo=valorMaximo, tipoLink='link'))

@app.route('/rede/dadosjson/<cpfcnpj>')
def serve_dados_detalhes(cpfcnpj):
    return jsonify(rede_relacionamentos.jsonDados(cpfcnpj))

#https://www.techcoil.com/blog/serve-static-files-python-3-flask/

static_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'static')
@app.route('/rede/static/<path:arquivopath>') #, methods=['GET'])
def serve_dir_directory_index(arquivopath):
    return send_from_directory(static_file_dir, arquivopath)

local_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'arquivos')

@app.route('/rede/arquivos_json/<arquivopath>') #, methods=['GET'])
def serve_arquivos_json(arquivopath):
    filename = secure_filename(arquivopath)
    extensao = os.path.splitext(filename)[1]
    if not extensao:
        filename += '.json'
        return send_from_directory(local_file_dir, filename)
    elif extensao =='.json':
        return send_from_directory(local_file_dir, filename)
    else:
        return Response("Solicitação não autorizada", status=400)

@app.route('/rede/arquivos_json_upload/<nomeArquivo>', methods=['POST'])
def serve_arquivos_json_upload(nomeArquivo):
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

@app.route('/rede/arquivos_download/<path:arquivopath>') #, methods=['GET'])
def serve_arquivos_download(arquivopath):
    pedacos = os.path.split(arquivopath)  
    #print(f'{arquivopath=}')
    #print(f'{pedacos=}')
    if not pedacos[0]:
        return send_from_directory(local_file_dir, pedacos[1]) #, as_attachment=True)
    if not usuarioLocal():
        return Response("Solicitação não autorizada", status=400)
    else:
        return send_file(arquivopath) #, as_attachment=True)

      
@app.route('/rede/dadosemarquivo/<formato>', methods = ['GET', 'POST'])
def serve_dadosEmArquivo(formato='xlsx'):
    dados = json.loads(request.form['dadosJSON'])
    return send_file(rede_relacionamentos.dadosParaExportar(dados), attachment_filename="rede_dados_cnpj.xlsx", as_attachment=True)

@app.route('/rede/formdownload.html', methods = ['GET','POST'])
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
    
# @app.route('/rede/abrir_arquivo/', methods = ['POST'])
# def serve_abrirArquivoLocal(nomeArquivo=''):
#     #print('remote addr', request.remote_addr)
#     #print('host url', request.host_url)
#     #print(f'{nomeArquivo=}')
#     return #xxx
#     lista = request.get_json()
#     nomeArquivo = lista[0]
#     if not usuarioLocal():
#         print('operação negada.', f'{request.remote_addr=}')
#         return jsonify({'retorno':False})
#     #arquivoParaAbrir = nomeArquivo #secure_filename(nomeArquivo) 
#     if '/' not in nomeArquivo:
#         nomeArquivo = os.path.join(local_file_dir, nomeArquivo)  
#     extensao = os.path.splitext(nomeArquivo)[1].lower()
#     if (extensao in ['.xls','xlsx','.txt','.docx','.doc','.pdf', '.ppt', '.pptx', '.csv','.html','.htm','.jpg','.jpeg','.png']) and os.path.exists(nomeArquivo):
#         os.startfile(nomeArquivo)
#         #return HttpResponse(json.dumps({'retorno':True}), content_type="application/json")
#         return jsonify({'retorno':True})
#     else:
#         return jsonify({'retorno':False})

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

def removeAcentos(data):
  import unicodedata, string
  if data is None:
    return ''
  return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.printable)

if __name__ == '__main__':
    import webbrowser
    webbrowser.open('http://127.0.0.1:5000/rede', new=0, autoraise=True)
    #app.run(debug=True, use_reloader=True)    
    app.run(host='0.0.0.0',debug=True, use_reloader=False)
