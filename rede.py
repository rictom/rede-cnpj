# -*- coding: utf-8 -*-
"""
Created on set/2020
@author: github rictom/rede-cnpj
"""
#http://pythonclub.com.br/what-the-flask-pt-1-introducao-ao-desenvolvimento-web-com-python.html
from flask import Flask, request, render_template, send_from_directory, send_file, jsonify
import json
import os, sys
from werkzeug.utils import secure_filename
import secrets
import rede_relacionamentos
#from requests.utils import unquote
app = Flask("rede")
gp = {}
'''
import pysos #data persistence
nomes_bloqueados = pysos.List('pysos_nomes_bloqueados')
urls_bloqueados = pysos.List('pysos_urls_bloqueados')
'''

import configparser
config = configparser.ConfigParser()
config.read('rede.ini')

cpfcnpjInicial = config['rede'].get('cpfcnpjInicial', '')
camadaInicial = int(config['rede'].get('camadaInicial',1))

referenciaBD = config['rede'].get('referenciaBD','')
if referenciaBD:
    referenciaBD = 'Referência - ' + referenciaBD + '.'
    
gp['numeroDeEmpresasNaBase']=rede_relacionamentos.numeroDeEmpresasNaBase()
camadaMaxima = 15

@app.route("/rede/")
@app.route("/rede/grafico/<int:camada>/<cpfcnpj>")
@app.route("/rede/grafico_no_servidor/<path:idArquivoServidor>")
def html_pagina(cpfcnpj='', camada=camadaInicial, idArquivoServidor=''):
    mensagemInicial = ''
    inserirDefault=''
    camada = min(camadaMaxima, camada)
    #print(list(request.args.keys()))
    #print(request.args.get('mensagem_inicial'))
    idArquivoServidor = secure_filename(idArquivoServidor)
    if not cpfcnpj and not idArquivoServidor: #define cpfcnpj inicial, só para debugar.
        cpfcnpj = cpfcnpjInicial
        numeroEmpresas = gp['numeroDeEmpresasNaBase']
        tnumeroEmpresas = format(numeroEmpresas,',').replace(',','.')
        if numeroEmpresas>40000000: #no código do template, dois pontos será substituida por .\n
            mensagemInicial = f"LEIA ANTES DE PROSSEGUIR..Todos os dados exibidos são públicos, provenientes da página de dados públicos da Secretaria da Receita Federal..O autor não se responsibiliza pela utilização desses dados, pelo mau uso das informações ou incorreções..A base tem {tnumeroEmpresas} empresas. " + referenciaBD
        else:
            mensagemInicial = f"A base sqlite de TESTE tem {tnumeroEmpresas} empresas fictícias..Para inserir um novo elemento digite TESTE (CNPJ REAL NÃO SERÁ LOCALIZADO)"
            inserirDefault =' TESTE'
    return render_template('rede_template.html', cpfcnpjInicial=cpfcnpj, camadaInicial=camada, 
                           mensagemInicial=mensagemInicial, inserirDefault=inserirDefault, idArquivoServidor=idArquivoServidor)

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

json_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'arquivos_json')

@app.route('/rede/arquivos_json/<path:arquivopath>') #, methods=['GET'])
def serve_arquivos_json(arquivopath):
    filename = secure_filename(arquivopath)
    return send_from_directory(json_file_dir, filename + '.json')

@app.route('/rede/arquivos_json_upload/<nomeArquivo>', methods=['POST'])
def serve_arquivos_json_upload(nomeArquivo):
    filename = secure_filename(nomeArquivo)
    if len(request.get_json())>100000:
        return jsonify({'mensagem':{'lateral':'', 'popup':'O arquivo é muito grande e não foi salvo', 'confirmar':''}})
    nosLigacoes = request.get_json()
    filename += '.'+secrets.token_hex(10)
    cam = os.path.join(json_file_dir, filename + '.json')  
    with open(cam, 'w') as outfile:
        json.dump(nosLigacoes, outfile)
    return jsonify({'nomeArquivoServidor':filename})

@app.route('/rede/dadosemarquivo/<formato>', methods = ['GET', 'POST'])
def serve_dadosEmArquivo(formato='xlsx'):
    lista = json.loads(request.form['dadosJSON'])
    return send_file(rede_relacionamentos.dadosParaExportar(lista), attachment_filename="rede_dados_cnpj.xlsx", as_attachment=True)

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
