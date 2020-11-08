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
try:
    cpfcnpjInicial = config['rede']['cpfcnpjInicial']
    camadaInicial = int(config['rede']['camadaInicial'])
except:
    #print('o arquivo sqlite não foi localizado. Veja o arquivo de configuracao rede.ini')
    cpfcnpjInicial=''
    camadaInicial=1
try:
    referenciaBD = 'Referência - ' + config['rede']['referenciaBD'] + '.'
except:
    referenciaBD = ''
    
gp['numeroDeEmpresasNaBase']=rede_relacionamentos.numeroDeEmpresasNaBase()
camadaMaxima = 15

@app.route("/rede/")
@app.route("/rede/grafico/<cpfcnpj>/<int:camada>")
def html_pagina(cpfcnpj='', camada=camadaInicial):
    print('htmlpagina',cpfcnpj, camada)
    mensagemInicial = ''
    inserirDefault=''
    camada = min(camadaMaxima, camada)
    if not cpfcnpj: #define cpfcnpj inicial, só para debugar.
        cpfcnpj = cpfcnpjInicial
        numeroEmpresas = gp['numeroDeEmpresasNaBase']
        tnumeroEmpresas = format(numeroEmpresas,',').replace(',','.')
        if numeroEmpresas>40000000: #no código do template, dois pontos será substituida por .\n
            mensagemInicial = f"LEIA ANTES DE PROSSEGUIR..Todos os dados exibidos são públicos, provenientes da página de dados públicos da Secretaria da Receita Federal..O autor não se responsibiliza pela utilização desses dados, pelo mau uso das informações ou incorreções..A base tem {tnumeroEmpresas} empresas. " + referenciaBD
        else:
            mensagemInicial = f"A base sqlite de TESTE tem {tnumeroEmpresas} empresas fictícias..Para inserir um novo elemento digite TESTE (CNPJ REAL NÃO SERÁ LOCALIZADO)"
            inserirDefault =' TESTE'
    return render_template('rede_template.html', cpfcnpjInicial=cpfcnpj, camadaInicial=camada, mensagemInicial=mensagemInicial, inserirDefault=inserirDefault)

@app.route('/rede/grafojson/<cpfcnpj>/<int:camada>/cnpj')
def serve_rede_json_cnpj(cpfcnpj, camada=1):
    print('serve_rede_json-cnpj:', cpfcnpj)
    camada = min(camadaMaxima, int(camada))
    return jsonify(rede_relacionamentos.jsonRede(cpfcnpj, camada=camada))

@app.route('/rede/grafojson/<cpfcnpj>/<int:camada>/links/<int:numeroItens>/<int:valorMinimo>/<int:valorMaximo>')
def serve_rede_json_links(cpfcnpj, camada=1, numeroItens=15, valorMinimo=0, valorMaximo=0):
    print('serve_rede_json-link:', cpfcnpj)
    camada = min(camadaMaxima, int(camada))
    return jsonify(rede_relacionamentos.camadaLink(cpfcnpj, camada=camada, numeroItens=numeroItens, valorMinimo=valorMinimo, valorMaximo=valorMaximo))

@app.route('/rede/dadosjson/<cpfcnpj>')
def serve_dados_detalhes(cpfcnpj):
    print('serve_dados_detalhes:', cpfcnpj)
    return jsonify(rede_relacionamentos.jsonDados(cpfcnpj))

#https://www.techcoil.com/blog/serve-static-files-python-3-flask/

static_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'static')
 
@app.route('/rede/static/<path:arquivopath>') #, methods=['GET'])
def serve_dir_directory_index(arquivopath):
    return send_from_directory(static_file_dir, arquivopath)

@app.route('/rede/dadosemarquivo/<formato>', methods = ['GET', 'POST'])
def serve_dadosEmArquivo(formato='xlsx'):
    print('serve_dadosEmArquivo')
    lista = json.loads(request.form['dadosJSON'])
    #print(lista)
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

#from requests.utils import unquote

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
