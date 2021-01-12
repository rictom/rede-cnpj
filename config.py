# -*- coding: utf-8 -*-
"""
Created on Sun Jan 10 09:17:19 2021
@author: github rictom/rede-cnpj
"""
import configparser, argparse, os, sys
config = configparser.ConfigParser()
config.read('rede.ini')

#cpfcnpjInicial = config['REDE'].get('cpfcnpj', '')
#camadaInicial = int(config['REDE'].get('camada',1))
#camadaInicial = config['REDE'].getint('camada',1)

def runParser():
    parser = argparse.ArgumentParser(description='descrição', epilog='rictom')
    parser.add_argument('-i', '--inicial', action='store', dest='cpfcnpjInicial', default=config['INICIO'].get('cpfcnpj', ''), type=str, help='1 ou mais cnpj separados por ponto e vírgula; Nome ou Razao Social Completa')
    parser.add_argument('-c', '--camada', action='store', dest='camadaInicial', type=int, default=config['INICIO'].getint('camada',1), help='camada')
    parser.add_argument('-k', '--conf_file', action='store', default='',help="defina arquivo de configuração", metavar="FILE")
    parser.add_argument('-j', '--json', action='store', dest='idArquivoServidor', default='', type=str, help='nome json no servidor')
    parser.add_argument('-a', '--lista', action='store', dest='arquivoEntrada', default='',help="inserir itens de arquivo em gráfico", metavar="FILE")
    parser.add_argument('-e', '--encoding', action='store', dest='encodingArquivo', default='utf8',help="codificação do arquivo", metavar="FILE")

    parser.add_argument('-m', '--n-mensagem', action='store_false', dest='bExibeMensagemInicial', default=config['INICIO'].getboolean('exibe_mensagem_advertencia',True),  help='não exibe mensagem inicial' )
    parser.add_argument('-M',  '--mensagem',action='store_true', dest='bExibeMensagemInicial', default=config['INICIO'].getboolean('exibe_mensagem_advertencia',True), help='exibe mensagem inicial')
    parser.add_argument('-y','--n-menuinserir', action='store_false', dest='bMenuInserirInicial', default=config['INICIO'].getboolean('exibe_menu_inserir',True), help='não exibe menu para inserir no inicio' )
    parser.add_argument('-Y','--menuinserir', action='store_true', dest='bMenuInserirInicial', default=config['INICIO'].getboolean('exibe_menu_inserir',True), help='exibe menu para inserir no inicio' )
    return parser.parse_args()

par = runParser()
if (par.conf_file): #se foi fornecido arquivo de configuracao pela linha de comando, recarrega configparser
    if (os.path.exists(par.conf_file)):
        config.read(par.conf_file)
        par = runParser()
    else:
        print('O arquivo de configuracao ' + par.conf_file + ' não existe. Parando...')
        sys.exit(1)

if par.arquivoEntrada:
    if not os.path.exists(par.arquivoEntrada):
        print('O arquivo ' + par.arquivoEntrada + ' não existe. Parando...')
        sys.exit(1)    
        
referenciaBD = config['BASE'].get('referencia_bd','')
if referenciaBD:
    referenciaBD = 'Referência - ' + referenciaBD + '.'
    
#dic = vars(pr) #converte para dicionario 
# cpfcnpjInicial = par.cpfcnpjInicial
# camadaInicial = par.camadaInicial
# bExibeMensagemInicial = par.bExibeMensagemInicial
# bMenuInserirInicial = par.bMenuInserirInicial
# idArquivoServidorInicial  = par.idArquivoServidor
#print('par)


'''
import pysos #data persistence
nomes_bloqueados = pysos.List('pysos_nomes_bloqueados')
urls_bloqueados = pysos.List('pysos_urls_bloqueados')
'''
