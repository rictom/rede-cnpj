# -*- coding: utf-8 -*-
"""

lista relação de arquivos na página de dados públicos da receita federal
e faz o download
https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj
https://dadosabertos.rfb.gov.br/CNPJ/
http://200.152.38.155/CNPJ/
"""
from bs4 import BeautifulSoup
import requests, wget, os, sys, time, glob, parfive

#url = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/2024-08/' #padrão a partir de agosto/2024
#url_dados_abertos = 'https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/'
url_dados_abertos = 'http://200.152.38.155/CNPJ/dados_abertos_cnpj/'

pasta_zip = r"dados-publicos-zip" #local dos arquivos zipados da Receita
pasta_cnpj = 'dados-publicos'


def requisitos():
    #se pastas não existirem, cria automaticamente
    if not os.path.isdir(pasta_cnpj):
        os.mkdir(pasta_cnpj)
    if not os.path.isdir(pasta_zip):
        os.mkdir(pasta_zip)
        
    arquivos_existentes = list(glob.glob(pasta_cnpj +'/*.*')) + list(glob.glob(pasta_zip + '/*.*'))
    if len(arquivos_existentes):
        #eg.msgbox("Este programa baixa arquivos csv.zip de dados abertos da Receita Federal e converte para uso na RedeCNPJ aplicativo.\nIMPORTANTE: Para prosseguir, as pastas 'dados-publicos' e 'dados-publicos-zip', devem estar vazias, senão poderá haver inconsistências (juntar dados de meses distintos).\n",'Criar Bases RedeCNPJ')
        #if eg.ynbox('Deseja apagar os arquivos das pastas ' + pasta_cnpj + ' e ' + pasta_zip + '?\nNÃO SERÁ POSSÍVEL REVERTER!!!!\n' + '\n'.join(arquivos_existentes) + '\nATENÇÃO: SE FOR EXECUTAR APENAS ALGUMA PARTE DO PROGRAMA, NÃO SELECIONE ESTA OPÇÃO, APAGUE MANUALMENTE.','Criar Bases RedeCNPJ', ['SIM-APAGAR', 'NÃO']):
        r = input('Deseja apagar os arquivos das pastas ' + pasta_cnpj + ' e ' + pasta_zip + '?\n' + '\n'.join(arquivos_existentes) + '\nATENÇÃO: SE FOR EXECUTAR APENAS ALGUMA PARTE DO PROGRAMA, NÃO SELECIONE ESTA OPÇÃO, APAGUE MANUALMENTE. \nNÃO SERÁ POSSÍVEL REVERTER!!!!\nDeseja prosseguir e apagar os arquivos (y/n)??')
        if r and r.upper()=='Y':
            for arq in arquivos_existentes:
                    print('Apagando arquivo ' + arq)
                    os.remove(arq)
        else:
            print('Parando... Apague os arquivos ' + pasta_cnpj + ' e ' + pasta_zip +' e tente novamente')
            input('Pressione Enter')
            sys.exit(1)
    # else:
    #     eg.msgbox("Este programa baixa arquivos csv.zip de dados abertos da Receita Federal e converte para uso na RedeCNPJ aplicativo.",'Criar Bases RedeCNPJ')

    # if len(glob.glob(os.path.join(pasta_zip,'*.zip'))):
    #     print(f'Há arquivos zip na pasta {pasta_zip}. Apague ou mova esses arquivos zip e tente novamente')
    #     input('Pressione Enter')
    #     sys.exit(1)
requisitos()

print(time.asctime(), f'Início de {sys.argv[0]}:')

soup_pagina_dados_abertos = BeautifulSoup(requests.get(url_dados_abertos).text, features="lxml")
try:
    ultima_referencia = sorted([link.get('href') for link in soup_pagina_dados_abertos.find_all('a') if link.get('href').startswith('20')])[-1]
except:
    print('Não encontrou pastas em ' + url_dados_abertos)
    r = input('Pressione Enter.')
    sys.exit(1)


url = url_dados_abertos + ultima_referencia
# page = requests.get(url)    
# data = page.text
soup = BeautifulSoup(requests.get(url).text, features="lxml")
lista = []
print('Relação de Arquivos em ' + url)
for link in soup.find_all('a'):
    if str(link.get('href')).endswith('.zip'): 
        cam = link.get('href')
        if not cam.startswith('http'):
            print(url+cam)
            lista.append(url+cam)
        else:
            print(cam)
            lista.append(cam)

if __name__ == '__main__':        
    resp = input(f'Deseja baixar os arquivos acima para a pasta {pasta_zip} (y/n)?')
    if resp.lower()!='y' and resp.lower()!='s':
        sys.exit()
        


print(time.asctime(), 'Início do Download dos arquivos...')

if True: #baixa usando parfive, download em paralelo
    downloader = parfive.Downloader()
    for url in lista:
        downloader.enqueue_file(url, path=pasta_zip, filename=os.path.split(url)[1])
    downloader.download()
else: #baixar sequencial, rotina antiga
    def bar_progress(current, total, width=80):
        if total>=2**20:
            tbytes='Megabytes'
            unidade = 2**20
        else:
            tbytes='kbytes'
            unidade = 2**10
        progress_message = f"Baixando: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
        # Don't use print() as it will print in new line every time.
        sys.stdout.write("\r" + progress_message)
        sys.stdout.flush()
      
    for k, url in enumerate(lista):
        print('\n' + time.asctime() + f' - item {k}: ' + url)
        wget.download(url, out=os.path.join(pasta_zip, os.path.split(url)[1]), bar=bar_progress)

        
print('\n\n'+ time.asctime(), f' Finalizou {sys.argv[0]}!!!')
print(f"Baixou {len(glob.glob(os.path.join(pasta_zip,'*.zip')))} arquivos.")
if __name__ == '__main__':
    input('Pressione Enter')

#lista dos arquivos (até julho/2024)
'''
http://200.152.38.155/CNPJ/Cnaes.zip
http://200.152.38.155/CNPJ/Empresas0.zip
http://200.152.38.155/CNPJ/Empresas1.zip
http://200.152.38.155/CNPJ/Empresas2.zip
http://200.152.38.155/CNPJ/Empresas3.zip
http://200.152.38.155/CNPJ/Empresas4.zip
http://200.152.38.155/CNPJ/Empresas5.zip
http://200.152.38.155/CNPJ/Empresas6.zip
http://200.152.38.155/CNPJ/Empresas7.zip
http://200.152.38.155/CNPJ/Empresas8.zip
http://200.152.38.155/CNPJ/Empresas9.zip
http://200.152.38.155/CNPJ/Estabelecimentos0.zip
http://200.152.38.155/CNPJ/Estabelecimentos1.zip
http://200.152.38.155/CNPJ/Estabelecimentos2.zip
http://200.152.38.155/CNPJ/Estabelecimentos3.zip
http://200.152.38.155/CNPJ/Estabelecimentos4.zip
http://200.152.38.155/CNPJ/Estabelecimentos5.zip
http://200.152.38.155/CNPJ/Estabelecimentos6.zip
http://200.152.38.155/CNPJ/Estabelecimentos7.zip
http://200.152.38.155/CNPJ/Estabelecimentos8.zip
http://200.152.38.155/CNPJ/Estabelecimentos9.zip
http://200.152.38.155/CNPJ/Motivos.zip
http://200.152.38.155/CNPJ/Municipios.zip
http://200.152.38.155/CNPJ/Naturezas.zip
http://200.152.38.155/CNPJ/Paises.zip
http://200.152.38.155/CNPJ/Qualificacoes.zip
http://200.152.38.155/CNPJ/Simples.zip
http://200.152.38.155/CNPJ/Socios0.zip
http://200.152.38.155/CNPJ/Socios1.zip
http://200.152.38.155/CNPJ/Socios2.zip
http://200.152.38.155/CNPJ/Socios3.zip
http://200.152.38.155/CNPJ/Socios4.zip
http://200.152.38.155/CNPJ/Socios5.zip
http://200.152.38.155/CNPJ/Socios6.zip
http://200.152.38.155/CNPJ/Socios7.zip
http://200.152.38.155/CNPJ/Socios8.zip
http://200.152.38.155/CNPJ/Socios9.zip
'''