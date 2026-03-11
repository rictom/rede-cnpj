# -*- coding: utf-8 -*-
"""
lista relação de arquivos na página de dados públicos da receita federal
e faz o download
Os arquivos estão disponíveis em
https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj
Houve alteração de layout da página de download em fev/2026.
Talvez seja necessário ajustar o valor do parâmetro share_token na função consulta_base_webdap
"""
from bs4 import BeautifulSoup
import requests, wget, os, sys, time, glob, parfive, re

pasta_zip = r"dados-publicos-zip" #local dos arquivos zipados da Receita
pasta_cnpj = 'dados-publicos'

def requisitos():
    #se pastas não existirem, cria automaticamente
    if not os.path.isdir(pasta_cnpj):
        os.mkdir(pasta_cnpj)
    if not os.path.isdir(pasta_zip):
        os.mkdir(pasta_zip)
        
    arquivos_existentes = [k for k in glob.glob(pasta_cnpj +'/*.*') if not k.endswith('.txt')] + list(glob.glob(pasta_zip + '/*.zip'))
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
#.def requisitos

def consulta_base_webdap(share_token="YggdBLfdninEJX9", base_url="https://arquivos.receitafederal.gov.br/public.php/webdav"):
    # código adaptado de github.com/caiopizzol/cnpj-data-pipeline/blob/main/downloader.py
    from xml.etree import ElementTree
    DAV_NS = {"d": "DAV:"} # WebDAV XML namespace
    url = base_url + "/"
    response = requests.request("PROPFIND", url, auth=(share_token, ""), headers={"Depth": "1"}) #, timeout=(self.config.connect_timeout, self.config.read_timeout))
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)
    
    directories = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        match = re.search(r"(\d{4}-\d{2})/?$", href) # Match YYYY-MM directory pattern from href path
        if match:
            directories.append(match.group(1))
            
    ultimoAnoMes = directories[-1]
    #obtem lista de arquivos
    response = requests.request("PROPFIND", url+ultimoAnoMes+"/", auth=(share_token, ""), headers={"Depth": "1"}) # ,timeout=(self.config.connect_timeout, self.config.read_timeout) )
    response.raise_for_status()
    root = ElementTree.fromstring(response.content)
    
    files = []
    for response in root.findall("d:response", DAV_NS):
        href = response.find("d:href", DAV_NS).text
        # Extract .zip filenames from href
        match = re.search(r"/([^/]+\.zip)$", href, re.IGNORECASE)
        if match:
            files.append(match.group(1))

    urlBaseArquivosDoMes = f'https://arquivos.receitafederal.gov.br/public.php/dav/files/{share_token}/{ultimoAnoMes}/' #para baixar usando requests sem webdav
    urlPaginaDownloadMeses = f'https://arquivos.receitafederal.gov.br/index.php/s/{share_token}'
    #return {'ultimoAnoMes':ultimoAnoMes, 'arquivos':files}
    return {'anoMes':ultimoAnoMes, 'urlBaseArquivosDoMes':urlBaseArquivosDoMes, 'arquivos':files, 'urlPaginaDownloadMeses':urlPaginaDownloadMeses}
#.def consulta_base_webdap

def consulta_base(bwebdap=True):
    #consulta primeiro por webdap, se não der certo abre a página com webdriver no modo headless
    r = None
    if bwebdap:
        try:
            r = consulta_base_webdap()
            print('Consulta por webdap')
        except Exception as e:
            print(f"Error: {e}")    
            print('Não conseguiu obter a data da base pelo método webdap...')
            print('Navegue até a página https://arquivos.receitafederal.gov.br/ e localize a pasta Dados>Cadastros>CNPJ')
            print('A url da página conterá um código (user ou share_token) após https://arquivos.receitafederal.gov.br/index.php/s/')
            print('Copie o user e atualize este código.') 
    return r
#.def consulta_base

print(time.asctime(), f'Início de {sys.argv[0]}:')

parametrosSite = consulta_base()
if not parametrosSite:
    r = input('Pressione Enter.')
    sys.exit(1)   
    
ultima_referencia = parametrosSite['anoMes']
urlBaseArquivosDoMes = parametrosSite['urlBaseArquivosDoMes']
print('Última base disponível:', ultima_referencia)
requisitos()

lista = [urlBaseArquivosDoMes+arq for arq in parametrosSite['arquivos']]

print('Relação de Arquivos disponíveis:')
for k in lista:
    print(k)

if __name__ == '__main__':        
    resp = input(f'Deseja baixar os arquivos acima para a pasta {pasta_zip} (y/n)?')
    if resp.lower()!='y' and resp.lower()!='s':
        sys.exit()
        
print(time.asctime(), 'Início do Download dos arquivos...')

if True: #baixa usando parfive, download em paralelo
    #downloader = parfive.Downloader()
    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Windows; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36", "Accept": "*/*"}
    downloader = parfive.Downloader(max_conn=5, max_splits=1, config=parfive.SessionConfig(headers=headers))
    for url in lista:
        downloader.enqueue_file(url, path=pasta_zip, filename=os.path.split(url)[1])
    downloader.download()
# else: #baixar sequencial, rotina antiga
#     def bar_progress(current, total, width=80):
#         if total>=2**20:
#             tbytes='Megabytes'
#             unidade = 2**20
#         else:
#             tbytes='kbytes'
#             unidade = 2**10
#         progress_message = f"Baixando: %d%% [%d / %d] {tbytes}" % (current / total * 100, current//unidade, total//unidade)
#         # Don't use print() as it will print in new line every time.
#         sys.stdout.write("\r" + progress_message)
#         sys.stdout.flush()
      
#     for k, url in enumerate(lista):
#         print('\n' + time.asctime() + f' - item {k}: ' + url)
#         wget.download(url, out=os.path.join(pasta_zip, os.path.split(url)[1]), bar=bar_progress)

        
print('\n\n'+ time.asctime(), f' Finalizou {sys.argv[0]}!!!')
print(f"Baixou {len(glob.glob(os.path.join(pasta_zip,'*.zip')))} arquivos.")
if __name__ == '__main__':
    input('Pressione Enter')

