# -*- coding: utf-8 -*-
"""
Created on julho/2022
@author: github rictom/rede-cnpj
https://github.com/rictom/rede-cnpj
"""
import os, copy, json, time, re, pathlib, ddgs
#from bs4 import BeautifulSoup as soup
import requests
from requests.utils import quote, unquote
from urllib.parse import urlparse
import bs4
import asyncio
import aiohttp
import platform
if platform.system()=='Windows': #evitar um erro no windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
try: #spacy usa muita memória. Se não for utilizar, não colocar rede_spacy na pasta busca.
    from . import rede_spacy
    bspacy = True
except Exception as err:
    print('módulo rede_spacy não foi carregado: ', err)
    bspacy = False
#https://realpython.com/python-requests/
#https://www.codementor.io/@aviaryan/downloading-files-from-urls-in-python-77q3bs0un
#https://stackoverflow.com/questions/55226378/how-can-i-get-the-file-size-from-a-link-without-downloading-it-in-python
#https://365datascience.com/tutorials/python-tutorials/python-requests-package/

gtime = None
ktamanhoMaximoArquivoDownload = 10000000
debug = True
imagens = []
gsitesAIgnorar = None

kCamArquivoSitesAIgnorar = os.path.join(os.path.dirname(__file__), 'rede_google_sites_a_ignorar.txt')
def pularSite(url):
    global gsitesAIgnorar
    if gsitesAIgnorar is None:
        if os.path.exists(kCamArquivoSitesAIgnorar):
            gsitesAIgnorar = set(open(kCamArquivoSitesAIgnorar).read().split('\n'))
        else:
            gsitesAIgnorar = set()
    dominio = urlparse(url).netloc
    if dominio.removeprefix('www.') in gsitesAIgnorar:
        return True
    else:
        return False
#.def pularSite

def tamanhoArquivo(url):
    response = requests.head(url, allow_redirects=True)
    size = response.headers.get('content-length', -1)
    # size in megabytes (Python 2, 3)
    print('{:<40}: {:.2f} MB'.format('FILE SIZE', int(size) / float(1 << 20)))
    return size
    # size in megabytes (f-string, Python 3 only)
    # print(f"{'FILE SIZE':<40}: {int(size) / float(1 << 20):.2f} MB")
    
def baixarArquivo(url, nomeArquivoLocal=''):
    response = requests.get(url)
    with open(nomeArquivoLocal, 'wb') as file:
        file.write(response.content)
    #variante, pegar pedaços
    if False:
        r = requests.get(url, stream=True)
        with open(nomeArquivoLocal, 'wb') as f:
            for chunk in r.iter_content(chunk_size=16*1024):
                f.write(chunk)

'''
utilizava código de  https://github.com/Nv7-GitHub/googlesearch

'''
import random

        
class googleSearch():

    def __init__(self):
        self.dados = []
        
    def search(self, key, page=1, n_palavras_chave=20, segundos_delay=2.0):
        '''raspa o google. se key tiver número depois de @, será considerado o número de palavras chave para se obter'''
        self.key = key #.replace(' ','+')
        self.n_palavras_chave = n_palavras_chave
        if key:
            self.getPageGoogle(page=page, segundos_delay=segundos_delay)
        else:
            self.page=''
            


    '''
    utilizava código de     https://github.com/Nv7-GitHub/googlesearch, parou de funcionar em outubro/2025 - parou suporte ao Lynx
    usando agora ddgs (busca pelo duckduckgo)
    
    '''
    def getPageGoogle(self, page=1, segundos_delay=2.0):
    #def search(term, num_results=10, lang="en", proxy=None, advanced=False, sleep_interval=0, timeout=5, safe="active", ssl_verify=None, region=None, start_num=0, unique=False):
        """Search the Google search engine"""
        self.url="https://duckduckgo.com/?q="+quote(self.key) # + "&start=%d" % (tamPagina*(page-1)) + "&num=%d" %tamPagina 
        self.links = set() #como está usando dois css de filtro, pode haver repetição.
        
        #lang="en"; proxy=None; advanced=False; sleep_interval=0; timeout=5; safe="active"; ssl_verify=None; region=None; 
        unique=False
        sleep_interval = segundos_delay
        # Proxy setup
        #proxies = {"https": proxy, "http": proxy} if proxy and (proxy.startswith("https") or proxy.startswith("http")) else None
        tamPagina = 10
        num_results= 10
        #start = tamPagina*(page-1) #start_num
        fetched_results = 0  # Keep track of the total fetched results
        fetched_links = set() # to keep track of links that are already seen previously
        
        term = self.key
        #while fetched_results < num_results:

        with ddgs.DDGS() as searchd:
            #resp = searchd.text(term, page=page, region='br-pt', backend='duckduckgo')
            resp = searchd.text(term, page=page, region='br-pt', backend='yandex, bing, duckduckgo') #wikipedia é sempre incluido... causa inconsistências
        dicImagens = {} #dicImagensGoogle(resp.text) #não está funcionando, busca só funciona com user agent lynx, navegador de texto

        new_results = 0  # Keep track of new results in this iteration
        for result in resp:
            link = result['href']
            if link in fetched_links and unique:
                continue  # Skip this result if the link is not unique
            if pularSite(link):
                continue
            fetched_links.add(link)
            self.links.add(link)
            self.dados.append({'link':link, 'tlink':result['title'], 'texto':result['body'], 'imagem':''})
            if fetched_results >= num_results:
                break  # Stop if we have fetched the desired number of results
        #start += 10  # Prepare for the next set of results
        time.sleep(sleep_interval)
    #.def getPageGoogle(

    
    def json_google_chaves_sincrono(self):
        '''baixa o texto das urls da página do google de forma síncrona'''
        #gs = self
        #print('json_google_chaves_sincrono')
        nos = []
        ligacoes =[]
        no = {'id': 'LI_' + self.url, 'descricao':self.key, "imagem":"search.png", "cor":"dodgerblue", 'camada': 0}
        nos.append(copy.deepcopy(no))
        for item in self.dados:
            origem = 'LI_' + self.url
            destino = 'LI_' + item['link']
            #no = {'id': destino, 'descricao':item['tlink'], 'nota':item['texto'], "imagem":"link.png", "cor":"yellow", 'camada': 1}
            no = {'id': destino, 'descricao':item['tlink'], 'nota':item['texto'], "imagem":item['imagem'], "cor":"yellow", 'camada': 1}
            no['imagem'] = item['imagem']
            nos.append(copy.deepcopy(no))
            ligacao = {"origem":origem, 
                       "destino":destino, 
                       "cor": "silver", 
                       "camada":0,
                       "tipoDescricao":'link',
                       "label":'busca'}
            ligacoes.append(copy.deepcopy(ligacao))
        if bspacy and self.n_palavras_chave: #abre identificadores
            for item in self.dados:
                if debug: print('lendo ' + item['link'])
                try:
                    palavras = rede_spacy.palavrasChaveDeURL(item['link'], self.n_palavras_chave)
                except Exception as err:
                    print('Erro em palavrasChaveDeURL', err)
                    continue
                origem = 'LI_' + item['link']
                for p in palavras:
                    #destino = 'LI_' + "https://www.google.com/search?q="+quote('"'+ p + '"')
                    destino = 'CH_' + p
                    no = {'id': destino, 'descricao':'', 'nota':'',  "imagem":"key.png", "cor":"", 'camada': 2}
                    nos.append(copy.deepcopy(no))
                    ligacao = {"origem":origem, 
                               "destino":destino, 
                               "cor": "silver", 
                               "camada":0,
                               "tipoDescricao":'link',
                               "label":'chave'}
                    ligacoes.append(copy.deepcopy(ligacao))                
                
        textoJson={'no': nos, 'ligacao':ligacoes, 'mensagem':''} 
        return textoJson
    #.def json_google_chaves_sincrono

    
    def json_google(self):
        '''baixa o texto das urls da página do google de forma assíncrona'''
        #gs = self
        #print('json_google')
        nos = []
        ligacoes =[]
        no = {'id': 'LI_' + self.url, 'descricao':self.key, "imagem":"search.png", "cor":"dodgerblue", 'camada': 0}
        nos.append(copy.deepcopy(no))
        for item in self.dados:
            origem = 'LI_' + self.url
            destino = 'LI_' + item['link']
            no = {'id': destino, 'descricao':item['tlink'], 'nota':item['texto'], "imagem":item['imagem'] # if item.get('imagem') else "link.png" 
                  , "cor":"yellow", 'camada': 1}
            nos.append(copy.deepcopy(no))
            ligacao = {"origem":origem, 
                       "destino":destino, 
                       "cor": "silver", 
                       "camada":0,
                       "tipoDescricao":'link',
                       "label":'busca'}
            ligacoes.append(copy.deepcopy(ligacao))
        # if bspacy and self.n_palavras_chave: #abre identificadores
        #     #listaUrl = [item['link'] for item in self.dados]
        #     nos_ligacoes_p = await json_busca_palavras_urls(self.links, self.n_palavras_chave)
        #     nos.extend(copy.deepcopy(nos_ligacoes_p['no']))
        #     ligacoes.extend(copy.deepcopy(nos_ligacoes_p['ligacao']))
        #print(f'{nos=}')
        return {'no': nos, 'ligacao':ligacoes, 'mensagem':''} 
    #.async def json_google
    
    def salva_json(self, dados, nome_arquivo='rede_google.json'):
        j = self.json_google()
        with open(nome_arquivo, 'wt') as f:
            f.write(json.dumps(j))
    #.def salva_json
#.class googleSearch():

def json_busca_palavras_doc(arqId, caminho, n_palavras_chave=20):
    nos = []
    ligacoes = []
    start_time = time.time()
    #resultadoDownload = await downloadLista(listaUrl)
    resultadoDownload = [[caminho, textoDocumentoLocal(caminho)]]
    if debug: print('tempo download', time.time()-start_time)
    start_time = time.time()
    #return None
    for url, texto in resultadoDownload:
        palavras = []
        if texto:
            try:
                soup=bs4.BeautifulSoup(texto,'html.parser')
                #print('sopa', soup.title)
                palavras = rede_spacy.palavrasChave(soup.text, n_palavras_chave)
                #if debug: print(palavras)
            except Exception as err:
                print('erro na sopa: ', err)
                palavras = []
        origem = arqId #'LI_' + url
        for p in palavras:
            destino = 'CH_' + p
            no = {'id': destino, 'descricao':'' #p
                  , 'nota':''
                  , "imagem":"key.png", "cor":"", 'camada': 2}
            nos.append(copy.deepcopy(no))
            ligacao = {"origem":origem, 
                       "destino":destino, 
                       "cor": "silver", 
                       "camada":0,
                       "tipoDescricao":'link',
                       "label":'chave'}
            ligacoes.append(copy.deepcopy(ligacao))                
    if debug: print('tempo parse keys', time.time()-start_time)
    #return nos, ligacoes
    #textoJson={'no': nos, 'ligacao':ligacoes, 'mensagem':''} 
    return {'no': nos, 'ligacao':ligacoes, 'mensagem':''}     
#.def json_busca_palavras_urls    

async def json_busca_palavras_urls(listaUrl, n_palavras_chave=20):
    nos = []
    ligacoes = []
    start_time = time.time()
    #print('json_busca_palavras_urls')
    resultadoDownload = await downloadLista(listaUrl)
    #print('json_busca_palavras_urls - resultadoDownload')
    if debug: print('tempo download', time.time()-start_time)
    start_time = time.time()
    #return None
    for url, texto in resultadoDownload:
        palavras = []
        if texto:
            try:
                soup=bs4.BeautifulSoup(texto,'html.parser')
                #print('sopa', soup.title)
                palavras = rede_spacy.palavrasChave(soup.text, n_palavras_chave)
                #if debug: print(palavras)
            except Exception as err:
                print('erro na sopa: ', err)
                palavras = []
        origem = 'LI_' + url
        for p in palavras:
            destino = 'CH_' + p
            no = {'id': destino, 'descricao':'' #p
                  , 'nota':''
                  , "imagem":"key.png", "cor":"", 'camada': 2}
            nos.append(copy.deepcopy(no))
            ligacao = {"origem":origem, 
                       "destino":destino, 
                       "cor": "silver", 
                       "camada":0,
                       "tipoDescricao":'link',
                       "label":'chave'}
            ligacoes.append(copy.deepcopy(ligacao))                
    if debug: print('tempo parse keys', time.time()-start_time)
    #return nos, ligacoes
    #textoJson={'no': nos, 'ligacao':ligacoes, 'mensagem':''} 
    return {'no': nos, 'ligacao':ligacoes, 'mensagem':''}     
#.async def json_busca_palavras_urls

user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:101.0) Gecko/20100101 Firefox/101.0'
headers={'User-Agent':user_agent}

async def downloadLista(lista):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in lista:
            tasks.append(asyncio.ensure_future(textoURL(session, url)))
        try:
            original = await asyncio.gather(*tasks)
            #print(original)
        except Exception as err:
            print('erro em download: ', err)
            original =''

        return original
#.async def downloadLista

async def textoURL(session, url):
    import textract, aiofiles
    texto = ''
    #print('lendo: ', url)
    try:
        async with session.get(url, verify_ssl=False, timeout=15, headers=headers) as response:
            size = response.headers.get('Content-Length', '0')
            if int(size)>ktamanhoMaximoArquivoDownload: #se for muito grande, 10MB, não carrega
                print(f'Arquivo não baixado por ser grande: {int(size)//1000000} MB', url)
                return url, ''
            #print(response.headers)
            if response.headers['Content-Type']== 'application/pdf':  
                async with aiofiles.tempfile.NamedTemporaryFile('wb', delete=False) as temp:
                    await temp.write(await response.read())
                    temp.flush()
                    #content = textract.process(temp.name,encoding='utf-8',extension=".pdf")
                    content = textract.process(temp.name, extension='.pdf')
                    try:
                        texto = content.decode('latin-1')
                        if debug: print('pdf em latin-1')
                    except Exception as err:
                        print('erro em decode latin: ', err)
                        try:
                            texto = content.decode('utf-8')
                            if debug: print('pdf em utf8')
                        except Exception as err:
                            print('não conseguiu ler pdf:', err)
                            texto = ''
                    nomeArquivoTemp = temp.name
                if os.path.exists(nomeArquivoTemp):
                    os.remove(nomeArquivoTemp)
            elif response.headers['Content-Type'].startswith('application/'):
                print('Não baixou arquivo tipo ', response.headers['Content-Type'])
                texto = ''
            else:
                #print('tentando ler html')
                content = bs4.BeautifulSoup(await response.text(), "html.parser")
                texto = content.text
        #.async with
    except Exception as err:
        print('erro em textoURL: ', err)
    if len(texto)>1000000: #texto muito grande dá erro com o spacy
        print('texto muito grande, será ignorado', url)
        return url, ''
    return url, texto
#.async def textoURL

def textoDocumentoLocal(caminho):
    import textract
    #texto = ''
    lencoding = ('latin-1', 'utf-8')
    extensao = pathlib.Path(caminho).suffix
    if extensao in ('.exe', '.com', '.vbs', '.py', '.msi'):
        return ''
    if extensao=='.pdf':
        content = textract.process(caminho, extension='.pdf', encoding='utf-8')
    elif extensao=='.docx':
        content = textract.process(caminho, encoding='utf-8')
    else:
        #content = open(caminho, 'rb').read()
        content = textract.process(caminho)
    for enc in lencoding:
        if extensao in ('.pdf','.docx') and enc=='latin-1':
            continue
        try:
            texto = content.decode(enc)
            if debug: print('em ' + enc)
            return texto
        except Exception as err:
            print('erro em decode latin: ', err)
        
    try:
        return texto
    except:
        print(f'não conseguiu ler arquivo {caminho}:')
        return ''   
     
    return texto


def dicImagensGoogle(page):
    ''' o google bagunça as imagens, colocando imagem base no meio da seção <script>'''
    lista = []
    dicImagens = {}

    scripts = bs4.BeautifulSoup(page, 'html.parser').find_all('script')
    pat = re.compile(r"var s\=\'data\:image(?P<base>.*)\'\;var ii\=\[\'(?P<id>.*)\'\]\;")
    patldi = re.compile(r"google.ldi\=\{(?P<dldi>.*)\}\;google.pim\=")
    for k, s in enumerate(scripts):
        if 'data:image/' in str(s):
            x = pat.search(str(s)) 
            if x:
                lista.append((x.group('base'), x.group('id')))
                dicImagens[x.group('id')] = 'data:image' + x.group('base').removesuffix('\\x3d').removesuffix('\\x3d') #.removesuffix(r'\\x3d')
        if 'google.ldi=' in str(s):
            y = patldi.search(str(s))
            if y:
                ty= '{' + y.group('dldi') + '}'
                dy = json.loads(ty)
                for k, v in dy.items():
                    dicImagens[k] = v
    return dicImagens
#.def dicImagensGoogle

def juntaJson(tjson, tjsonAJuntar):
    nos = tjson['no']
    ligacoes = tjson['ligacao']
    nos.extend(copy.deepcopy(tjsonAJuntar['no']))
    ligacoes.extend(copy.deepcopy(tjsonAJuntar['ligacao']))   
    return
#.def juntaJson

def json_google_chaves_sincrono(termos, pagina=1, n_palavras_chave=20):
    gs = googleSearch()
    #print(termos, pagina, n_palavras_chave)
    gs.search(termos, pagina, n_palavras_chave)
    return gs.json_google_chaves_sincrono()
#.def json_google_chaves_sincrono

#async def json_busca(termos, pagina=1, n_palavras_chave=20):
async def json_google_chaves(termos, pagina=1, n_palavras_chave=20, segundos_delay=2.0):
    gs = googleSearch()
    #print(termos, pagina, n_palavras_chave)
    gs.search(termos, pagina, n_palavras_chave, segundos_delay=segundos_delay)
    tjson = gs.json_google()
    if n_palavras_chave:
        tjsonc = await json_busca_palavras_urls(gs.links, n_palavras_chave)
        juntaJson(tjson, tjsonc)
    return tjson
#.async def json_google_chaves

# async def json_busca_palavras_urls(listaLinks, n_palavras_chave=20):
#     nos, ligacoes = await json_busca_palavras_urls(listaLinks, n_palavras_chave)
#     textoJson={'no': nos, 'ligacao':ligacoes, 'mensagem':''} 
#     return textoJson
# #.async def json_busca

if __name__ == '__main__':
    gs = googleSearch()
    #input está dando erro no spyder 5.1
    #termos = input('Digite texto para pesquisar no Google: ')
    termos = 'cgu tcu'
    termos = 'eike batista'
    if True: #baixa os arquivos dos links de forma sincrona
        print(f'{termos=}')
        gs.search(termos, 1, 0)
        for k in gs.dados:
            print(k['link'])
        print(len(gs.dados))
        #gs.salva_json(gs.dados)
        #print(gs.json_google_s())
    elif False:
        x = json_google_chaves_sincrono(termos, 1, 20)
        print(x)
    elif False:
        x = asyncio.run(json_google_chaves(termos, 1, 20))
        print(x)
    elif False: #assíncrona
        gs.search(termos, 1, 10)
        #print(gs.dados)
        for k in gs.dados:
            print(k['link'])
        j = asyncio.run(gs.json_google())
        #print('json: ', j)
        #gs.salva_json(gs.dados)
    elif False:
        url = r'https://www.folha.com.br/'
        r = asyncio.run(json_busca_palavras_urls([url], 20))
        print(r)
    if False:
        dic = dicImagensGoogle(gs.page)
    if False:
        r = json_busca_palavras_doc('id', r"caminho.pdf")
        print(r)

