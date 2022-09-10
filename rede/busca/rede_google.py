# -*- coding: utf-8 -*-
"""
Created on julho/2022
@author: github rictom/rede-cnpj
https://github.com/rictom/rede-cnpj
"""
import os, copy, json, sys, time, re, pathlib
#from bs4 import BeautifulSoup as soup
import requests
from requests.utils import quote
from urllib.parse import urlparse
import bs4
import asyncio
import aiohttp
import platform
if platform.system()=='Windows': #isso faz dá algum erro no windows
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
try:
    import rede_spacy
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

    def getPageGoogle(self, page=1, segundos_delay=2.0):
        global result, gtime
        if gtime:
            if abs(time.time()-gtime)<segundos_delay:
                time.sleep(segundos_delay)
        gtime = time.time()
        tamPagina = 10 #10
        #self.user_agent='Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:64.0) Gecko/20100101 Firefox/64.0'
        self.user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:101.0) Gecko/20100101 Firefox/101.0'
        self.headers={'User-Agent':self.user_agent}
        self.url="https://www.google.com/search?q="+quote(self.key) # + "&start=%d" % (tamPagina*(page-1)) + "&num=%d" %tamPagina 
        params = {'start':tamPagina*(page-1), 'num': tamPagina}
        self.page = requests.get(self.url, headers = self.headers, params=params).text #text is unicode; .content em bytes        
        content=bs4.BeautifulSoup(self.page, "html.parser") 
        #result=content.find(id="res").find_all("div", class_="jtfYYd") #pouco resultado, quando tem imagem do lado usa outro css
        # busca por css com class_ é imprecisa?? O google altera isso com frequência??
        self.result=content.find(id="res").find_all("div", class_=re.compile("(jtfYYd|Ww4FFb|GLI8Bc UK95Uc)")) #checa tres tipos
        dicImagens = dicImagensGoogle(self.page)
        self.links = set() #como está usando dois css de filtro, pode haver repetição.
        for item in self.result:
            bloco = bs4.BeautifulSoup(str(item), "html.parser")
            link = bloco.find("a",href=True).get('href')
            if link in self.links:
                #print('link repetido' + '-'*20, bloco)
                continue
            if pularSite(link):
                continue
            self.links.add(link)
            tlink = bloco.find("h3", class_="LC20lb").get_text()
            try: #às vezes esse elemento não existe
                texto = bloco.find_all("div", class_="VwiC3b")[0].get_text() 
            except:
                texto = ''
            try:
                #imagem = bloco.find('img').get_attribute_list('src')[0] #google troca imagem 
                imagemId = bloco.find('img').get('id')
            except:
                imagemId=''
            imagem = dicImagens.get(imagemId, 'link.png')
            self.dados.append({'link':link, 'tlink':tlink,'texto':texto, 'imagem':imagem}) #, 'imagemId':imagemId})
        gtime = time.time()
        if debug: print(f'Encontrou {len(self.links)} links na página {page} do Google.')
    #.def getPageGoogle
    
    def json_google_chaves_sincrono(self):
        '''baixa o texto das urls da página do google de forma síncrona'''
        #gs = self
        nos = []
        ligacoes =[]
        no = {'id': 'LI_' + self.url, 'descricao':self.key, "imagem":"google.png", "cor":"dodgerblue", 'camada': 0}
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
                       "label":'google'}
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
        nos = []
        ligacoes =[]
        no = {'id': 'LI_' + self.url, 'descricao':self.key, "imagem":"google.png", "cor":"dodgerblue", 'camada': 0}
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
                       "label":'google'}
            ligacoes.append(copy.deepcopy(ligacao))
        # if bspacy and self.n_palavras_chave: #abre identificadores
        #     #listaUrl = [item['link'] for item in self.dados]
        #     nos_ligacoes_p = await json_busca_palavras_urls(self.links, self.n_palavras_chave)
        #     nos.extend(copy.deepcopy(nos_ligacoes_p['no']))
        #     ligacoes.extend(copy.deepcopy(nos_ligacoes_p['ligacao']))
            
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
    resultadoDownload = await downloadLista(listaUrl)
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
     
    # else:
    #     async with aiofiles.open('filename', mode='r') as f:
    #         contents = await f.read()
    #     print(contents)
    return texto

# def removeSufixo(line, suffix): #no python 3.9, substituir por removesuffix
#     if not line:
#         return ''
#     if line.endswith(suffix):
#         return line[:-len(suffix)]
#     else:
#         return line
# #.def removeSufixo

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

async def json_busca(termos, pagina=1, n_palavras_chave=20):
    gs = googleSearch()
    #print(termos, pagina, n_palavras_chave)
    gs.search(termos, pagina, n_palavras_chave)
    tjson = gs.json_google()
    tjsonc = await json_busca_palavras_urls(gs.links, n_palavras_chave)
    juntaJson(tjson, tjsonc)
    return tjson
#.async def json_busca

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
    if False: #baixa os arquivos dos links de forma sincrona
        gs.search(termos, 1, 0)
        print(gs.dados)
        for k in gs.dados:
            print(k['link'])
        print(len(gs.dados))
        #gs.salva_json(gs.dados)
        #print(gs.json_google_s())
    elif True:
        x = json_google_chaves_sincrono(termos, 1, 20)
        print(x)
    elif False:
        x = asyncio.run(json_busca(termos, 1, 20))
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
    elif False:
        dic = dicImagensGoogle(gs.page)
    if False:
        r = json_busca_palavras_doc('id', r"caminho.pdf")
        print(r)

