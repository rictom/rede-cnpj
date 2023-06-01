# -*- coding: utf-8 -*-
"""
Created on julho/2022
@author: github rictom/rede-cnpj
https://github.com/rictom/rede-cnpj

pip install spacy
pip install langdetect
python -m spacy download en_core_web_sm
https://spacy.io/models/pt#pt_core_news_md
python -m spacy download pt_core_news_lg 
#(500MB)
--criar symbolic link no linux, se não achar o modelo
ln -s /home/..../lib/python3.9/site-packages/pt_core_news_sm/pt_core_news_sm-3.4.0 pt_core_news_lg
ln -s /home/..../lib/python3.9/site-packages/en_core_web_sm/en_core_web_sm-3.4.0 en_core_web_sm

#para ocr
conda install -c conda-forge poppler 
pip install pdftotext
"""
#%%
# import spacy
# nlp = spacy.load('en_core_web_sm')
# sentence = "Apple is looking at buying U.K. startup for $1 billion"
# doc = nlp(sentence)
# for ent in doc.ents:
#     print(ent.text, ent.start_char, ent.end_char, ent.label_)
    
#%%
import spacy, time
nlps = {}  
#nlp = spacy.load('pt_core_news_sm')
#nlp = spacy.load('pt_core_news_lg') #parece que causa lentidão para o app iniciar
# sentence = "Petrobrás tem dívida bilionária"
# doc = nlp(sentence)
# for ent in doc.ents:
#     print(ent.text, ent.start_char, ent.end_char, ent.label_)
    
#%%
#https://www.howtouselinux.com/post/insecurerequestwarning-in-python-urllib3-requests
import requests
requests.packages.urllib3.disable_warnings()
# from urllib3.exceptions import InsecureRequestWarning
# from urllib3 import disable_warnings
# disable_warnings(InsecureRequestWarning)

from bs4 import BeautifulSoup as soup
import re, sys
import collections
import os, tempfile #, mimetypes
import textract
import langdetect 
#pip install textract
#para ler pdf, pdftotext 2.2.2 instalar poppler
#conda install -c conda-forge poppler 
#problema de certificado no windows (não funcionou)
#pip install pip_system_certs
#gambiarra, colocar verify=False e desativar notificações do requests.

#https://github.com/pythonprobr/palavras #lista de palavras a partir de dicionário do LibreOffice
#https://github.com/fserb/pt-br #lista com verbos conjugados (arquivo conjucações.txt)
camPalavras = os.path.join(os.path.dirname(__file__), 'palavras.txt')
camPalavras_conjugacoes = os.path.join(os.path.dirname(__file__), 'palavras_conjugacoes.txt')
spalavras = set(open(camPalavras, encoding='utf8').read().split('\n')).union(set(open(camPalavras_conjugacoes, encoding='utf8').read().split('\n')))

user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:101.0) Gecko/20100101 Firefox/101.0'
headers={'User-Agent':user_agent}

def pegaTextoURL(url):
    #response = requests.head(url, allow_redirects=True, timeout=5, headers=headers, verify='certs.pem') #, verify=False)
    response = requests.head(url, allow_redirects=True, timeout=5, headers=headers, verify=False) #, verify=False)
    size = response.headers.get('content-length', -1)
    if int(size)>10000000: #se for muito grande, 10MB, não carrega
        print(f'Arquivo não baixado por ser grande {int(size)//1000000} MB: ', url)
        return ''
    # tipoMime = mimetypes.guess_extension(url)
    # tipo = None
    # if tipoMime:
    #     tipo = mimetypes.guess_extension(tipoMime[0])
    #response = requests.get(url, timeout=10, headers=headers, verify='certs.pem') #, verify=False) #.text #text is unicode; .content em bytes    
    response = requests.get(url, timeout=10, headers=headers, verify=False) #, verify=False) #.text #text is unicode; .content em bytes    
    #if response.headers['Content-Type']== 'application/pdf':   
    if response.headers['Content-Type']== 'application/pdf':   
        with tempfile.NamedTemporaryFile(delete=False) as temp: #com delete=True está dando erro de leitura
            temp.write(response.content)
            temp.flush()
            #content = textract.process(temp.name,encoding='utf-8',extension=".pdf")
            content = textract.process(temp.name, extension='.pdf')
            try:
                sentence = content.decode('latin1')
            except:
                sentence = content.decode('utf8')
            nomeArquivoTemp = temp.name
        # with open('temp.pdf','wb') as temp:
        #     temp.write(response.content)
        #     content = textract.process('temp.pdf', extension=".pdf")
        #     sentence = content.decode('utf8')
        if os.path.exists(nomeArquivoTemp):
            os.remove(nomeArquivoTemp)
    elif response.headers['Content-Type'].startswith('application/'):
        print('Não baixou ', response.headers['Content-Type'])
        sentence = ''
    else:
        content = soup(response.text, "html.parser")
        sentence = content.text
    return sentence
#.def pegaTextoURL
    
def palavrasChaveDeURL(url, ntermos=20, textoIn = ''):
    if textoIn:
        sentence = textoIn
    else:
        try:
            sentence = pegaTextoURL(url)
        except Exception as err:
            print('erro em palavrasChaveDeURL: ', err)
            sentence = ''
    try:
        return palavrasChave(sentence, ntermos=ntermos)    
    except Exception as err:
        print('erro em palavrasChaveDeURL>palavrasChave: ', err)
        return []
#.def palavrasChaveDeURL        

def palavrasChave(textoIn, ntermos=20):
    global nlps
    if not nlps:
        nlps['pt'] = spacy.load('pt_core_news_lg')
        nlps['en'] = spacy.load('en_core_web_sm')
        #nlps['pt'] = spacy.load('/home/ubuntu/rede/redev/lib/python3.9/site-packages/pt_core_news_sm/pt_core_news_sm-3.4.0')
        #nlps['en'] = spacy.load('/home/ubuntu/rede/redev/lib/python3.9/site-packages/en_core_web_sm/en_core_web_sm-3.4.0')        
    if not textoIn:
        return []
    lang = langdetect.detect(textoIn)
    nlp = nlps.get(lang, nlps['pt'])
    doc = nlp(textoIn)

    cnt = collections.Counter()  
    termos = []
    lista = []
    pattern = re.compile('([^\s\w]|_)+')
    for ent in doc.ents:
        #print(ent.text, ent.start_char, ent.end_char, ent.label_)
        lista.append((ent.text, ent.label_))
        if '\n' in ent.text:
            continue
        if ent.text[0] != ent.text[0].upper():
            continue
        if ent.label_ in ('PER','ORG') : #, 'LOC'):
            tlower = ent.text.lower()
            if ent.label_=='PER' and ( ' ' not in ent.text): #se for nome simples, ignora
                    continue
            if pattern.search(ent.text) or palavraComMaiusculaNoMeio(ent.text):
                continue
            if tlower in spalavras: #na lista de palavras, se for nome próprio começa com maísculas
                continue
            taux = tlower.removesuffix('s')
            if taux in spalavras:
                continue
            if taux.endswith('o'): #gambiarra, o dicionário não tem flexão de gênero
                if (taux.removesuffix('o') + 'a') in spalavras:
                    continue
            if taux.endswith('a'):
                if (taux.removesuffix('a') + 'o') in spalavras:
                    continue

            #termos.append(pattern.sub(' ', ent.text))
            termos.append(ent.text)
    cnt = collections.Counter(termos)
    #print('Mais comuns ', '-'*20)
    #print(cnt.most_common(ntermos))
    return [k.strip() for (k,c) in cnt.most_common(ntermos)]
#.def palavrasChaveDeURL

def palavraComMaiusculaNoMeio(t):
    caracAnt = ''
    for carac in t:
        if caracAnt and caracAnt==caracAnt.lower() and caracAnt.isalnum():
            if carac==carac.upper() and carac.isalnum():
                return True
        caracAnt = carac
    return False
#.def palavraComMaiusculaNoMeio
#%%

if __name__ == '__main__':
    url = "https://www.redecnpj.com.br/rede/"
    inicio = time.time()
    print(palavrasChaveDeURL(url))
    print(time.time() - inicio)