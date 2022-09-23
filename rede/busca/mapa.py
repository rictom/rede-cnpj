# -*- coding: utf-8 -*-
"""
Created on Fri Sep  2 20:05:44 2022

author: github rictom/rede-cnpj
https://github.com/rictom/rede-cnpj

adapted from
https://www.python-graph-gallery.com/312-add-markers-on-folium-map

https://nominatim.org/release-docs/latest/api/Search/
street=<housenumber> <streetname>
city=<city>
county=<county>
state=<state>
country=<country>
postalcode=<postalcode>
format=[xml|json|jsonv2|geojson|geocodejson]
https://medium.com/@nargessmi87/how-to-customize-the-openstreetmap-marker-icon-and-binding-popup-ab2254bddec2

"""

import folium, pandas as pd, json, random, time, os
import requests
from requests.utils import quote

camMapaMun = os.path.join(os.path.dirname(__file__), 'mapa_municipios_lat_long.json')
with open(camMapaMun, 'r') as infile:
    dicMun = json.load(infile)

def geraMapa(dados, qteMaximaGeocoding=10, mostraTooltip=True):
    '''dados é uma lista, normalmente o json_dados['no'] 
        por exemplo:
        dados = [{'id':'PJ_11111', 'descricao':'Nome Empresa X', pais:'Brasil', 'uf':'GO', 'municipio':'Goiania',
                 'logradouro':'Quadra X 5', 'logradouro_complemento':'apto 10'} , {...item seguinte...} ]
        #pais é opcional, se não tiver, supõe Brasil
    '''
    m = folium.Map(location=[-15.7801, -47.9292], tiles="OpenStreetMap", zoom_start=4) #Coordenadas de Brasília
    slatlon = set()
    dadosEnderecos =[]
    for k in dados:
        # if ('municipio' not in k) or ('logradouro' not in k) or ('uf' not in k):
        #     continue
        if not any(item in ['municipio','logradouro','uf','pais'] for item in k.keys()):
            continue
        dadosEnderecos.append(k)
    
    for k in dadosEnderecos:
        comentario = ''
        dadosgeo = None
        if len(dadosEnderecos)<=qteMaximaGeocoding:
            dadosgeo = geocode(k)
            #print(dadosgeo)
        if dadosgeo:
            lat, long = dadosgeo['lat'], dadosgeo['lon']
        else:
            #print('deu erro, usando coordenadas do municipio')
            ufmun = k.get('uf','')+'/'+k.get('municipio','')
            lat, long = dicMun.get(ufmun,(None,None))
            comentario = 'Coordenadas do Município'
            if len(dadosEnderecos)<=qteMaximaGeocoding:
                comentario = 'Erro no geocoding. ' + comentario
        if lat is None:
            #print('Não encontrou coordenadas de ' + ufmun)
            continue
        lat, long = float(lat), float(long)
        # if (lat,long) in slatlon:
        #     deslocamento = 1.0 + (random.random()/100000 if dadosgeo else random.random()/1000)
        # else:
        #     deslocamento = 1.0
        deslocamento = 0
        while (lat * (1.0 + deslocamento), long * (1.0 + deslocamento)) in slatlon:
            deslocamento += 1/100000 if dadosgeo else 2/100000
        #print('lat, long', lat, long)
        lat, long = lat * (1.0 + deslocamento), long * (1.0 + deslocamento)
        slatlon.add((lat,long))
        logradouro = k.get('logradouro','')
        pais = k.get('pais','Brasil')
        urlgoogle = r'https://www.google.com.br/maps/place/' + quote(k.get('logradouro','').removesuffix('S/N')) + ',' + quote(k.get('municipio','')) 
        urlgoogle += '' if k.get('uf','EX')=='EX' else '-'+ k.get('uf','EX')
        urlgoogle += ' ' + k.get('pais','')
        if k['id'].startswith('PJ'):
            ttip = 'CNPJ ' + '<b>' + k['id'].removeprefix('PJ_') + '</b>'
        else:
            ttip = '<b>' + k['id'] + '</b>'
        ttip += f"""<br><br><b>{k['descricao']}</b><br><br>{logradouro}"""
        ttip += f"<br>{k['logradouro_complemento']}" if k.get('logradouro_complemento','') else ''
        ttip += f"<br>{k['municipio']}" if k['municipio'] else ''
        ttip += f"{'/' + k.get('uf','') if k.get('uf','') not in ('EX','') else ''}"
        ttip += f' - {pais}' if pais!='Brasil' else ''
        ttip += f"<br><br>{comentario}" if comentario else ''
        linkGoogle = f"""<a href={urlgoogle} target='_blank' title='Abrir no Google Maps'><img src="../static/imagem/google.png" alt="Google" style="width:11px;height:11px;" ></a>"""
        linkGoogle += f"""   <a href='/rede/grafico/1/{k['id']}' target='_blank' title='Abrir Rede deste cnpj em nova aba'><img src="../static/imagem/favicon.ico?v=1" alt="Rede" style="width:11px;height:11px;" /></a>"""
        linkGoogle += f"""   <button onclick='javascript:window.opener.menu_dados(true, "{k['id']}");' title='Dados do cnpj'><img src="../static/imagem/drivers-license-o.png" alt="Dados" style="width:11px;height:11px;" /></button>"""
        linkGoogle += f"""   <button onclick='javascript:window.opener.selecionaNoid("{k['id']}", false); javascript:window.blur(); javascript:window.opener.focus();' title='Seleciona na Rede de origem'><img src="../static/imagem/hand-o-left.png" alt="Dados" style="width:11px;height:11px;" /></button><br>"""
        #tpopup = linkGoogle + f""" {ttip.replace('<br>',' - ').replace(' - ','',1)}"""
        tpopup = linkGoogle + ttip
        
        if mostraTooltip:
            folium.Marker(location=[lat , long], popup=tpopup, tooltip=folium.Tooltip(ttip)).add_to(m)
        else:
            folium.Marker(location=[lat , long], popup=tpopup).add_to(m)        
        #tooltip=folium.Tooltip("test", permanent=True)
    #m.save('folium.html')
    import io
    outputStream = io.BytesIO() #io.StringIO()
    m.save(outputStream, close_file=False)
    outputStream.seek(0)
    return outputStream
#.def geraMapa

def geocode(k):
    urlBase=r'https://nominatim.openstreetmap.org/search?'
    logradouro = k.get('logradouro','').replace('S/N' ,' ')
    if k.get('pais', 'Brasil') == 'Brasil':
        url = urlBase + f"country=Brasil&state={quote(k.get('uf',''))}&city={quote(k.get('municipio',''))}&street={quote(logradouro)}"
    else:
        pais = k.get('pais','Brasil')
        pais = pais.replace('(',',').split(',')[0] #quebra o nome do país com por vírgula
        municipio = k.get('municipio','')
        if municipio in pais: #pais estrangeiro, as vezes aparecendo repetido
            municipio = ''
        url = urlBase + f"country={quote(pais)}" + (f"&city={quote(municipio)}" if municipio else '')
        # if logradouro.strip().removesuffix(' S/N').removesuffix(' 0').strip(): #não melhora muito.
        #    url +=  '&street=' + quote(logradouro.strip().removesuffix(' S/N').removesuffix(' 0').strip())
        print(url)
    r = requests.get(url+'&format=json')
    dadosgeo = None
    try:
        if len(r.json())==0: #se não encontrou nada, r.json()==[]
            return None
        dadosgeo = r.json()[0] #pega o primeiro da lista
        #print(dadosgeo)
        time.sleep(1)
    except Exception as err:
        print('exception em geocode: ', err)
        dadosgeo = None
    return dadosgeo
#.def geocode

if __name__ == '__main__':
    if False:
        #https://www.python-graph-gallery.com/312-add-markers-on-folium-map
        
        # Make an empty map
        m = folium.Map(location=[20,0], tiles="OpenStreetMap", zoom_start=2)
        
        # Show the map
        # Import the pandas library
        
        # Make a data frame with dots to show on the map
        data = pd.DataFrame({
           'lon':[-58, 2, 145, 30.32, -4.03, -73.57, 36.82, -38.5],
           'lat':[-34, 49, -38, 59.93, 5.33, 45.52, -1.29, -12.97],
           'name':['Buenos Aires', 'Paris', 'melbourne', 'St Petersbourg', 'Abidjan', 'Montreal', 'Nairobi', 'Salvador'],
           'value':[10, 12, 40, 70, 23, 43, 100, 43]
        }, dtype=str)
        
        # add marker one by one on the map
        for i in range(0,len(data)):
           folium.Marker(
              location=[data.iloc[i]['lat'], data.iloc[i]['lon']],
              popup=data.iloc[i]['name'],
              #icon=folium.DivIcon(html=f"""<div style="font-family: courier new; color: blue">TEXTO</div>""") #para exibir texto 
           ).add_to(m)
        
        m.save('folium.html')
    if True:
        #gera mapa_exemplo.html com 3 endereços
        dados = [
            {'id':'PJ_11111', 'descricao':'Nome Empresa X', 'pais':'Brasil', 'uf':'GO', 'municipio':'Goiania',
                     'logradouro':'Rua castro alves 10', 'logradouro_complemento':'apto 10'} ,
            {'id':'PJ_2222', 'descricao':'Nome Empresa Y', 'uf':'SP', 'municipio':'Sao Paulo',
                     'logradouro':'Avenida Paulista 1000', 'logradouro_complemento':'apto 10'} ,                     
            {'id':'PJ_33333', 'descricao':'Nome Empresa U', 'pais':'França', 'municipio':'Paris'} ,
        ]
        rstream = geraMapa(dados, qteMaximaGeocoding=10)
        with open("mapa_exemplo.html", "wb") as f:
            f.write(rstream.read())