# -*- coding: utf-8 -*-
"""
Created on Sat Aug 20 15:19:17 2022

@author: ricar

biblioteca para gerar pyanx, ver 
https://github.com/pcbje/pyanx
"""

import sys, json
sys.path.append('pyanx')
import pyanx
#from pyanx import pyanx

def removeAcentos(data):
  import unicodedata, string
  if data is None:
    return ''
  return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.printable)
#.def removeAcentos

def jsonParai2(djson):
    chart = pyanx.Pyanx()
    
    
    dicImagens = {'icone-grafo-desconhecido.png':'Person',
                  'icone-grafo-masculino.png':'Person',
                  'icone-grafo-feminino.png':'Woman',
                   'icone-grafo-empresa.png':'Office',
                   'icone-grafo-empresa-publica.png':'House',
                   'icone-grafo-empresa-estrangeira.png':'House',
                   'icone-grafo-empresa-fundacao.png':'Workshop',
                   'icone-grafo-empresa-individual.png':'Workshop',
                   'icone-grafo-telefone.png':'Phone',
                   'icone-grafo-endereco.png':'Place',
                   'icone-grafo-email.png':'Email',
                   'Conta':'Account',
                   'key.png':'Key', #verificar, ícone invalido
                   'link.png':'Link' #verificar, ícone invalido
                   #'':u'Cabinet',
                   #'':u'Office'
                   } 
    
    chart = pyanx.Pyanx()
    
    #tjson = open('rede.json', encoding='utf8').read()
    #djson = json.loads(tjson)
    noi2={}
    for campos in djson['no']:
        idc = removeAcentos(campos['id'])
        #noi2[idc] = chart.add_node(entity_type=dicTiposIngles.get(tipo,''), label=unicode(campos['id']) + u'-' +removeAcentos(descricao))
        if campos['descricao'] in idc:
            tlabel = idc[3:]
        else:
            tlabel = removeAcentos(idc[3:] + ( '-' + campos['descricao'] if campos['descricao'] else ''))
        noi2[idc] = chart.add_node(entity_type=dicImagens.get(campos['imagem'],'Document'), 
                                   label=tlabel,
                                   posx = campos['posicao']['x'], posy=campos['posicao']['y'])
    
    for campos in djson['ligacao']:
        id1 = removeAcentos(campos['origem'])
        id2 = removeAcentos(campos['destino'])
        tipo = campos['label']
        if tipo.startswith('end') or tipo.startswith('tel') or tipo.startswith('email') or tipo.startswith('chave'):
            tligacao = ''
        else:
            tligacao = removeAcentos(campos['label']+(':'+campos['tipoDescricao'] if campos['tipoDescricao'] else ''))
        chart.add_edge(noi2[id1], noi2[id2], tligacao)
    #chart.create('demo.anx')
    return chart.createStream()

if __name__ == '__main__':

    if False:
        chart = pyanx.Pyanx()
        
        tyrion = chart.add_node(entity_type='Person', label='Tyrion')
        tywin = chart.add_node(entity_type='Person', label='Tywin')
        jaime = chart.add_node(entity_type='Person', label='Jaime')
        cersei = chart.add_node(entity_type='Woman', label='Cersei')
        
        chart.add_edge(tywin, tyrion, 'Father of')
        chart.add_edge(jaime, tyrion, 'Brother of')
        chart.add_edge(cersei, tyrion, 'Sister of')
        
        chart.create('demo.anx')
    if True:
        djson = json.loads(open('rede.json', encoding='utf8').read())
        f = jsonParai2(djson)
        with open('rede.json.anx', 'wb') as arq:
            arq.write(f.read())