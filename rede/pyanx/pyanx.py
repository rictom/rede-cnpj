#!/usr/bin/env python
"""
This work is made available under the Apache License, Version 2.0.

You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
"""

import random, io

#from . import anx
import anx
import datetime

__author__ = 'Petter Chr. Bjelland (petter.bjelland@gmail.com)'


class Pyanx(object):
  def __init__(self, ring_margin=5):
    self.entity_types = {}
    self.edges = []
    self.nodes = {}
    self.timezones = {}
    self.ring_margin = ring_margin

  def add_node(self, entity_type='Anon', label=None, ring_color=None, description='', datestr=None, datestr_description=None, dateformat='%Y-%m-%dT%H:%M:%S', timezone=None, posx=None, posy=None):
    current_id = label

    if timezone and timezone not in self.timezones:
      self.timezones[timezone] = len(self.timezones)

    if entity_type not in self.entity_types:
      self.entity_types[entity_type] = True

    if datestr:
      _datetime = datetime.datetime.strptime(datestr, dateformat)
    else:
      _datetime = None

    self.nodes[current_id] = {
      'entity_type': entity_type,
      'label': label,
      'ring_color': ring_color,
      'description': description,
      'datetime': _datetime,
      'datetime_description': datestr_description,
      'timezone': timezone,
      'posx':posx,
      'posy':posy
    }

    return current_id

  def add_edge(self, source, sink, label='', color=0, style='Solid', description='', datestr=None, datestr_description=None, dateformat='%Y-%m-%dT%H:%M:%S', timezone=None):
    if datestr:
        _datetime = datetime.datetime.strptime(datestr, dateformat)
    else:
      _datetime = None

    if timezone and timezone not in self.timezones:
      self.timezones[timezone] = len(self.timezones)

    self.edges.append((source, sink, {
      'label': label,
      'color': color,
      'style': style,
      'description': description,
      'datetime': _datetime,
      'datetime_description': datestr_description,
      'timezone': timezone
    }))

  def __add_entity_types(self, chart):
    entity_type_collection = anx.EntityTypeCollection()

    for entity_type in self.entity_types:
      entity_type_collection.add_EntityType(anx.EntityType(Name=entity_type, IconFile=entity_type))

    chart.add_EntityTypeCollection(entity_type_collection)

  def __add_link_types(self, chart):
    link_type_collection = anx.LinkTypeCollection()
    link_type_collection.add_LinkType(anx.LinkType(Name="Link"))
    chart.add_LinkTypeCollection(link_type_collection)

  def __set_date(self, chart_item, data):
    if not data['datetime']:
      return

    chart_item.set_DateTime(data['datetime'])

    if data['timezone']:
      chart_item.set_TimeZone(anx.TimeZone(data['timezone'], UniqueID=self.timezones[data['timezone']]))

    chart_item.set_DateTimeDescription(data['datetime_description'])
    chart_item.set_DateSet(True)
    chart_item.set_TimeSet(True)

  def __add_entities(self, chart):
    chart_item_collection = anx.ChartItemCollection()

    for data in list(self.nodes.values()):
      circle = None

      if data['ring_color']:
          circle = anx.FrameStyle(Colour=data['ring_color'], Visible=1, Margin=self.ring_margin)

      #x, y = (random.randint(0, 1000), random.randint(0, 1000))
      x = data['posx'] if data['posx'] else random.randint(0, 1000)
      y = data['posy'] if data['posy'] else random.randint(0, 1000)
      icon = anx.Icon(IconStyle=anx.IconStyle(Type=data['entity_type'], FrameStyle=circle))
      entity = anx.Entity(Icon=icon, EntityId=data['label'], Identity=data['label'])
      chart_item = anx.ChartItem(XPosition=x, Label=data['label'], End=anx.End(X=x, Y=y, Entity=entity), Description=data['description'])

      self.__set_date(chart_item, data)

      chart_item_collection.add_ChartItem(chart_item)

    chart.add_ChartItemCollection(chart_item_collection)

  def __add_links(self, chart):
    chart_item_collection = anx.ChartItemCollection()

    for source, sink, data in self.edges:
      link_style = anx.LinkStyle(StrengthReference=data['style'], Type='Link', ArrowStyle='ArrowOnHead', LineColour=data['color'], MlStyle="MultiplicityMultiple")
      link = anx.Link(End1Id=source, End2Id=sink, LinkStyle=link_style)

      chart_item = anx.ChartItem(Label=data['label'], Link=link, Description=data['description'])

      self.__set_date(chart_item, data)

      chart_item_collection.add_ChartItem(chart_item)

    chart.add_ChartItemCollection(chart_item_collection)

  def create(self, path, pretty=True):
    chart = anx.Chart(IdReferenceLinking=False)
    chart.add_StrengthCollection(anx.StrengthCollection([
        anx.Strength(DotStyle="DotStyleDashed", Name="Dashed", Id="Dashed"),
        anx.Strength(DotStyle="DotStyleSolid", Name="Solid", Id="Solid")
    ]))

    self.__add_entity_types(chart)
    self.__add_link_types(chart)
    self.__add_entities(chart)
    self.__add_links(chart)

    with open(path, 'w') as output_file:
        chart.export(output_file, 0, pretty_print=pretty, namespacedef_=None)
    with open(path, 'r') as output_file:
        texto = output_file.read()
        texto = texto.replace('b\'"','"')
        texto = texto.replace('"\'','"')
    with open(path, 'w') as output_file:
        output_file.write(texto)

  def createStream(self, pretty=True):
    chart = anx.Chart(IdReferenceLinking=False)
    chart.add_StrengthCollection(anx.StrengthCollection([
        anx.Strength(DotStyle="DotStyleDashed", Name="Dashed", Id="Dashed"),
        anx.Strength(DotStyle="DotStyleSolid", Name="Solid", Id="Solid")
    ]))

    self.__add_entity_types(chart)
    self.__add_link_types(chart)
    self.__add_entities(chart)
    self.__add_links(chart)

    outputStream = io.StringIO()
    chart.export(outputStream, 0, pretty_print=pretty, namespacedef_=None)
    outputStream.seek(0)
    texto = outputStream.read()
    #GAMBIARRA ALERT!!!!!!!!!!!!!!!!
    #o código foi feito para python 2.7, por isso há diferenças na questão de string e bytes. A linha abaixo é um ajuste muito feio para a rotina funcionar.
    texto = texto.replace('b\'"','"')
    texto = texto.replace('"\'','"')
    outputStream = io.BytesIO(texto.encode('utf8'))
    return outputStream