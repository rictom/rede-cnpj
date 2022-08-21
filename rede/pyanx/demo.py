import pyanx

chart = pyanx.Pyanx()

tyrion = chart.add_node(entity_type='Person', label='Tyrion')
tywin = chart.add_node(entity_type='Person', label='Tywin')
jaime = chart.add_node(entity_type='Person', label='Jaime')
cersei = chart.add_node(entity_type='Woman', label='Cersei')

chart.add_edge(tywin, tyrion, 'Father of')
chart.add_edge(jaime, tyrion, 'Brother of')
chart.add_edge(cersei, tyrion, 'Sister of')

chart.create('demo.anx')
