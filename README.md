# REDE-CNPJ - Visualização de dados públicos de CNPJ

Ferramenta para observar o relacionamento entre empresas e sócios, a partir dos dados públicos disponibilizados pela Receita Federal.<br>

### Vídeo no youtube<br>
[![youtube](http://img.youtube.com/vi/nxz9Drhqn_I/0.jpg)](https://youtu.be/nxz9Drhqn_I)

<br>Outros vídeos de utilização:<br>
Opção básicas dos botões: https://youtu.be/-Ug6ToTRnE4 <br>
Criar uma ligação no gráfico: https://youtu.be/8I0oNb4U9Rw <br>
Aumentar tamanho da ligação: https://youtu.be/7hy74LE8e7A <br>
Exportar dados como json: https://youtu.be/WKn02G9yHbQ <br>
Arrastar células do Excel: https://youtu.be/Oxze-d4V7kE <br>
A rotina possibilita visualizar de forma gráfica os relacionamentos entre empresas e sócios, a partir da base de dados públicos de cnpj da Receita Federal. <br>
Foi testada nos navegadores Firefox, Edge e Chrome. NÃO FUNCIONA no Internet Explorer. <br>

## Versão online com base completa de dados públicos de CNPJ:
https://www.redecnpj.com.br<br>
Leia as informações iniciais. Para fazer uma consulta, digite um CNPJ, o radical de CNPJ, a Razão Social Completa, o Nome Completo de Sócio ou CPF do Sócio (dá resultado impreciso). Para exibir um CNPJ aleatório, digite "TESTE". Pode-se inserir vários CNPJs de uma só vez, separando-os por (;). Para fazer busca por parte da Razão Social ou parte do nome de Sócio, utilize o asterisco (*) na parte que faltar do nome.
Funciona parcialmente em celular, com menu errático.

## Versão em python:
É preciso ter instalado no computador, um interpretador de linguagem python (versão 3.7 ou posterior) como a distribuída pelo Anaconda ou WinPython.<br> 
Para iniciar esse script, em um console DOS digite<br>
python rede.py<br>
A rotina abrirá o endereço http://127.0.0.1:5000/rede/ no navegador padrão.
Se der algum erro como “module <nome do módulo> not found”, instale o módulo pelo comando pip install <nome do módulo>.<br>
As opções por linha de comando são exibidas fazendo python rede.py -h<br>
A pasta contém um arquivo <b>cnpj_teste.db</b>, que é o banco de dados com poucos itens para testar o funcionamento da rotina.<br> 

## Como utilizar o Banco de dados públicos completo de CNPJs:
O projeto https://github.com/rictom/cnpj-sqlite contém o código para a conversão dos arquivos zipados do site da Receita para o formato SQLITE, gerando o arquivo <b>cnpj.db</b> com a base completa. 
O link para a base completa em sqlite já tratada está disponível em https://github.com/rictom/cnpj-sqlite#arquivo_sqlite.<br>
O código foi ajustado para o formato disponibilizado pela Receita Federal em 2021 e 2022.<br> 
Para utilizar a base completa <b>cnpj.db</b> na <b>REDE-CNPJ</b>, altere o arquivo de configuração rede.ini, mudando a linha de configuração para<br>
<b>base_receita = cnpj.db</b><br>

## Opções:

A roda do mouse expande ou diminui o tamanho da exibição.<br>
Fazendo click duplo em um ícone, a rotina expande as ligações. Por exemplo, clique duplo no ícone de uma pessoa, exibirá todas as empresas que esta é sócia. Clique duplo em um ícone de CNPJ, exibirá todos os sócios da empresa.<br>
Apertando SHIFT, é possível selecionar mais de um ícone. <br>
Pressionando CTRL e arrastando na tela, adiciona a seleção os itens da área.
Clicar no botão do meio do mouse (roda) faz aparecer janela para editar uma Nota, que aparece numa terceira linha abaixo do ícone.

Outras opções da rede estão no menu contextual do mouse (botão direito), sendo configuradas teclas de atalho correspondentes aos comandos:
 

## Tecla – Descrição do comando.
- TECLAS de 1 a 9 - Inserir camadas correspondente ao número sobre o nó selecionado;
- I - Inserir CNPJ, Razão Social completa ou nome completo de sócio. Poderão ser colocados vários CNPJs ao mesmo tempo, separados por ponto e vírgula (;).
- U - Criar item novo (que não seja PF ou PJ) e ligar aos itens selecionados;
- E - Editar dados do item (que não seja PF ou PJ) selecionado;
- CRTL+Z – Desfaz Inserção;

- SubMenu Ligar:
- U - Ligar para novo item;
- L - Ligar itens selecionados, ligação tipo estrela (o primeiro ligado aos demais);
- SHIFT+L - Remover ligação entre itens selecionados;
- Remover Ligacoes - Remove todas as ligações dos itens selecionados;
- K - Ligar itens selecionados, ligação tipo fila (o primeiro ligado ao segundo, o segundo ao terceiro, etc);

- SubMenu Visualização:
- A - Gráfico em Nova Aba - Abre aba com os itens selecionados;
- Q - Quebrar o gráfico em abas - Divide o gráfico em partes menores, mantendo as ligações
- P - Fixar o nó na posição;
- SHIFT+P - Desfixar todos os nós do gráfico;
- CTRL+P - Fixa um nó em cada grupo conexo (para evitar que o gráfico se expanda indefinidamente);
- SubMenu Visualização>Rótulos:
- E - Editar rótulo; 
- N - Rótulo - Exibe apenas o primeiro nome;
- SHIFT+N - Oculta/exibe texto da ligação;


- Alterar Ícone;
- C - Colorir os nós selecionados;
- Escolher Cor;
- D – Abre um popup com dados;
- SHIFT+D – Abre numa nova aba com Dados;
- CTRL+D – Lista ids dos itens selecionados;
- Altera o nome da aba;
- Escala Inicial - Coloca a exibição sem zoom, na escala inicial.
- Barra de Espaço - Parar/reiniciar leiaute (se a tela tiver muitos nós, os comandos funcionam melhor se o leiaute estiver parado);

- F - Localizar - Localizar na Tela Nome, CNPJ ou CPF;
- SHIFT+F - Localizar apenas na seleção;
- CTRL+F - Localiza por campo (como cor do item);
- J – Seleciona itens adjacentes;
- SHIFT+J – Seleciona árvores dos itens selecionados;
- CTRL+J - Itens com mais ligações - Opção para selecionar os itens do gráfico com mais ligações;
- Itens ligados a coloridos;
- Grupos com duas cores;
- CTRL+A - Seleciona todos os itens;
- CTRL+SHIFT+A - Inverte seleção;
- SubMenu - Busca em sites:
- G – Abre o nó numa aba do site Google;
- SHIFT+G – Abre o endereço no Google Maps (só CNPJs);
- Jusbrasil - Busca no site Jusbrasil
- Portal da Transparência - Busca no Portal da Transparência;

- SubMenu Salvar/Abrir:

- Salvar dados em Excel;
- Salvar imagem em formato SVG;
- Salvar Arquivo Json - salva dados do gráfico no formato json;
- Abrir Arquivo Json;
- Exportar/Importar JSON ao Servidor - Exportar ou importar dados do gráfico em formato JSON carregados no servidor;
- Banco de Dados - Exporta dados para banco de dados sqlite (só para usuário local);

- SubMenu Excluir

- DEL – Excluir itens selecionados.
- SHIFT+DEL – Excluir todos os itens.
- Excluir Nó mantendo Link;
- Simplifica Gráfico - Remove itens nas bordas do gráfico que não tenham destaque;
- Excluir itens isolados - Remove itens sem ligação.

Os comandos valem para o último nó selecionado ou nós selecionados, que ficam em destaque com a animação no contorno ods ícones.
Pressionando SHIFT e click, é possível selecionar mais de um ícone para fazer Exclusão ou para Expansão de vínculos.
Pode-se arrastar células com listas de CNPJs do Excel para a janela, ou arrastar arquivos csv ou json.

## Fonte dos dados:

Base de CNPJ está disponível em https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj ou http://200.152.38.155/CNPJ/ em formato csv.<br>
Arquivo CNPJ.db sqlite já tratado pelo projeto https://github.com/rictom/cnpj-sqlite está disponível em 
https://github.com/rictom/cnpj-sqlite#arquivo_sqlite. Eu tento atualizar esse arquivo mensalmente.<br>
Baixe o arquivo CNPJ.7z e descompacte usando o 7zip (https://www.7-zip.org/download.html). O arquivo descompactado tem cerca de 25GB.<br>

## Outras referências:
Biblioteca em javascript para visualização:<br>
https://github.com/anvaka/VivaGraphJS<br>

Menu Contextual:<br>
https://www.cssscript.com/beautiful-multi-level-context-menu-with-pure-javascript-and-css3/

## Histórico de versões

versão 0.8.5 (julho/2022)
- visualização melhorada em celular Android;
- alteração do script rede_relacionamentos.py para rede_sqlite_cnpj.py.

versão 0.8.4 (junho/2022)
- aceita CNPJs ou CPFs com zeros à esquerda faltando.

versão 0.8.3 (maio/2022)
- inclusão de relacionamento de representante de sócio;
- campo de busca de cpf/cnpj na linha do menu;
- rotina cnpj_search.py para indexar coluna de razão social ou nome de sócio para busca por parte do nome.

versão 0.8.2 (janeiro/2022)
- opção no menu Salvar/Abrir>Baixar base CNPJ para abrir a página com o arquivo em SQLITE;
- tecla A - se houver só um item selecionado, abre um gráfico em nova aba no link /rede/grafico/NUMERO_CAMADA/PJ_X, cujo link poderá ser compartilhado;
- usando somente POST em algumas consultas;
- alerta de que consulta por CPF pode apresentar erros. A base da Receita só contem seis dígitos do CPF de sócios, por isso a busca exibe todos os CPFs que tem os mesmo dígitos;
- flask-limiter para diminuir excesso de consultas. Se precisar dos dados por api, rode o projeto localmente e altere os parâmetros limiter_padrao e limiter_dados do arquivo de configuração rede.ini.


versão 0.7.4 (dezembro/2021)
- O projeto online pode ser acessado em https://www.redecnpj.com.br;
- usando certificado digital na versão online;

versão 0.7.3 (setembro/2021)
- correção de erro em ligação em banco de dados local.

versão 0.7.2 (setembro/2021)
- opção para alterar o nome da aba;
- opção para selecionar todos os itens;
- opção para inverter seleção;

versão 0.7.1 (agosto/2021)
- opção para dividir gráficos em outras abas (tecla Q);
- correção de erro quando se apertava tecla CTRL;
- opção para selecionar itens adjacentes aos selecionados (tecla J);
- opção para selecionar árvore que contém o item (tecla SHIFT+J);
- opção para listar itens com mais links (tecla CTRL+J)
- opção para selecionar itens com mais ligações para ícones coloridos;
- opção "Nova Aba" (tecla A) abre nova aba com mais de um item selecionado;
- troca de nomes no menu de exportar json para salvar json;
- opção para exportar dados para banco de dados local (só funciona na máquina local);
- opção para exportar para json apenas itens selecionados;
- opção para ocultar rótulos de ligações (SHIFT+N);
- opção para remover todas as ligações dos itens selecionados;
- opção simplifica gráfico (remove itens que não são coloridos ou com comentário que tem apenas uma ligação).

versão 0.6.3 (julho/2021)
- melhoria para dar clique duplo em ícones;
- correção de erro de ligação para empresa no exterior sem cnpj;
- somente o ícone pode ser clicado;
- mensagem de alerta para utilizar caractere curinga;
- mudança nas tabelas temporárias;
- todas as tabelas de códigos (cnae, natureza jurídica, etc) foram incorporados ao arquivo sqlite;
- OBSERVAÇÃO. A versão 0.6.3 só vai funcionar com a versão mais atualizada do arquivo cnpj.db referência 16/7/2021.

versão 0.5.1 (junho/2021)
- atualização da tabela sqlite cnpj.db com dados públicos de 18/06/2021.

versão 0.5 (abril/2021)
- alteração do código para layout novo das tabelas;
- busca por Radical de CNPJ ou CPF de sócio (busca somente pelo miolo do CPF);

versão 0.4 (janeiro/2021)
- usando lock para evitar erro de consulta em requisições simultâneas;
- opção para fazer busca do termo no Portal da Transparência da CGU;
- correção de link para google search.

versão 0.3.4 (janeiro/2021)
- Possibilita ver o texto do lado direito do ícone;
- diagramas de tabela hierárquica;
- ver diagramas de arquivo com código em python;
- mais opções por linha de comando.

versão 0.3 (janeiro/2021)
- Opção para inserção de novos itens para elaboração de mapas mentais;
- Opções para inserir itens novos como link para sites e arquivos locais.
- Opção para arrastar células do excel, leitura de arquivo csv;
- Opções de leitura de entrada por linha de comando;
- Itens selecionados ficam em destaque com linha animada;
- Alteração no formato do arquivo de configuração rede.ini.

versão 0.2 (dezembro/2020)
- Suporte para busca por parte do nome na base de empresas;
- Exportação/importação de gráfico no formato json para o servidor.

versão 0.1 (setembro/2020)
- Primeira versão
