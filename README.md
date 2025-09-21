# RedeCNPJ - Visualização de dados públicos de CNPJ

Ferramenta para observar de forma gráfica os relacionamentos entre empresas e sócios, a partir dos dados públicos disponibilizados pela [Receita Federal](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).<br>

### Vídeo no youtube<br>
[![youtube](http://img.youtube.com/vi/nxz9Drhqn_I/0.jpg)](https://youtu.be/nxz9Drhqn_I)<br>

Foi testada nos navegadores Firefox, Edge, Chrome e Safari. <br>

## RedeCNPJ.com Online:
A versão online com dados públicos funciona agora no site https://www.redecnpj.com. A versão online com acesso gratuito foi descontinuada em maio/2023.

## RedeCNPJ Demo Online:
Uma versão demo, com base de dados incompleta está rodando em https://www.redecnpj.com.br/demo/. Nesta versão apenas o primeiro nome de sócio é exibido e algumas opções estão desabilitadas. 

## Versões Aplicativo:
Para os usuários que não tem familiaridade com o Python, em [https://www.redecnpj.com.br/rede/aplicativo.html](https://www.redecnpj.com.br/rede/pag/aplicativo.html) é possível obter gratuitamente a versão "aplicativo" da RedeCNPJ e outro programa para Windows que baixa os arquivos de dados abertos do site da Receita Federal e converte para o formato para uso na RedeCNPJ. Os aplicativos foram gerados a partir do código fonte deste repositório. O procedimento de geração das tabelas levam várias horas e deve ser repetido mensalmente. Utilizar a versão online https://www.redecnpj.com evitará esse transtorno.<br>

## Versão em python (execução local):
É preciso ter instalado no computador:
- python <b>versão 3.9</b> a <b>3.12</b>.<br> 
- 100GB de espaço livre, para utilizar a base de CNPJs em sqlite.

Faça um clone do código deste projeto.<br>
O recomendado é trabalhar em um ambiente virtual. Siga orientações como em https://docs.python.org/3/library/venv.html<br>
Use o comando<br>
<b>pip install -r requirements.txt</b><br>
para instalar as bibliotecas utilizadas.<br><br>
Para iniciar o script, em um console digite<br>
<b>python rede.py</b><br>
A rotina abrirá o endereço http://127.0.0.1:5000/rede/ no navegador padrão.
As opções por linha de comando são exibidas fazendo python rede.py -h<br>
A pasta contém os arquivos <b>rede_teste.db</b> e <b>cnpj_teste.db</b>, que são amostras dos bancos de dados para testar o funcionamento da rotina.<br> 

## Como utilizar o Banco de dados completo de CNPJs com a versão em Python:

A pasta <b>rede_cria_tabelas</b> contém os scripts para baixar os arquivos zip do site de Dados Abertos, gerar a base completa de empresas e as tabelas utilizadas na redeCNPJ.<br>
a) para baixar os arquivos zip do site de Dados Abertos, rode o comando:<br>
<b>python dados_cnpj_baixa.py</b><br>
b) para criar a base de empresas <b>cnpj.db</b>, rode o comando:<br>
<b>python dados_cnpj_para_sqlite.py</b><br>
c) para criar a tabela de vínculos <b>rede.db</b> utilizada na redeCNPJ, rode o comando:<br>
<b>python rede_cria_tabela_rede.db.py</b><br>
d) para criar a tabela de vínculos <b>cnpj_links_ete.db</b> de endereços, de emails e de telefones utilizada na redeCNPJ, rode o comando:<br>
<b>python rede_cria_tabela_cnpj_links_ete.py</b><br>
<br>
Ao final, mova os arquivos cnpj.db, rede.db, rede_search.db, cnpj_links_ete.db da pasta <b>rede_cria_tabelas/dados-publicos</b> para a <b>rede/bases</b>. Os arquivos restantes nas pastas dados-publicos e dados-publicos-zip poderão ser apagados.<br>
<br>
Observação: O cnpj_links_ete.db é opcional, somente se quiser visualizar vinculos por endereços, telefones ou email em comum.<br>
Veja o projeto https://github.com/rictom/cnpj-sqlite para ver informações sobre o código para baixar os arquivos com dados abertos e converter para sqlite.<br>

O código foi ajustado para o formato disponibilizado pela Receita Federal a partir de 2021, testado em agosto de 2024.<br> 
Orientações detalhadas para instalação em Windows no link https://github.com/rictom/rede-cnpj/blob/master/doc/instalacao_windows.md<br>

### Diagrama da RedeCNPJ na RedeCNPJ<br>
![image](https://user-images.githubusercontent.com/71139693/235322145-30fe1956-76f7-438c-825a-9552b5a7004f.png)<br>
[Clique para abrir o diagrama](https://www.redecnpj.com.br/demo/grafico_no_servidor/rede_cnpj_diagrama.json)

## Opções:<a id="ajuda"></a>

A roda do mouse expande ou diminui o tamanho da exibição.<br>
Fazendo click duplo em um ícone, a rotina expande as ligações. Por exemplo, clique duplo no ícone de uma pessoa, exibirá todas as empresas que esta é sócia. Clique duplo em um ícone de CNPJ, exibirá todos os sócios da empresa.<br>
Apertando SHIFT, é possível selecionar mais de um ícone. <br>
Pressionando CTRL e arrastando na tela, adiciona a seleção os itens da área.
Clicar no botão do meio do mouse (roda) faz aparecer janela para editar uma Nota, que aparece numa terceira linha abaixo do ícone.

Outras opções da rede estão no menu contextual do mouse (botão direito), sendo configuradas teclas de atalho correspondentes aos comandos:
 
## Tecla – Descrição do comando.
- I - Inserir CNPJ, Razão Social completa ou nome completo de sócio. Poderão ser colocados vários CNPJs ao mesmo tempo, separados por ponto e vírgula (;).
- TECLAS de 1 a 9 - Inserir camadas correspondente ao número sobre o nó selecionado;
- SHIFT + TECLAS de 1 a 9 - Inserir camadas correspondente a ligações com valores, como dados do TSE de doadores/pagamentos das eleições 2022, recebimento por favorecido pelo Governo Federal com dados do Portal da Transparência da CGU ou pagamentos de municípios paulistas com dados do TCE/SP;
- U - Criar item novo (que não seja PF ou PJ) e ligar aos itens selecionados;
- E - Editar dados do item (que não seja PF ou PJ) selecionado;
- CRTL+Z – Desfaz Inserção;

- <b>SubMenu Ligar:</b>
- U - Ligar para novo item;
- L - Ligar itens selecionados, ligação tipo estrela (o primeiro ligado aos demais);
- SHIFT+L - Remover ligação entre itens selecionados;
- Remover Ligacoes - Remove todas as ligações dos itens selecionados;
- K - Ligar itens selecionados, ligação tipo fila (o primeiro ligado ao segundo, o segundo ao terceiro, etc);

- <b>SubMenu Visualização:</b>
- A - Gráfico em Nova Aba - Abre aba com os itens selecionados;
- Q - Quebrar o gráfico em abas - Divide o gráfico em partes menores, mantendo as ligações
- Abre OpenStreetMap - Abre Mapa com os endereços de empresas;
- P - Fixar o nó na posição;
- SHIFT+P - Desfixar todos os nós do gráfico;
- CTRL+P - Fixa um nó em cada grupo conexo (para evitar que o gráfico se expanda indefinidamente);
- <b>SubMenu Visualização>Rótulos:</b>
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
- <b>SubMenu - Busca em sites:</b>
- B - Abre abas dos sites deste submenu;
- Jusbrasil - Busca no site Jusbrasil;
- Escavador - Busco no site Escavador;
- Portal da Transparência - Busca no Portal da Transparência da CGU;
- Bing - Busca no Bing;
- DuckDuckGo - Busca no DuckDuckGo;
- Reclame Aqui - Busca no Reclame Aqui;
- G – Abre o nó numa aba do site Google;
- SHIFT+G – Abre o endereço no Google Maps (só CNPJs);
- H ou CTRL+G - Raspa a página do Google e coloca links;
- SHIFT+H - Raspa a página do Google e busca palavras chave nos links;

- <b>SubMenu Salvar/Abrir:</b>

- Salvar dados em Excel;
- Salvar em formato i2 Chart Reader;
- Salvar imagem em formato SVG;
- Salvar Arquivo Json - salva dados do gráfico no formato json;
- Abrir Arquivo Json;
- Exportar/Importar JSON ao Servidor - Exportar ou importar dados do gráfico em formato JSON carregados no servidor;
- Banco de Dados - Exporta dados para banco de dados sqlite (só para usuário local);

- <b>SubMenu Excluir</b>

- DEL – Excluir itens selecionados.
- SHIFT+DEL – Excluir todos os itens.
- Excluir Nó mantendo Link;
- Simplifica Gráfico - Remove itens nas bordas do gráfico que não tenham destaque;
- Excluir itens isolados - Remove itens sem ligação.

Os comandos valem para o último nó selecionado ou nós selecionados, que ficam em destaque com a animação no contorno ods ícones.
Pressionando SHIFT e click, é possível selecionar mais de um ícone para fazer Exclusão ou para Expansão de vínculos.
Pode-se arrastar células com listas de CNPJs do Excel para a janela, ou arrastar arquivos csv ou json.

## Fonte dos dados:

#### Base de CNPJ
Está disponível em https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj ou https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/ (link a partir de 28/10/2024.)<br>

## Como gerar listas de empresas a partir de parâmetros:
A partir da base em sqlite cnpj.db, o projeto https://github.com/rictom/cnpj_consulta gera listas a partir de parâmetros como UF, Município ou CNAE e exporta os dados de empresas para Excel. 

## Bibliotecas e outros projetos utilizados:
Biblioteca em javascript para visualização:<br>
https://github.com/anvaka/VivaGraphJS<br>

Menu Contextual:<br>
https://www.cssscript.com/beautiful-multi-level-context-menu-with-pure-javascript-and-css3/

Ícones:<br>
https://www.flaticon.com<br>
https://fontawesome.com<br>

## Histórico de versões
<b>versão 1.5 (abril/2025)</b>
- Correção para utilizar tecla Command ao invés de CTRL no MacOS.<br>

<b>versão 1.2 (agosto/2024)</b>
- Scripts para gerar as tabelas com dados completos em rede_criar_tabelas<br>

<b>versão 1.1 (fevereiro/2024)</b>
- Removido a dependência de sqlalchemy<=2.0, evitando erro se a biblioteca sqlalchemy for atualizada;<br>
- Opção para inserir foto (Tecla E):<br>
![image](https://github.com/rictom/rede-cnpj/assets/71139693/3fdf5def-36ea-4cfa-bbc6-8e19123c07ad)



<b>versão 1.0 (junho/2023)</b>
- Agora é necessário usar os arquivos rede.db e rede_search.db (ver guia de instalação Windows);<br>
- Arquivos sqlite .db na pasta bases;<br>
- Arquivos db de teste tem nomes embaralhados;<br>
- Compatível com versão aplicativo disponível no site redecnpj.com.br;<br>
- Botões Gravar no Navegador e Adicionar: Possibilita guardar o gráfico e recuperar os dados em outra sessão;<br>
- Opções Copiar (CTRL+C) e Colar (CTRL+V) itens entre abas da RedeCNPJ.<br>
- Botão Copy/Drag para copiar itens selecionados para uma nova aba da RedeCNPJ;<br>
- Opção Menu> Filtrar/Localizar> Busca em sites> Abre Busca em Abas (Tecla B) - Abre abas com busca no Google, Bing, DuckDuckGo, etc.<br>
- Arrastar link traz o favicon do site correspondente.<br>
- Opção para buscar caminhos;<br>
- Opção para apagar arquivo json no servidor.<br>

<b>versão 0.9 (dezembro/2022)</b>
- Grande melhoria de desempenho (10X mais rápido!!!);<br>
- Utilizando tabelas temporárias em memória do sqlite3;<br>
- Inclusão de flag quando houver dados adicionais no cpf/cnpj;<br>
- SHIFT+D exibe sócios de empresas;<br>
- Exibição do CNAE Secundário;<br>
- Aba com dados de empresas no Excel em lista;<br>
- Nome Fantasia no tooltip;<br>
- Busca por nome mais flexível;<br>
- Alteração no sentido das setas;<br>
- Busca de empresa por Nome Fantasia;<br>
- Troca de fonte.<br>
- <b>Observação: </b>A partir desta versão, é necessário criar um arquivo <b>rede.db</b> para a rotina funcionar.<br>


<b>versão 0.8.8 (setembro/2022)</b>
- busca por Nome Fantasia de cnpj;<br>
- alteração no espaçamento dos botões para Android;<br>
- popup de mensagem inicial vai aparecer menos;<br>
- melhoria no visual do popup nos mapas;<br>
- remoção de caixa de entrada para perguntar a camada do item a ser inserido. Aperte uma tecla de 2 a 9 para inserir mais camadas;<br>
- clicar no botão camada, duplo-clique ou pressionar tecla 1 de forma repetida irá trazer mais camadas.

<b>versão 0.8.7 (setembro/2022)</b>
- abre mapa com endereços das empresas no OpenStreetMap, pelo botão ![image](https://user-images.githubusercontent.com/71139693/189489292-d00f9e2f-f9e7-497d-a97a-c6fde790ad1d.png) no canto superior direito da tela: <br>
 ![image](https://user-images.githubusercontent.com/71139693/192645610-cf763429-af6c-4a82-9e2a-691130c07c40.png)
- o parâmetro geocode_max em rede.ini define a quantidade de endereços que serão buscados pela api do OpenStreetMap. Para evitar bloqueio, é feito uma pausa de um segundo entre cada chamada da api. Se a quantidade de endereços for maior que o parâmetro geocode_max, os endereços dos cnpjs serão substituidos pelas coordenadas dos municipios de acordo com tabela do IBGE.<br>
- em cada popup de endereço, há quatro opções: a) abrir o endereço no Google Maps; b) abrir uma rede de relacionamentos do cnpj; c) exibir dados do cnpj; d) selecionar o item na rede original.

<b>versão 0.8.6 (agosto/2022)</b>
- exportação para gráfico no formato (ex-IBM) i2 Chart Reader ou Analyst's Notebook (.anx), pela opção no menu Salvar/Abrir>Salvar i2...:<br>![image](https://user-images.githubusercontent.com/71139693/185807202-2e0a7243-d952-470f-bee0-5ef4d45ed38e.png)<br>
- O i2 Chart Reader pode ser baixado gratuitamente no site https://i2group.com/i2-chart-reader?hsLang=en<br>

- opção H para abrir árvore com links de busca na página do Google:![image](https://user-images.githubusercontent.com/71139693/185807100-2627141d-d67b-4325-92fa-0014cedaac7c.png)

- opção SHIFT+H para abrir árvore com palavras chaves dos links da página do Google. Exemplo: 
![chaves](https://user-images.githubusercontent.com/71139693/189489251-8aba03e6-8cb0-4c68-9a6e-e38655d6b748.png)


- para usar a opção SHIFT+H, deve-se instalar as bibliotecas pelo comando pip install -r requirements_chaves.txt<br> e depois instalar os módulos do spacy pelos comandos python -m spacy download en_core_web_sm e python -m spacy download pt_core_news_lg. No arquivo rede.ini, modifique o parâmetro para busca_chaves = 1
- Como a opção de chaves é lenta, ela não foi habilitada para rodar no servidor. Utilize com a versão local do projeto.

<b>versão 0.8.5 (julho/2022)</b>
- visualização melhorada em celular Android;
- alteração do script rede_relacionamentos.py para rede_sqlite_cnpj.py;
- alteração do parâmetro gravidade, para diminuir a dispersão de grupos não conexos;
- tutorial de instalação passo a passo.

<b>versão 0.8.4 (junho/2022)</b>
- aceita CNPJs ou CPFs com zeros à esquerda faltando.

<b>versão 0.8.3 (maio/2022)</b>
- inclusão de relacionamento de representante de sócio;
- campo de busca de cpf/cnpj na linha do menu;
- rotina cnpj_search.py para indexar coluna de razão social ou nome de sócio para busca por parte do nome.

<b>versão 0.8.2 (janeiro/2022)</b>
- opção no menu Salvar/Abrir>Baixar base CNPJ para abrir a página com o arquivo em SQLITE;
- tecla A - se houver só um item selecionado, abre um gráfico em nova aba no link /rede/grafico/NUMERO_CAMADA/PJ_X, cujo link poderá ser compartilhado;
- usando somente POST em algumas consultas;
- alerta de que consulta por CPF pode apresentar erros. A base da Receita só contem seis dígitos do CPF de sócios, por isso a busca exibe todos os CPFs que tem os mesmo dígitos;
- flask-limiter para diminuir excesso de consultas. Se precisar dos dados por api, rode o projeto localmente e altere os parâmetros limiter_padrao e limiter_dados do arquivo de configuração rede.ini.


<b>versão 0.7.4 (dezembro/2021)</b>
- O projeto online pode ser acessado em https://www.redecnpj.com.br;
- usando certificado digital na versão online;

<b>versão 0.7.3 (setembro/2021)</b>
- correção de erro em ligação em banco de dados local.

<b>versão 0.7.2 (setembro/2021)</b>
- opção para alterar o nome da aba;
- opção para selecionar todos os itens;
- opção para inverter seleção;

<b>versão 0.7.1 (agosto/2021)</b>
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

<b>versão 0.6.3 (julho/2021)</b>
- melhoria para dar clique duplo em ícones;
- correção de erro de ligação para empresa no exterior sem cnpj;
- somente o ícone pode ser clicado;
- mensagem de alerta para utilizar caractere curinga;
- mudança nas tabelas temporárias;
- todas as tabelas de códigos (cnae, natureza jurídica, etc) foram incorporados ao arquivo sqlite;
- OBSERVAÇÃO. A versão 0.6.3 só vai funcionar com a versão mais atualizada do arquivo cnpj.db referência 16/7/2021.

<b>versão 0.5.1 (junho/2021)</b>
- atualização da tabela sqlite cnpj.db com dados públicos de 18/06/2021.

<b>versão 0.5 (abril/2021)</b>
- alteração do código para layout novo das tabelas;
- busca por Radical de CNPJ ou CPF de sócio (busca somente pelo miolo do CPF);

<b>versão 0.4 (janeiro/2021)</b>
- usando lock para evitar erro de consulta em requisições simultâneas;
- opção para fazer busca do termo no Portal da Transparência da CGU;
- correção de link para google search.

<b>versão 0.3.4 (janeiro/2021)</b>
- Possibilita ver o texto do lado direito do ícone;
- diagramas de tabela hierárquica;
- ver diagramas de arquivo com código em python;
- mais opções por linha de comando.

<b>versão 0.3 (janeiro/2021)</b>
- Opção para inserção de novos itens para elaboração de mapas mentais;
- Opções para inserir itens novos como link para sites e arquivos locais.
- Opção para arrastar células do excel, leitura de arquivo csv;
- Opções de leitura de entrada por linha de comando;
- Itens selecionados ficam em destaque com linha animada;
- Alteração no formato do arquivo de configuração rede.ini.

<b>versão 0.2 (dezembro/2020)</b>
- Suporte para busca por parte do nome na base de empresas;
- Exportação/importação de gráfico no formato json para o servidor.

<b>versão 0.1 (setembro/2020)</b>
- Primeira versão
