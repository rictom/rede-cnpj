# REDE-DADOS-CNPJ - Visualização de dados públicos de CNPJ

### Vídeo no youtube<br>
[![youtube](http://img.youtube.com/vi/nxz9Drhqn_I/0.jpg)](https://youtu.be/nxz9Drhqn_I)

<br>
A rotina possibilita visualizar de forma gráfica os relacionamentos entre empresas e sócios, a partir da base de dados públicos de cnpj da Receita Federal. <br>
Foi testada nos navegadores Firefox, Edge e Chrome. NÃO FUNCIONA no Internet Explorer. <br>
A base de dados é o arquivo CNPJ_full.db, banco de dados no formato sqlite. Para exemplificar o funcionamento da rotina, este repositório tem o arquivo com cerca de mil registros com dados fictícios de empresas e de sócios. <br>

## Versão online com base completa de dados públicos de CNPJ:
http://168.138.150.250/rede/ <br>
Leia as informações iniciais, e digite "TESTE", CNPJ, Razão Social ou Nome Completo de Sócio.
Funciona parcialmente em celular, com menu errático.

## Versão online com base de testes:
http://rtomi.pythonanywhere.com/rede/ <br>
Utilizando a base de testes. Ao abrir a janela, digite "Teste". (Não dá pra digitar um cnpj porque todos os dados são fictícios)

## Versão em python:
É preciso ter instalado no computador, um interpretador de linguagem python (versão 3.7 ou posterior) como a distribuída pelo Anaconda ou WinPython.<br> 
Para iniciar esse script, em um console DOS digite<br>
python rede.py<br>
A rotina abrirá o endereço http://127.0.0.1:5000/rede/ no navegador padrão.
Se der algum erro como “module <nome do módulo> not found”, instale o módulo pelo comando pip install <nome do módulo>.<br>

## Versão executável:
Para iniciar a versão executável, primeiro descompacte o arquivo [rede-versao-exe.7z](https://www.dropbox.com/s/dl9d0cwhj378rfd/rede-versao-exe.7z?dl=0). Para executar a rotina, clique duas vezes em rede.exe. Obs: a versão executável foi criada por pyinstaller para funcionar no windows. É possível que falte alguma dll para funcionar corretamente.<br>
A rotina abrirá o endereço http://127.0.0.1:5000/rede/ no navegador padrão e um console do DOS. Para parar a execução, feche o console.<br>

## Configurar nós iniciais:
Se não houver cpfcnpj inicial configurado em rede.ini, o navegador abrirá um popup pedindo para inserir um cnpj ou nome. Colocando TESTE (ou teste), será inserido um ícone com um cnpj aleatório do banco de dados.<br>
Se desejar definir itens iniciais da rede de relacionamentos, edite o arquivo rede.ini em um editor de texto e altere a linha que começa com “cpfcnpjinicial=”. Isso só vai ser útil se vc estiver utilizando a base CNPJ_full.db completa.<br>

## Como utilizar o Banco de dados públicos de cnpj:
A pasta contém um arquivo CNPJ_full.db, que é o banco de dados sqlite com dados para teste. Substitua esse arquivo pela base de CNPJ em sqlite que pode ser obtido pelo script disponível em https://github.com/fabioserpa/CNPJ-full. Esse script converte os arquivos zipados no site da Receita Federal para o formato sqlite ou csv.<br>
Para facilitar, o arquivo CNPJ_full.db gerado pelo script do fabioserpa foi colocado no Google Drive https://drive.google.com/drive/folders/1Gkeq27aHv6UgT8m30fc4hZWMPqdhEHWr?usp=sharing. <br>Se desejar colocar o banco de dados em algum lugar fora da pasta, altere a configuração em rede.ini.

## Opções:
Ao iniciar o script,  será aberto um console (para coletar erros) e http://127.0.0.1:5000/rede/ no navegador padrão. <br>

A roda do mouse expande ou diminui o tamanho da exibição.<br>
Fazendo click duplo em um ícone, a rotina expande as ligações.<br>
Apertando SHIFT, é possível selecionar mais de um ícone. <br>

Outras opções da rede estão no menu contextual do mouse (botão direito), sendo configuradas teclas de atalho correspondentes aos comandos:
 

## Tecla – Descrição do comando.
- TECLAS de 1 a 9 - Inserir camadas correspondente ao número sobre o nó selecionado;
- I - Inserir CNPJ, Razão Social completa ou nome completo de sócio. Poderão ser colocados vários CNPJs ao mesmo tempo, separados por ponto e vírgula (;).
- CRTL+Z – Desfaz Inserção;
- D – Abre um popup com dados de CNPJ;
- SHIFT+D – Abre numa nova janela com Dados, que pode ser selecionada e copiada;
- Exportar dados em Excel (somente itens selecionados ou toda a rede);
- L – Localizar na Tela Nome, CNPJ ou CPF;
- G – Abre o nó numa aba do site Google;
- SHIFT+G – Abre o endereço no Google Maps (só CNPJs);
- J – Abre o nó numa aba do site Jusbrasil;
- N - Rótulo - Exibe apenas o primeiro nome;
- A - Gráfico em Nova Aba;
- SHIFT+A - Abre uma nova Aba vazia;
- P - Fixar o nó na posição;
- Escala Inicial - Coloca a exibição sem zoom, na escala inicial.
- Q - Parar leiaute (se a tela tiver muitos nós, os comandos funcionam melhor se o leiaute estiver parado);
- SHIFT-Q- reiniciar leiaute;
- DEL – Excluir itens selecionados.

Os comandos valem para o último nó Selecionado, que fica com um retângulo preto em volta do ícone. 
Pressionando SHIFT e click, é possível selecionar mais de um ícone para fazer Exclusão.

## Fonte dos dados:

Base de CNPJ. A base de dados públicos de CNPJ da Receita Federal tem informação de Capital Social de empresas. A tabela de sócios contém apenas os sócios ativos de empresas, com CPF descaracterizado e nome completo do sócio.<br>
https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj<br>

Scripts em python para converter a base de cnpj da receita em sqlite:<br>
https://github.com/fabioserpa/CNPJ-full<br>
Obs: é preciso alterar o script para indexar a coluna “razão_social” da tabela empresas, senão a consulta por nome é lenta.<br>

Arquivo CNPJ_full.db completo, referência 23/11/2020, já no formato sqlite, dividido em cinco blocos, foi copiado no Google Drive:<br>
https://drive.google.com/drive/folders/1Gkeq27aHv6UgT8m30fc4hZWMPqdhEHWr?usp=sharing <br>
Para juntar os blocos, abra o primeiro (CNPJ_full-partes.7z.001) no 7zip. O arquivo compactado tem o tamanho de 4,1GB. O arquivo descompactado tem 22GB, por isso a descompactação pode demorar.<br>

## Outras referências:
Biblioteca em javascript para visualização:<br>
https://github.com/anvaka/VivaGraphJS<br>

Menu Contextual:<br>
https://www.cssscript.com/beautiful-multi-level-context-menu-with-pure-javascript-and-css3/
