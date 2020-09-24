# rede-cnpj

[![youtube](http://img.youtube.com/vi/nxz9Drhqn_I/0.jpg)](http://www.youtube.com/watch?v=nxz9Drhqn_I)

A rotina possibilita visualizar de forma gráfica os relacionamentos entre empresas e sócios. A rede foi testada nos navegadores Firefox, Edge e Chrome. NÃO FUNCIONA no Internet Explorer. 

A base de dados é o arquivo CNPJ_full.db, banco de dados no formato sqlite. Para exemplificar o funcionamento da rotina, esse arquivo tem cerca de mil registros com dados reais de empresas e de sócios. 

Há duas versões da rotina, uma executável e outra em script python.

Versão executável:
Para iniciar a versão executável, primeiro descompacte o arquivo “rede-versao-exe.7z” https://www.dropbox.com/s/x9zg2mh4vr8ftjs/rede-versai-exe-2020-09-23.7z?dl=0 Para executar a rotina, clique duas vezes no arquivo rede.exe.
 

A rotina abrirá o endereço http://127.0.0.1:5000/rede/ no navegador padrão e um console do DOS. Para parar a execução, feche o console.

Versão em python:
É preciso ter instalado no computador, um interpretador de linguagem python como a distribuída pelo Anaconda ou WinPython. Para iniciar esse script, em um console DOS digite 
python rede.py
Se der algum erro como “module <nome do módulo> not found”, instale o módulo pelo comando 
pip install <nome do módulo>.
Para facilitar a execução, edite o arquivo rede.bat, ajustando o caminho para ativar as variáveis de ambiente para o python.

Configurar nós iniciais:
Se não houver cpfcnpj inicial configurado em rede.ini, o navegador abrirá um popup pedindo para inserir um cnpj ou nome. Colocando TESTE (ou teste), será inserido um ícone com um cnpj aleatório do banco de dados.
Se desejar definir itens iniciais da rede de relacionamentos, edite o arquivo rede.ini em um editor de texto e altere a linha que começa com “cpfcnpjinicial=”. Isso só vai ser útil se vc estiver utilizando a base CNPJ_full.db completa.

Banco de dados de teste:
A pasta contém um arquivo CNPJ_full.db, que é o banco de dados sqlite. Ele é um pedaço do arquivo completo CNPJ_full.db obtido pelo script disponível em https://github.com/fabioserpa/CNPJ-full. Esse arquivo completo foi colocado no Google Drive https://drive.google.com/drive/folders/1FWogWd6raiKsuWUa2_M1cmV-OiTJcNqN?usp=sharing para facilitar o manuseio. Se desejar colocar o banco de dados em algum lugar fora da pasta, altere a configuração em rede.ini.

Você poderá substituir o banco de dados CNPJ_full.db pela base de dados públicos completa da Receita Federal.

Opções:
Ao executar o arquivo rede.exe,  será aberto um console (para coletar erros) e http://127.0.0.1:5000/rede/ no navegador padrão. 

A roda do mouse expande ou diminui o tamanho da exibição.
Fazendo click duplo em um ícone, a rotina expande as ligações.
Apertando SHIFT, é possível selecionar mais de um ícone. 

Outras opções da rede estão no menu contextual do mouse (botão direito), sendo configuradas teclas de atalho correspondentes aos comandos:
 

Tecla – Descrição do comando.
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
- A - Gráfico em Nova Aba;
- P - Fixar o nó na posição;
- Q - Parar leiaute (se a tela tiver muitos nós, os comandos funcionam melhor se o leiaute estiver parado);
- SHIFT-Q- reiniciar leiaute;
- DEL – Excluir itens selecionados.

Os comandos valem para o último nó Selecionado, que fica com um retângulo preto em volta do ícone. 
Pressionando SHIFT e click, é possível selecionar mais de um ícone para fazer Exclusão.

O menu contextual é uma contribuição do Fábio.

Fonte dos dados:

Base de CNPJ. A base de dados públicos de CNPJ da Receita Federal tem informação de Capital Social de empresas. A tabela de sócios contém apenas os sócios ativos de empresas, com CPF descaracterizado e nome completo do sócio.
https://receita.economia.gov.br/orientacao/tributaria/cadastros/cadastro-nacional-de-pessoas-juridicas-cnpj/dados-publicos-cnpj

Scripts em python para converter a base de cnpj da receita em sqlite:
https://github.com/fabioserpa/CNPJ-full
Obs: é preciso alterar o script para indexar a coluna “razão_social” da tabela empresas, senão a consulta por nome é lenta.

Arquivo CNPJ_full.db completo, referência julho/2020, já no formato sqlite, dividido em cinco blocos, foi copiado no Google Drive:
https://drive.google.com/drive/folders/1FWogWd6raiKsuWUa2_M1cmV-OiTJcNqN?usp=sharing 
Para juntar os blocos, abra o primeiro (final 001) no 7zip. O arquivo compactado tem o tamanho de 4,1GB. O arquivo descompactado tem 22Gb.
