# REDE-CNPJ - Visualização de dados públicos de CNPJ

## TUTORIAL:
Instalação passo-a-passo no Windows (versão 1/6/2023):<br> 

- Instale o Anaconda, no link https://www.anaconda.com/<br>
![image](https://user-images.githubusercontent.com/71139693/179334927-750cff12-88ce-4102-b004-05a9f005c470.png)

- Baixe o zip do projeto:<br>
![image](https://user-images.githubusercontent.com/71139693/179334945-881453bc-2da8-468e-99e4-0a4a9affdcaf.png)

- Descompacte o zip<br>
![image](https://user-images.githubusercontent.com/71139693/179334963-dff2b823-d932-4553-be3f-52d466266728.png)

- Abra um console no ambiente do anaconda, pelo menu Windows>Anaconda>Anaconda Prompt (tem que ser pelo console no “ambiente python” já configurado, por isso abrir um console do Windows direto pode dar erro)<br>
![image](https://user-images.githubusercontent.com/71139693/179335002-31a9888c-3659-4236-9e01-db8a4054cfd0.png)

- O console vai aparecer assim, começando com (base)<br>
![image](https://user-images.githubusercontent.com/71139693/179335162-cd0fa7e1-0425-46e8-a2a6-6697af9edecc.png)

- Mova o console até a pasta que foi descompactada, a rede-cnpj-master. Uma dica é usar o shift + botão direito para copiar o caminho até a pasta<br>
![image](https://user-images.githubusercontent.com/71139693/179335410-6f935843-d8ce-4b83-8fcf-7ff051751353.png)

- No console, digite cd e cole o caminho:<br>
![image](https://user-images.githubusercontent.com/71139693/179335454-d52e449c-2fc9-4fd1-8ca9-d3b3d475ecd9.png)

![image](https://user-images.githubusercontent.com/71139693/179335459-3c537cea-f1b8-4232-b106-5684c0c071fc.png)

- Digite pip install -r requirements.txt, para instalar as bibliotecas necessárias para rodar o projeto.<br>
![image](https://user-images.githubusercontent.com/71139693/179335475-ab1279d7-c96f-40d8-9109-90449efb88b5.png)
![image](https://user-images.githubusercontent.com/71139693/179335482-85938f00-3176-45ed-82be-d51b54c30e6b.png)

- O Projeto pode ser executado, mas deve se mudar o console para a pasta rede. Digite cd rede <Enter> e na outra linha<br>
 <b>python rede.py</b><br>
para executar a rede-cnpj<br>
![image](https://user-images.githubusercontent.com/71139693/179335510-4f092b99-c988-4c02-a22d-200f500d8d42.png)

 - O console vai ficar desta forma, com uma linha "Running on http:...":<br>
 ![image](https://user-images.githubusercontent.com/71139693/179633950-4f5e28c8-fafb-4b63-8ff5-8e3696da36e9.png)

 - O script vai tentar abrir uma janela no navegador padrão:<br>
  ![image](https://user-images.githubusercontent.com/71139693/179335572-768b1699-a92d-4ddc-92af-538b8a07f145.png)

 - O projeto está rodando localmente com uma base de testes.
 - Para parar, pressione CTRL+C no console. 
 
  
 ## USAR A BASE COMPLETA DE CNPJS: <br>
 - As instruções para gerar a base completa de CNPJs estão na página https://github.com/rictom/cnpj-sqlite, mas para facilitar o uso, o código foi replicado na neste projeto.<br>
 - Posicione o console na pasta rede_cria_tabelas. Use cd .. ou cd rede_cria_tabelas .<br>
 - A pasta rede_cria_tabelas contém os scripts para baixar os arquivos zip do site de Dados Abertos, gerar a base completa de empresas e as tabelas utilizadas na redeCNPJ.<br>
 - a) para baixar os arquivos zip do site de Dados Abertos, rode o comando <b>python dados_cnpj_baixa.py</b> . Após o processo, deverá haver 37 arquivos .zip na pasta dados-publicos-zip. <br>
 - b) para criar a base de empresas cnpj.db, rode o comando <b>python dados_cnpj_para_sqlite.py</b> . Isso irá criar um arquivo cnpj.db na pasta dados-publicos em torno de 30GB.<br>
 - c) para criar a tabela de vínculos, execute o comando <b>python rede_cria_tabela_rede.db.py</b> . Ao final, deverá haver os arquivos rede.db e rede_search.db na pasta dados-publicos.<br>
 - d) para criar a tabela de vínculos de endereços, rode o comando <b>python rede_cria_tabela_cnpj_links_ete.py</b> . Isto irá gerar o arquivo cnpj_links_ete.db.<br>

<br>
 - Depois de seguir as orientações a) até d), mova os arquivos cnpj.db, rede.db, rede_search.db, cnpj_links_ete.db da pasta <b>rede_cria_tabelas/dados-publicos</b> para a <b>rede/bases</b>. Os arquivos restantes dentro das pastas dados-publicos e dados-publicos-zip poderão ser apagados.<br>
<br>
 - Observação: O cnpj_links_ete.db é opcional, somente se quiser visualizar vinculos por endereços, telefones ou email em comum.<br>

 - Para uso na redeCNPJ, os arquivos podem ser posicionados em outros locais, alterando-se o arquivo o rede.ini no Bloco de Notas. Por exemplo, se quiser colocar em um disco externo, faça algo como:<br>
<b>base_rede=D:/arquivos_grandes/rede.db</b><br>

 - No arquivo rede.ini, altere também a mensagem_advertencia para não causar confusão. Tire o # do primeiro mensagem_advertencia e coloque #  no segundo mensagem_advertencia. <br>
   ![image](https://user-images.githubusercontent.com/71139693/179335724-39085411-4caf-4ee5-ac5b-275ff195a8a8.png)
 - Altere o parâmetro referencia_bd que será exibido na parte superior da tela da redeCNPJ e coloque a data da base de CNPJs, por exemplo:<br>
   <b>referencia_bd=CNPJ(11/02/2023)</b><br>
 - Salve o arquivo rede.ini. <br>
 - Se a rede-cnpj ainda estiver rodando no console, pressione CTRL+C para parar (não dá para rodar duas instâncias do projeto ao mesmo tempo)<br>
 - Digite no console <b>python rede.py</b><br>
![image](https://user-images.githubusercontent.com/71139693/179335747-16939bf1-0f02-4329-849d-d41677f05920.png)

 - Agora o projeto está rodando localmente com a base completa de cnpjs.<br>


 - Os scripts para criação das tabelas estão disponíveis como aplicativo Windows no link https://www.redecnpj.com.br/rede/pag/aplicativo.html<br>


  
  


