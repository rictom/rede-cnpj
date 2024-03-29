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
 - As instruções para gerar a base completa de CNPJs estão na página https://github.com/rictom/cnpj-sqlite <br>
 - Copie o arquivo cnpj.db para a pasta rede/bases: <br>

 - A partir da versão 1.0, é preciso gerar um arquivo rede.db e rede_search.db. Para isso, posicione o console na pasta bases e rode o comando <b>python rede_cria_tabela_rede.db.py</b>. Obs: Se o computador este tiver pouca memória RAM, a rotina poderá dar erro de "database or disk full"<br>
 - Para utilizar a base completa na rede-cnpj, abra o arquivo o rede.ini no Bloco de Notas:<br>
<b>base_rede = bases/rede.db<br>
base_rede_search = bases/rede_search.db<br>
 base_receita = bases/cnpj.db<br></b>

 - Altere também a mensagem_advertencia para não causar confusão. Tire o # do primeiro mensagem_advertencia e coloque #  no segundo mensagem_advertencia. <br>
   ![image](https://user-images.githubusercontent.com/71139693/179335724-39085411-4caf-4ee5-ac5b-275ff195a8a8.png)
 - Altere o parâmetro referencia_bd que será exibido na parte superior da tela da redeCNPJ e coloque a data da base de CNPJs, por exemplo:<br>
   <b>referencia_bd=CNPJ(11/02/2023)</b><br>
 - Salve o arquivo rede.ini. <br>
 - Se a rede-cnpj ainda estiver rodando no console, pressione CTRL+C para parar (não dá para rodar duas instâncias do projeto ao mesmo tempo)<br>
 - Digite no console <b>python rede.py</b><br>
![image](https://user-images.githubusercontent.com/71139693/179335747-16939bf1-0f02-4329-849d-d41677f05920.png)

 - Agora o projeto está rodando localmente com a base completa de cnpjs.<br>
  
 ## Para visualizar os links de endereços, telefones e e-mails
 - Gere o arquivo cnpj_links_ete.db pelo comando <b>python rede_cria_tabela_cnpj_links_ete.py</b><br>
 - Obs: O arquivo cnpj_links_ete.db é opcional, a redeCNPJ funciona mesmo sem este arquivo, que tem os vínculos entre empresas com mesmo endereço, telefone ou email.


 - Altere o arquivo de configuração rede.ini<br>
 <b>base_endereco_normalizado = cnpj_links_ete.db.</b><br>
 Salve o rede.ini e reinicie o projeto.<br>


  
  


