# REDE-CNPJ - Visualização de dados públicos de CNPJ

## TUTORIAL:
Instalação passo-a-passo no Windows:<br>

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
python rede.py<br>
para executar a rede-cnpj<br>
![image](https://user-images.githubusercontent.com/71139693/179335510-4f092b99-c988-4c02-a22d-200f500d8d42.png)

 - O console vai ficar desta forma, com uma linha "Running on http:...":<br>
 ![image](https://user-images.githubusercontent.com/71139693/179633950-4f5e28c8-fafb-4b63-8ff5-8e3696da36e9.png)

 - O script vai tentar abrir uma janela no navegador padrão:<br>
  ![image](https://user-images.githubusercontent.com/71139693/179335572-768b1699-a92d-4ddc-92af-538b8a07f145.png)

 - O projeto está rodando localmente com uma base de testes.
 - Para parar, pressione CTRL+C no console. 
 
  
 ## USAR A BASE COMPLETA DE CNPJS: <br>
 - Para baixar a base inteira de cnpj, vá para a página https://github.com/rictom/cnpj-sqlite#arquivo_sqlite <br>
  ![image](https://user-images.githubusercontent.com/71139693/179335625-98c0087b-ce6b-457a-8cca-f95983413328.png)
 
 - Clique no link http://www.mediafire.com...<br>
  ![image](https://user-images.githubusercontent.com/71139693/179335636-451a2164-84d4-4265-9980-17096ca1253b.png)

 - Baixe o arquivo cnpj.db.20aa-xx-xx.7z, que tem cerca de 7GB. Faça o download do 7zip https://www.7-zip.org/download.html e descompacte o arquivo cnpj.db com o 7-zip.<br>
 - Copie o arquivo cnpj.db para a pasta rede: <br>
  ![image](https://user-images.githubusercontent.com/71139693/179335685-d193dcf6-738e-4628-8221-b5132896c27a.png)

 - A partir da versão 0.9, é preciso gerar um arquivo rede.db. Para isso, rode o comando <b>python rede_cria_tabela.py</b><br>
 - Para utilizar a base completa na rede-cnpj, abra o arquivo o rede.ini no Bloco de Notas:<br>
 ![image](https://user-images.githubusercontent.com/71139693/208316096-b29c636e-6af1-4cbd-810a-1d7a3eec4a3f.png)

 - Altere o parâmetro base_rede para rede.db e base_receita para cnpj.db<br>
![image](https://user-images.githubusercontent.com/71139693/208316115-0054e118-df41-46d6-8474-ae2618039fef.png)

 - Altere também a mensagem_advertencia para não causar confusão. Tire o # do primeiro mensagem_advertencia e coloque #  no segundo mensagem_advertencia. <br>
   ![image](https://user-images.githubusercontent.com/71139693/179335724-39085411-4caf-4ee5-ac5b-275ff195a8a8.png)
 - Salve o arquivo rede.ini. <br>
 - Se a rede-cnpj ainda estiver rodando no console, pressione CTRL+C para parar (não dá para rodar duas instâncias do projeto ao mesmo tempo)<br>
 - Digite no console python rede.py<br>
![image](https://user-images.githubusercontent.com/71139693/179335747-16939bf1-0f02-4329-849d-d41677f05920.png)

 - Agora o projeto está rodando localmente com a base completa de cnpjs.<br>
  
 ## Para visualizar os links de endereços, telefones e e-mails
 - Baixe o arquivo cnpj_links:<br>
 ![image](https://user-images.githubusercontent.com/71139693/179335797-85f19fdf-30bc-4e4c-afc9-d2550209dedd.png)

 - Descompacte e coloque na pasta rede.<br>
 - Altere o arquivo de configuração rede.ini<br>
![image](https://user-images.githubusercontent.com/71139693/179335812-edc5461b-1bee-45ee-8741-d1171e919b9b.png)
- Coloque base_endereco_normalizado = cnpj_links_ete.db. Salve o rede.ini e reinicie o projeto.


  
  


