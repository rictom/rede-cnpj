# Use uma imagem base com Python 3.9
FROM python:3.9

# Atualize o sistema e instale as dependências básicas
RUN apt-get update && \
    apt-get install -y libpoppler-cpp-dev && \
    apt-get clean

# Crie um diretório de trabalho
WORKDIR /app

# Copie o arquivo requirements.txt para o diretório de trabalho
COPY requirements.txt .

# Instale as dependências especificadas no arquivo requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copie o código do aplicativo para o diretório de trabalho
COPY . .

# Define o diretório de trabalho como "rede"
WORKDIR /app/rede

# Defina o comando padrão para executar o aplicativo
CMD ["python", "rede.py"]
