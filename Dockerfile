# 1. Usar a imagem base oficial do Python, conforme especificado no case
FROM python:3.11.12-bookworm

# 2. Definir o diretório de trabalho dentro do container
WORKDIR /app

# 3. Copiar o arquivo de dependências primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# 4. Instalar as dependências
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copiar todo o resto do projeto para o diretório de trabalho no container
COPY . .

# 6. O comando que será executado quando o container iniciar.
#    O comando real será sobrescrito no docker-compose.yml para mais controle.
CMD ["python", "etl.py"]