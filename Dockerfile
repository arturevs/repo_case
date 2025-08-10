# 1. Usar a imagem base oficial do Python
FROM python:3.11.12-bookworm

# 2. Definir o diretório de trabalho
WORKDIR /app

# 3. Copiar apenas o arquivo de definição do projeto
COPY pyproject.toml .

# 4. Instalar as dependências a partir do pyproject.toml, isso evita problemas de versionamento futuros.
# O "." indica para o pip instalar o projeto no diretório atual
RUN pip install --no-cache-dir .

# 5. Copiar o resto do projeto
COPY . .

# 6. Comando de execução
CMD ["python", "src/etl.py"]