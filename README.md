# Análise de Desempenho de Atendimento (IDA) - Data Mart

Este projeto implementa um pipeline de ETL (Extração, Transformação e Carga) completo e automatizado para analisar o Índice de Desempenho no Atendimento (IDA) de operadoras de telecomunicações no Brasil. Os dados são extraídos do Portal de Dados Abertos do governo brasileiro, processados e carregados num Data Mart dimensional (modelo estrela) em PostgreSQL.

O objetivo final é permitir análises de performance, como a variação mensal do atendimento de cada operadora em relação à média do mercado, através de uma `VIEW` SQL pré-construída para facilitar a consulta.

## Funcionalidades

- **Download Automatizado:** O pipeline inicia com o download automático dos arquivos de dados `.ods` para os serviços SMP, STFC e SCM, abrangendo o histórico de 2013 a 2019.
- **Pipeline de ETL Robusto:** Utiliza Python com Pandas para extrair, limpar, transformar e carregar os dados de forma eficiente e idempotente.
- **Banco de Dados Dimensional:** O schema do banco (tabelas e view) é criado programaticamente usando SQLAlchemy ORM, garantindo uma fonte única da verdade no código Python.
- **Views Analíticas:** Duas `VIEW`s SQL são criadas automaticamente para análises distintas:
  - `vw_performance_relativa_mercado`: Compara a variação mensal de performance de cada operadora contra a média do mercado.
  - `vw_ranking_desempenho_absoluto`: Cria um ranking mensal de operadoras com base no valor absoluto do indicador para cada serviço.
- **Containerizado com Docker:** Todo o ambiente (banco de dados PostgreSQL e aplicação Python) é orquestrado com Docker Compose, permitindo a execução completa do projeto com um único comando.

## Tecnologias Utilizadas

- **Linguagem:** Python 3.11
- **Containerização:** Docker & Docker Compose
- **Banco de Dados:** PostgreSQL
- **Bibliotecas Python:**
  - SQLAlchemy (para ORM e conexão com o DB)
  - Pandas (para manipulação de dados)
  - Requests (para o download dos arquivos)
  - ODFPy (para leitura dos arquivos `.ods`)
  - UV (para gestão de dependências)

## Estrutura do Projeto

```
.
├── dados_brutos/      # Diretório onde os arquivos .ods são baixados e persistidos
├── src/
│   ├── etl.py         # Script principal do pipeline de ETL
│   ├── models.py      # Definição do schema do DB com SQLAlchemy ORM
│   └── views.py       # Definição da view SQL analítica
├── .env               # Arquivo de variáveis de ambiente (PRECISA SER CRIADO)
├── .gitignore
├── docker-compose.yml # Orquestração dos serviços Docker
├── Dockerfile         # Receita para construir a imagem da aplicação Python
├── pyproject.toml     # Definição do projeto e suas dependências
└── README.md
```

## Visualizando a Documentação

O projeto foi inteiramente documentado utilizando docstrings, seguindo as melhores práticas do Python. Você pode explorar a documentação de todas as classes e métodos iniciando um servidor web local com o `pydoc`.

1.  **Inicie o Servidor:**
    No seu terminal, a partir da **raiz do projeto**, execute o seguinte comando:
    ```bash
    python -m pydoc -b -p 8001
    ```
    - O seu terminal indicará que o servidor está a correr em `http://localhost:8001/`. O processo ficará ativo.

2.  **Navegue pela Documentação:**
    - Abra o seu navegador e aceda a **`http://localhost:8001/`**.
    - Na página, clique no pacote **`src`** para ver os módulos do projeto.
    - A partir daí, pode clicar em `etl`, `models` ou `views` para ver a documentação detalhada de cada um.

3.  **Para Desligar o Servidor:**
    - Volte ao seu terminal e pressione `Ctrl + C`.

## Pré-requisitos

Para executar este projeto, você precisa ter instalado:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Como Executar

Siga os passos abaixo para executar o pipeline completo.

### 1. Clonar o Repositório

```bash
git clone <url-do-seu-repositorio>
cd <nome-do-repositorio>
```

### 2. Criar o Arquivo de Variáveis de Ambiente

Este é o passo mais importante. O Docker Compose precisa de um arquivo `.env` para configurar as credenciais do banco de dados. Crie um arquivo chamado `.env` na raiz do projeto.

Copie e cole o seguinte conteúdo dentro do seu arquivo `.env`:
```env
# Variáveis para o serviço do PostgreSQL
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin
POSTGRES_DB=beanalytic_db

# Variáveis usadas pela aplicação Python para se conectar ao DB
# POSTGRES_HOST deve ser o nome do serviço do DB no docker-compose.yml
POSTGRES_HOST=db 
POSTGRES_PORT=5432
```

### 3. Executar o Docker Compose

Com o arquivo `.env` criado, execute o seguinte comando no seu terminal. Ele irá construir a imagem da aplicação, iniciar os serviços e rodar o pipeline de ETL do início ao fim.

```bash
docker-compose up --build
```
- A flag `--build` garante que a imagem Docker seja construída do zero, aplicando quaisquer alterações que você tenha feito nos arquivos.

O processo pode levar alguns minutos na primeira vez. Você verá os logs de cada etapa: configuração do banco, download dos arquivos, extração, transformação e carga.

## Acessando os Resultados

Após a conclusão do script (você verá a mensagem "ETL concluído com sucesso" nos logs), o Data Mart estará populado e pronto para ser consultado.

Você pode se conectar ao banco de dados usando seu cliente SQL preferido (DBeaver, DataGrip, etc.) com as seguintes informações:

- **Host:** `localhost`
- **Porta:** `5433` (conforme definido no `docker-compose.yml`)
- **Banco de Dados:** `beanalytic_db`
- **Usuário:** `admin`
- **Senha:** `admin`

Para visualizar o resultado final da análise de performance, execute a seguinte consulta:

```sql
SELECT * FROM vw_performance_relativa_mercado;
```

# Documento Suplementar: Análise Estratégica para Evolução do Data Mart

## 1. Introdução

Este documento acompanha a solução técnica funcional entregue para o case e tem como objetivo aprofundar a análise sobre a evolução estratégica do Data Mart. Após a implementação bem-sucedida do pipeline para os dados do Índice de Desempenho no Atendimento (IDA), o próximo passo lógico é considerar a integração do seu sucessor, o dataset do **Regulamento de Qualidade dos Serviços de Telecomunicações (R-QUAL)**, que entrou em vigor a partir de 2019.

A análise a seguir detalha duas estratégias distintas para abordar esta integração, cada uma com seus próprios trade-offs entre completude histórica, complexidade de implementação e profundidade analítica.

---

## 2. Estratégia 1: Unificação Histórica via Transformação Prévia (IDA + R-QUAL)

Esta estratégia foca em criar um único dataset contínuo, unificando os dados históricos do IDA com os dados mais recentes do R-QUAL. O processo envolveria a criação de uma camada de transformação robusta no pipeline de ETL para mapear e compatibilizar os dois schemas em um formato canônico antes da carga no Data Mart.

### Pontos Fortes

* **Continuidade e Visão de Longo Prazo:** O principal benefício é a capacidade de realizar análises de tendências que abrangem todo o período histórico disponível (pré e pós-2019). Isso permite uma visão macro sobre a evolução da performance das operadoras, o que é de imenso valor estratégico.
* **Maximização do Ativo de Dados:** Garante que toda a informação coletada (tanto do IDA quanto do R-QUAL) seja aproveitada, extraindo o máximo de valor dos dados disponíveis e justificando o esforço de coleta e armazenamento.

### Pontos Fracos

* **Aumento da Complexidade Técnica:** A implementação de uma lógica de mapeamento entre dois schemas potencialmente distintos dentro do ETL eleva consideravelmente a complexidade do código. Isso exige um tratamento cuidadoso de exceções e uma documentação rigorosa.
* **Risco de Viés Analítico na Unificação:** Este é um risco sutil, mas crítico. As métricas, mesmo que tenham nomes parecidos, podem ter sido definidas ou calculadas de formas diferentes entre o IDA e o R-QUAL. Uma normalização imperfeita pode introduzir um "degrau" artificial na série histórica, levando a conclusões analíticas incorretas sobre o desempenho das operadoras. A validação e documentação dessas regras de negócio são essenciais para mitigar este risco.

---

## 3. Estratégia 2: Foco no Dataset Moderno (Uso Exclusivo do R-QUAL)

Esta abordagem propõe uma decisão estratégica de focar exclusivamente na fonte de dados mais recente, rica e que terá continuidade no futuro. O pipeline seria otimizado para extrair e processar apenas os dados do R-QUAL, desconsiderando os dados históricos do IDA.

### Pontos Fortes

* **Maior Granularidade e Profundidade Analítica:** Ao concentrar os esforços no R-QUAL, a análise pode se beneficiar de um dataset potencialmente mais detalhado e padronizado, resultando em insights mais precisos e profundos sobre o cenário competitivo recente.
* **Simplicidade e Manutenibilidade do Pipeline:** O ETL se torna significativamente mais simples, pois precisa lidar com apenas um schema de origem. Isso reduz o tempo de desenvolvimento, facilita a manutenção e diminui a probabilidade de erros.

### Pontos Fracos

* **Perda da Perspectiva Histórica:** A principal desvantagem é a incapacidade de analisar tendências de longo prazo ou comparar o desempenho atual com o período anterior a 2019. Perde-se um contexto histórico valioso.
* **Desvio do Escopo Implícito:** Embora não explícito, a solicitação de compilar dados de múltiplos serviços sugere um desejo por uma visão abrangente. Adotar apenas a fonte de dados mais recente poderia ser interpretado como uma entrega parcial, que não utiliza todos os ativos de dados disponíveis.

---

## 4. Conclusão e Recomendação

A escolha entre as estratégias representa um trade-off fundamental entre **amplitude histórica (Estratégia 1)** e **profundidade e simplicidade recente (Estratégia 2)**.

A solução entregue neste case estabelece uma base sólida ao processar com sucesso os dados do IDA. A partir daqui:

* A **Estratégia 1 (Unificação)** é o caminho de maior valor de negócio, pois atende à necessidade de análise de longo prazo. É a evolução mais completa, mas exige um investimento cuidadoso na análise de compatibilidade das métricas para evitar a introdução de viés.

* A **Estratégia 2 (Apenas R-QUAL)** seria uma alternativa viável somente se houvesse uma confirmação por parte do "cliente" (o stakeholder do projeto) de que a análise histórica pré-2019 não é uma prioridade e que o foco absoluto está na precisão e granularidade dos dados mais recentes.

**Recomendação:**

A recomendação principal é, antes de tudo, realizar uma Análise de Viabilidade para determinar se a unificação dos datasets IDA e R-QUAL é analiticamente válida. Esta fase é crucial, pois, dependendo da magnitude das diferenças de schema e métricas, a integração pode não ser viável ou recomendável, arriscando a introdução de um viés que comprometeria as análises.

Somente após esta validação, a etapa seguinte seria apresentar as soluções possíveis ao cliente, evidenciando os pontos fortes e fracos de cada caminho. O resultado da análise definirá as opções realistas a serem discutidas: se a unificação for viável, ambas as estratégias (Unificação Histórica e Foco no R-QUAL) seriam apresentadas para uma decisão de negócio; caso contrário, a única proposta íntegra seria focar exclusivamente no dataset R-QUAL.