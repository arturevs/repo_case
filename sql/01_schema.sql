-- Criando a dimensão de Tempo
CREATE TABLE dim_tempo (
    id_tempo SERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    ano INT NOT NULL,
    mes INT NOT NULL
);

COMMENT ON TABLE dim_tempo IS 'Dimensão que armazena informações temporais, como ano e mês.';
COMMENT ON COLUMN dim_tempo.id_tempo IS 'Chave primária autoincremental da dimensão de tempo.';
COMMENT ON COLUMN dim_tempo.data_referencia IS 'Data completa de referência (primeiro dia do mês).';
COMMENT ON COLUMN dim_tempo.ano IS 'Ano extraído da data de referência.';
COMMENT ON COLUMN dim_tempo.mes IS 'Mês extraído da data de referência.';


-- Criando a dimensão de Grupo Econômico
CREATE TABLE dim_grupo_economico (
    id_grupo SERIAL PRIMARY KEY,
    nome_grupo VARCHAR(100) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_grupo_economico IS 'Dimensão que armazena os nomes dos grupos econômicos (operadoras).';
COMMENT ON COLUMN dim_grupo_economico.id_grupo IS 'Chave primária autoincremental da dimensão de grupo econômico.';
COMMENT ON COLUMN dim_grupo_economico.nome_grupo IS 'Nome único do grupo econômico.';


-- Criando a dimensão de Serviço
CREATE TABLE dim_servico (
    id_servico SERIAL PRIMARY KEY,
    nome_servico VARCHAR(50) NOT NULL UNIQUE
);

COMMENT ON TABLE dim_servico IS 'Dimensão que armazena os tipos de serviço de telecomunicações.';
COMMENT ON COLUMN dim_servico.id_servico IS 'Chave primária autoincremental da dimensão de serviço.';
COMMENT ON COLUMN dim_servico.nome_servico IS 'Nome único do serviço (ex: SMP, SCM, STFC).';


CREATE TABLE fato_atendimento (
    id SERIAL PRIMARY KEY,
    -- Chaves Estrangeiras para ligar com as dimensões
    -- CORREÇÃO: Apontar para os nomes corretos das chaves primárias das dimensões
    tempo_id INTEGER REFERENCES dim_tempo(id_tempo),
    servico_id INTEGER REFERENCES dim_servico(id_servico),
    grupo_economico_id INTEGER REFERENCES dim_grupo_economico(id_grupo),
    -- A métrica principal
    valor NUMERIC(10, 4),
    -- Restrição de unicidade para evitar dados duplicados para o mesmo contexto
    UNIQUE (tempo_id, servico_id, grupo_economico_id)
);

-- Os comentários permanecem os mesmos...
COMMENT ON TABLE fato_atendimento IS 'Tabela fato que armazena as métricas de desempenho de atendimento (IDA).';
COMMENT ON COLUMN fato_atendimento.id IS 'Chave primária para o registro da fato.';
COMMENT ON COLUMN fato_atendimento.tempo_id IS 'Chave estrangeira referenciando a dimensão de tempo (dim_tempo).';
COMMENT ON COLUMN fato_atendimento.servico_id IS 'Chave estrangeira referenciando a dimensão de serviço (dim_servico).';
COMMENT ON COLUMN fato_atendimento.grupo_economico_id IS 'Chave estrangeira referenciando a dimensão de grupo econômico (dim_grupo_economico).';
COMMENT ON COLUMN fato_atendimento.valor IS 'O valor da métrica (ex: taxa de resolvidas em 5 dias).';