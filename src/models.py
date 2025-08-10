# models.py

from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Numeric,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Base declarativa que será herdada por todos os modelos ORM.
Base = declarative_base()


class DimTempo(Base):
    """
    Modelo ORM para a tabela de dimensão de Tempo.
    Armazena informações temporais como ano e mês.
    """
    __tablename__ = "dim_tempo"
    __table_args__ = {"comment": "Dimensão que armazena informações temporais, como ano e mês."}

    id_tempo = Column(Integer, primary_key=True, comment="Chave primária autoincremental da dimensão de tempo.")
    data_referencia = Column(Date, nullable=False, unique=True, comment="Data completa de referência (primeiro dia do mês).")
    ano = Column(Integer, nullable=False, comment="Ano extraído da data de referência.")
    mes = Column(Integer, nullable=False, comment="Mês extraído da data de referência.")

    # Relacionamento com a tabela fato (opcional, mas bom para ORM)
    atendimentos = relationship("FatoAtendimento", back_populates="tempo")


class DimGrupoEconomico(Base):
    """
    Modelo ORM para a tabela de dimensão de Grupo Econômico.
    Armazena os nomes das operadoras.
    """
    __tablename__ = "dim_grupo_economico"
    __table_args__ = {"comment": "Dimensão que armazena os nomes dos grupos econômicos (operadoras)."}

    id_grupo = Column(Integer, primary_key=True, comment="Chave primária autoincremental da dimensão de grupo econômico.")
    nome_grupo = Column(String(100), nullable=False, unique=True, comment="Nome único do grupo econômico.")

    atendimentos = relationship("FatoAtendimento", back_populates="grupo_economico")


class DimServico(Base):
    """
    Modelo ORM para a tabela de dimensão de Serviço.
    Armazena os tipos de serviço de telecomunicações.
    """
    __tablename__ = "dim_servico"
    __table_args__ = {"comment": "Dimensão que armazena os tipos de serviço de telecomunicações."}

    id_servico = Column(Integer, primary_key=True, comment="Chave primária autoincremental da dimensão de serviço.")
    nome_servico = Column(String(50), nullable=False, unique=True, comment="Nome único do serviço (ex: SMP, SCM, STFC).")

    atendimentos = relationship("FatoAtendimento", back_populates="servico")


class FatoAtendimento(Base):
    """
    Modelo ORM para a tabela fato de Atendimento.
    Armazena as métricas de desempenho (IDA) e suas chaves para as dimensões.
    """
    __tablename__ = "fato_atendimento"
    __table_args__ = (
        UniqueConstraint("tempo_id", "servico_id", "grupo_economico_id", name="uq_atendimento_contexto"),
        {"comment": "Tabela fato que armazena as métricas de desempenho de atendimento (IDA)."}
    )

    id = Column(Integer, primary_key=True, comment="Chave primária para o registro da fato.")
    tempo_id = Column(Integer, ForeignKey("dim_tempo.id_tempo"), comment="Chave estrangeira referenciando a dimensão de tempo (dim_tempo).")
    servico_id = Column(Integer, ForeignKey("dim_servico.id_servico"), comment="Chave estrangeira referenciando a dimensão de serviço (dim_servico).")
    grupo_economico_id = Column(Integer, ForeignKey("dim_grupo_economico.id_grupo"), comment="Chave estrangeira referenciando a dimensão de grupo econômico (dim_grupo_economico).")
    valor = Column(Numeric(10, 4), comment="O valor da métrica (ex: taxa de resolvidas em 5 dias).")

    # Relacionamentos para facilitar o acesso aos objetos dimensionais
    tempo = relationship("DimTempo", back_populates="atendimentos")
    servico = relationship("DimServico", back_populates="atendimentos")
    grupo_economico = relationship("DimGrupoEconomico", back_populates="atendimentos")