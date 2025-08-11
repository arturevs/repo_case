import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from tqdm import tqdm

# Importa a Base declarativa e os modelos do novo arquivo
from .models import Base
from .views import VW_PERFORMANCE_SQL, VW_RANKING_ABSOLUTO_SQL

# --- Configurações ---
INPUT_DIR = Path("dados_brutos")
# Métrica alvo corrigida para corresponder aos arquivos de 2019
METRICA_ALVO = "Taxa de Respondidas em 5 dias Úteis"

# URLs diretas para os arquivos ODS
FILE_URLS = {
    "SMP": "https://www.anatel.gov.br/dadosabertos/PDA/IDA/SMP2019.ods",
    "STFC": "https://www.anatel.gov.br/dadosabertos/PDA/IDA/STFC2019.ods",
    "SCM": "https://www.anatel.gov.br/dadosabertos/PDA/IDA/SCM2019.ods"
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EtlPipeline:
    """
    Classe que encapsula o processo de ETL (Extração, Transformação e Carga)
    dos dados de atendimento das operadoras de telecomunicações.
    """

    def __init__(self, input_dir: Path, db_url: str):
        """
        Inicializa o pipeline de ETL.
        """
        self.input_dir = input_dir
        self.engine = create_engine(db_url)
        self.cleaned_data: Dict[str, pd.DataFrame] = {}
        self.final_df: pd.DataFrame = pd.DataFrame()

    def _setup_database(self) -> None:
        """
        Garante que a estrutura de tabelas e views exista no banco.
        """
        logging.info("Configurando o schema do banco de dados a partir dos modelos...")
        Base.metadata.create_all(self.engine)
        logging.info("Tabelas criadas com sucesso.")

        logging.info("Criando/Atualizando a view 'vw_performance_relativa_mercado'...")
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(text(VW_PERFORMANCE_SQL))
                connection.execute(text(VW_RANKING_ABSOLUTO_SQL))
        
        logging.info("Views criadas/atualizadas com sucesso.")

    def _download_source_files(self) -> None:
        """
        Baixa os arquivos ODS para um range de anos (2013-2019) para cada serviço.
        O processo é resiliente e não para se um arquivo específico não for encontrado.
        """
        logging.info("Iniciando o download do histórico de arquivos de dados (2013-2019)...")
        self.input_dir.mkdir(exist_ok=True)

        anos = range(2013, 2020)  # Gera anos de 2013 a 2019
        servicos = ["SMP", "STFC", "SCM"]
        base_url = "https://www.anatel.gov.br/dadosabertos/PDA/IDA/"

        for servico in servicos:
            for ano in anos:
                file_name = f"{servico}{ano}.ods"
                url = f"{base_url}{file_name}"
                output_path = self.input_dir / file_name

                try:
                    logging.info(f"Tentando baixar: {file_name}")
                    with requests.get(url, stream=True, timeout=60) as r:
                        # Lança um erro para status como 404 (Not Found)
                        r.raise_for_status()
                        
                        total_size = int(r.headers.get('content-length', 0))
                        with open(output_path, 'wb') as f, tqdm(
                            total=total_size, unit='iB', unit_scale=True, desc=file_name
                        ) as pbar:
                            for chunk in r.iter_content(chunk_size=8192):
                                f.write(chunk)
                                pbar.update(len(chunk))
                    logging.info(f"Arquivo '{file_name}' salvo com sucesso.")
                
                except requests.exceptions.HTTPError as e:
                    # Se o erro for 404, apenas avisa que o arquivo não existe e continua.
                    if e.response.status_code == 404:
                        logging.warning(f"Arquivo '{file_name}' não encontrado no servidor (404). Pulando.")
                    else:
                        logging.error(f"Falha ao baixar {file_name} com erro HTTP {e.response.status_code}.")
                except requests.exceptions.RequestException as e:
                    logging.error(f"Falha de conexão ao tentar baixar {file_name}: {e}")

    def _reformat_date_columns(self, columns: pd.Index) -> List[str]:
        """
        Renomeia as colunas de data do formato 'Mês/Ano' para 'AAAA-MM'.
        """
        month_map = {
            "jan": "01", "fev": "02", "mar": "03", "abr": "04", "mai": "05", "jun": "06",
            "jul": "07", "ago": "08", "set": "09", "out": "10", "nov": "11", "dez": "12",
        }
        new_column_names = []
        for col in columns:
            if isinstance(col, datetime):
                new_column_names.append(col.strftime("%Y-%m"))
                continue
            if not isinstance(col, str):
                new_column_names.append(col)
                continue
            try:
                cleaned_col = col.lower().replace(".", "").strip()
                if "/" in cleaned_col:
                    month_abbr, year_short = cleaned_col.split("/")
                    year = f"20{year_short.strip()}"
                    month = month_map.get(month_abbr.strip())
                    if month:
                        new_column_names.append(f"{year}-{month}")
                    else:
                        new_column_names.append(col)
                else:
                    new_column_names.append(col)
            except Exception:
                new_column_names.append(col)
        return new_column_names

    def extract_and_clean(self):
        """
        Extrai dados dos arquivos .ods, limpa-os e os armazena no atributo
        `self.cleaned_data`.
        """
        logging.info(f"Iniciando extração e limpeza do diretório: {self.input_dir}")
        ods_files = list(self.input_dir.glob("*.ods"))
        if not ods_files:
            logging.error(f"Nenhum arquivo .ods encontrado em '{self.input_dir}'. A etapa de download pode ter falhado.")
            return

        all_data = {}
        for file_path in ods_files:
            try:
                service_name = file_path.stem[:3]
                logging.info(f"Processando arquivo: {file_path.name} para o serviço '{service_name}'")
                df = pd.read_excel(file_path, engine="odf", header=8)
                df.dropna(how="all", axis=1, inplace=True)
                df.dropna(how="all", axis=0, inplace=True)
                df.columns = self._reformat_date_columns(df.columns)
                if service_name not in all_data:
                    all_data[service_name] = []
                all_data[service_name].append(df)
            except Exception as e:
                logging.error(f"Falha ao processar o arquivo {file_path.name}: {e}")

        final_dfs = {}
        for service, dfs in all_data.items():
            final_dfs[service] = pd.concat(dfs, ignore_index=True)
            logging.info(f"Serviço '{service}' consolidado com sucesso.")

        self.cleaned_data = final_dfs

    def transform(self):
        """
        Transforma os dados extraídos, realizando unpivot, filtragem e
        limpeza para preparar o DataFrame final para a carga.
        """
        if not self.cleaned_data:
            logging.warning("Dicionário de dados limpos está vazio. Encerrando a transformação.")
            return

        logging.info("Iniciando a fase de transformação (unpivot).")
        for service_name, df in self.cleaned_data.items():
            df["servico"] = service_name

        df_consolidated = pd.concat(self.cleaned_data.values(), ignore_index=True)
        id_vars = ["servico", "GRUPO ECONÔMICO", "VARIÁVEL"]
        value_vars = [col for col in df_consolidated.columns if col not in id_vars]

        df_unpivoted = df_consolidated.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="data_referencia",
            value_name="valor",
        )

        df_unpivoted["VARIÁVEL"] = df_unpivoted["VARIÁVEL"].str.strip()
        logging.info(f"Filtrando pela métrica alvo: '{METRICA_ALVO}'")
        df_filtered = df_unpivoted[df_unpivoted["VARIÁVEL"] == METRICA_ALVO].copy()

        if df_filtered.empty:
            logging.error(f"A métrica alvo '{METRICA_ALVO}' não foi encontrada.")
            self.final_df = pd.DataFrame()
            return

        df_final = df_filtered.drop(columns=["VARIÁVEL"])
        df_final.rename(
            columns={"GRUPO ECONÔMICO": "grupo_economico", "valor": "taxa_resolvidas_5_dias"},
            inplace=True,
        )
        df_final["data_referencia"] = pd.to_datetime(
            df_final["data_referencia"], format="%Y-%m"
        )
        df_final["taxa_resolvidas_5_dias"] = pd.to_numeric(
            df_final["taxa_resolvidas_5_dias"], errors="coerce"
        )
        df_final.dropna(subset=["taxa_resolvidas_5_dias", "grupo_economico"], inplace=True)

        self.final_df = df_final[
            ["data_referencia", "servico", "grupo_economico", "taxa_resolvidas_5_dias"]
        ]

    def load(self):
        """
        Carrega o DataFrame transformado para o Data Mart no PostgreSQL.
        """
        if self.final_df.empty:
            logging.warning("DataFrame vazio, nenhuma carga será realizada.")
            return

        logging.info("Iniciando a carga de dados para o PostgreSQL.")
        df = self.final_df

        with self.engine.connect() as conn:
            logging.info("Limpando tabelas existentes para garantir a idempotência.")
            conn.execute(text("TRUNCATE TABLE fato_atendimento, dim_tempo, dim_grupo_economico, dim_servico RESTART IDENTITY CASCADE;"))
            conn.commit()

        dim_servico_df = pd.DataFrame(df["servico"].unique(), columns=["nome_servico"])
        dim_servico_df.to_sql("dim_servico", self.engine, if_exists="append", index=False)
        logging.info(f"Carregados {len(dim_servico_df)} registros em dim_servico.")

        dim_grupo_df = pd.DataFrame(df["grupo_economico"].unique(), columns=["nome_grupo"])
        dim_grupo_df.to_sql("dim_grupo_economico", self.engine, if_exists="append", index=False)
        logging.info(f"Carregados {len(dim_grupo_df)} registros em dim_grupo_economico.")

        dim_tempo_df = pd.DataFrame({"data_referencia": df["data_referencia"].unique()})
        dim_tempo_df["ano"] = dim_tempo_df["data_referencia"].dt.year
        dim_tempo_df["mes"] = dim_tempo_df["data_referencia"].dt.month
        dim_tempo_df.to_sql("dim_tempo", self.engine, if_exists="append", index=False)
        logging.info(f"Carregados {len(dim_tempo_df)} registros em dim_tempo.")

        logging.info("Mapeando chaves estrangeiras para a tabela fato.")
        dim_servico_db = pd.read_sql("SELECT id_servico, nome_servico FROM dim_servico", self.engine)
        dim_grupo_db = pd.read_sql("SELECT id_grupo, nome_grupo FROM dim_grupo_economico", self.engine)
        dim_tempo_db = pd.read_sql("SELECT id_tempo, data_referencia FROM dim_tempo", self.engine)

        df = pd.merge(df, dim_servico_db, left_on="servico", right_on="nome_servico", how="left")
        df = pd.merge(df, dim_grupo_db, left_on="grupo_economico", right_on="nome_grupo", how="left")
        
        dim_tempo_db['data_referencia'] = pd.to_datetime(dim_tempo_db['data_referencia'])
        df = pd.merge(df, dim_tempo_db, on="data_referencia", how="left")

        df.rename(columns={
            "id_tempo": "tempo_id",
            "id_servico": "servico_id",
            "id_grupo": "grupo_economico_id",
            "taxa_resolvidas_5_dias": "valor"
        }, inplace=True)

        df_fato = df[["tempo_id", "servico_id", "grupo_economico_id", "valor"]].copy()

        logging.info(f"Carregando {len(df_fato)} registros na tabela fato_atendimento.")
        df_fato.to_sql("fato_atendimento", self.engine, if_exists="append", index=False)
        
        logging.info("Carga de dados para o PostgreSQL concluída com sucesso.")

    def run(self):
        """
        Orquestra a execução completa do pipeline de ETL.
        """
        logging.info("--- INICIANDO PIPELINE DE ETL PARA DADOS IDA ---")
        try:
            self._setup_database()
            self._download_source_files()
            self.extract_and_clean()
            self.transform()
            self.load()
            logging.info("ETL concluído com sucesso.")
        except Exception as e:
            logging.error(f"Ocorreu um erro durante a execução do pipeline: {e}")
        finally:
            logging.info("--- FIM DO PIPELINE ---")


def main():
    """
    Função principal que configura e executa o pipeline de ETL.
    """
    try:
        db_user = os.getenv("POSTGRES_USER", "default_user")
        db_password = os.getenv("POSTGRES_PASSWORD", "default_password")
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "default_db")

        database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        pipeline = EtlPipeline(input_dir=INPUT_DIR, db_url=database_url)
        pipeline.run()

    except Exception as e:
        logging.error(f"Falha ao iniciar o pipeline de ETL: {e}")


if __name__ == "__main__":
    main()