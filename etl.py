import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import logging
from datetime import datetime
import os
from sqlalchemy import create_engine, text

# --- Configurações ---
INPUT_DIR = Path("dados_brutos")
# Não vamos mais salvar em CSV, então podemos remover as configs de output
METRICA_ALVO = "Taxa de Resolvidas em 5 dias Úteis"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# As funções extract_and_clean e reformat_date_columns permanecem EXATAMENTE AS MESMAS
# ... (cole aqui as suas funções reformat_date_columns e extract_and_clean que já funcionam) ...
def reformat_date_columns(columns: pd.Index) -> List[str]:
    month_map = {
        'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04', 'mai': '05', 'jun': '06',
        'jul': '07', 'ago': '08', 'set': '09', 'out': '10', 'nov': '11', 'dez': '12'
    }
    new_column_names = []
    for col in columns:
        if isinstance(col, datetime):
            new_column_names.append(col.strftime('%Y-%m'))
            continue
        if not isinstance(col, str):
            new_column_names.append(col)
            continue
        try:
            cleaned_col = col.lower().replace('.', '').strip()
            if '/' in cleaned_col:
                month_abbr, year_short = cleaned_col.split('/')
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

def extract_and_clean(base_path: Path) -> Dict[str, pd.DataFrame]:
    logging.info(f"Iniciando extração e limpeza do diretório: {base_path}")
    ods_files = list(base_path.glob('*.ods'))
    if not ods_files:
        logging.error(f"Nenhum arquivo .ods encontrado em '{base_path}'. Verifique o caminho.")
        return {}
    cleaned_data = {}
    for file_path in ods_files:
        try:
            service_name = file_path.stem[:3]
            logging.info(f"Processando arquivo: {file_path.name} para o serviço '{service_name}'")
            df = pd.read_excel(file_path, engine='odf', header=8)
            df.dropna(how='all', axis=1, inplace=True)
            df.dropna(how='all', axis=0, inplace=True)
            df.columns = reformat_date_columns(df.columns)
            if service_name not in cleaned_data:
                cleaned_data[service_name] = []
            cleaned_data[service_name].append(df)
        except Exception as e:
            logging.error(f"Falha ao processar o arquivo {file_path.name}: {e}")
    final_dfs = {}
    for service, dfs in cleaned_data.items():
        final_dfs[service] = pd.concat(dfs, ignore_index=True)
        logging.info(f"Serviço '{service}' consolidado com sucesso.")
    return final_dfs


# A função transform permanece EXATAMENTE A MESMA
# ... (cole aqui a sua função transform que já funciona) ...
def transform(data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not data_dict:
        logging.warning("Dicionário de dados vazio. Encerrando a transformação.")
        return pd.DataFrame()
    logging.info("Iniciando a fase de transformação (unpivot).")
    for service_name, df in data_dict.items():
        df['servico'] = service_name
    df_consolidated = pd.concat(data_dict.values(), ignore_index=True)
    id_vars = ['servico', 'GRUPO ECONÔMICO', 'VARIÁVEL']
    value_vars = [col for col in df_consolidated.columns if col not in id_vars]
    df_unpivoted = df_consolidated.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name='data_referencia',
        value_name='valor'
    )
    df_unpivoted['VARIÁVEL'] = df_unpivoted['VARIÁVEL'].str.strip()
    logging.info(f"Filtrando pela métrica alvo: '{METRICA_ALVO}'")
    df_filtered = df_unpivoted[df_unpivoted['VARIÁVEL'] == METRICA_ALVO].copy()
    if df_filtered.empty:
        logging.error(f"A métrica alvo não foi encontrada.")
        return pd.DataFrame()
    df_final = df_filtered.drop(columns=['VARIÁVEL'])
    df_final.rename(columns={'GRUPO ECONÔMICO': 'grupo_economico', 'valor': 'taxa_resolvidas_5_dias'}, inplace=True)
    df_final['data_referencia'] = pd.to_datetime(df_final['data_referencia'], format='%Y-%m')
    df_final['taxa_resolvidas_5_dias'] = pd.to_numeric(df_final['taxa_resolvidas_5_dias'], errors='coerce')
    df_final.dropna(subset=['taxa_resolvidas_5_dias', 'grupo_economico'], inplace=True)
    return df_final[['data_referencia', 'servico', 'grupo_economico', 'taxa_resolvidas_5_dias']]

# --- NOVA FUNÇÃO DE CARGA ---
def load_to_postgres(df: pd.DataFrame, engine):
    """
    Carrega o DataFrame transformado para o Data Mart no PostgreSQL.
    """
    if df.empty:
        logging.warning("DataFrame vazio, nenhuma carga será realizada.")
        return

    logging.info("Iniciando a carga de dados para o PostgreSQL.")
    
    with engine.connect() as conn:
        logging.info("Limpando tabelas existentes para garantir a idempotência.")
        conn.execute(text("TRUNCATE TABLE fato_atendimento, dim_tempo, dim_grupo_economico, dim_servico RESTART IDENTITY CASCADE;"))
        conn.commit()

    # 1. Preparar e carregar Dimensões
    # Dimensão Serviço
    dim_servico_df = pd.DataFrame(df['servico'].unique(), columns=['nome_servico'])
    dim_servico_df.to_sql('dim_servico', engine, if_exists='append', index=False)
    logging.info(f"Carregados {len(dim_servico_df)} registros em dim_servico.")

    # Dimensão Grupo Econômico
    dim_grupo_df = pd.DataFrame(df['grupo_economico'].unique(), columns=['nome_grupo'])
    dim_grupo_df.to_sql('dim_grupo_economico', engine, if_exists='append', index=False)
    logging.info(f"Carregados {len(dim_grupo_df)} registros em dim_grupo_economico.")

    # Dimensão Tempo
    dim_tempo_df = pd.DataFrame({'data_referencia': df['data_referencia'].unique()})
    dim_tempo_df['ano'] = dim_tempo_df['data_referencia'].dt.year
    dim_tempo_df['mes'] = dim_tempo_df['data_referencia'].dt.month
    dim_tempo_df.to_sql('dim_tempo', engine, if_exists='append', index=False)
    logging.info(f"Carregados {len(dim_tempo_df)} registros em dim_tempo.")
    
    # 2. Mapear Chaves Estrangeiras
    logging.info("Mapeando chaves estrangeiras para a tabela fato.")
    
    # Ler as dimensões de volta do banco para obter os IDs gerados
    dim_servico_db = pd.read_sql("SELECT id_servico, nome_servico FROM dim_servico", engine)
    dim_grupo_db = pd.read_sql("SELECT id_grupo, nome_grupo FROM dim_grupo_economico", engine)
    dim_tempo_db = pd.read_sql("SELECT id_tempo, data_referencia FROM dim_tempo", engine)

    # Juntar (merge) para adicionar os IDs ao dataframe principal
    logging.info("Mapeando chaves estrangeiras para a tabela fato.")
    logging.info("Mapeando chaves estrangeiras de serviço e grupo econômico.")
    df = pd.merge(df, dim_servico_db, left_on='servico', right_on='nome_servico', how='left')
    df = pd.merge(df, dim_grupo_db, left_on='grupo_economico', right_on='nome_grupo', how='left')

    logging.info("Convertendo a coluna 'data_referencia' em dim_tempo_db para datetime.")
    dim_tempo_db['data_referencia'] = pd.to_datetime(dim_tempo_db['data_referencia'])
    # --- FIM DA NOVA CORREÇÃO ---


    # --- DEBUGGING ATUALIZADO ---
    # Agora vamos verificar os tipos de AMBOS os dataframes para confirmar que estão iguais.
    logging.info(f"Tipo de dado final em df['data_referencia']: {df['data_referencia'].dtype}")
    logging.info(f"Tipo de dado final em dim_tempo_db['data_referencia']: {dim_tempo_db['data_referencia'].dtype}")

    # Execute o merge
    df = pd.merge(df, dim_tempo_db, on='data_referencia', how='left')
    
    # CORREÇÃO: Renomear as colunas do DataFrame para corresponder ao schema da fato_atendimento
    # E também renomear a coluna da métrica de volta para 'valor'
    df.rename(columns={
        'id_tempo': 'tempo_id',
        'id_servico': 'servico_id',
        'id_grupo': 'grupo_economico_id',
        'taxa_resolvidas_5_dias': 'valor'  # Importante renomear a métrica
    }, inplace=True)

    # Agora, a seleção das colunas para df_fato vai funcionar corretamente
    df_fato = df[['tempo_id', 'servico_id', 'grupo_economico_id', 'valor']].copy()

    # Carrega o DataFrame final na tabela fato_atendimento
    logging.info(f"Carregando {len(df_fato)} registros na tabela fato_atendimento.")
    df_fato.to_sql('fato_atendimento', engine, if_exists='append', index=False)
    logging.info("ETL concluído com sucesso. Dados carregados no PostgreSQL.")


# --- FUNÇÃO MAIN MODIFICADA ---
def main():
    """
    Orquestra o pipeline de ETL completo: Extração, Transformação e Carga.
    """
    logging.info("--- INICIANDO PIPELINE DE ETL PARA DADOS IDA ---")
    
    # 1. Extrair e Limpar
    cleaned_data_frames = extract_and_clean(INPUT_DIR)
    
    # 2. Transformar
    final_data = transform(cleaned_data_frames)

    # 3. Carregar
    if not final_data.empty:
        try:
            # Conexão com o banco de dados usando variáveis de ambiente
            # Ex: postgresql://user:password@host:port/database
            db_user = os.getenv("POSTGRES_USER", "default_user")
            db_password = os.getenv("POSTGRES_PASSWORD", "default_password")
            db_host = os.getenv("POSTGRES_HOST", "localhost")
            db_port = os.getenv("POSTGRES_PORT", "5432")
            db_name = os.getenv("POSTGRES_DB", "default_db")
            
            database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            engine = create_engine(database_url)
            
            load_to_postgres(final_data, engine)
            logging.info("ETL concluído com sucesso. Dados carregados no PostgreSQL.")
            
        except Exception as e:
            logging.error(f"Falha na fase de carga para o banco de dados: {e}")
    else:
        logging.error("O processamento não gerou dados finais. Carga para o banco de dados cancelada.")
        
    logging.info("--- FIM DO PIPELINE ---")


if __name__ == "__main__":
    main()