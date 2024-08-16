import os, sys
import pandas as pd
from datetime import datetime
cwd = os.getcwd()
os.chdir('../chuva-vazao')
sys.path.append(os.getcwd())
from cloud.RDS import RDS
os.chdir(cwd)


def arruma_data_range(df_previsao: pd.DataFrame) -> pd.DataFrame:
    novo_index = pd.date_range(start=datetime.now().strftime('%Y-%m-%d 00:00:00'), end=datetime.now().strftime('%Y-%m-%d 23:59:00'), freq='10min')
    df_previsao = df_previsao.reindex(novo_index).ffill().dropna()
    df_previsao.index.name = 'data'
    return df_previsao

def arruma_colunas(df_previsao: pd.DataFrame) -> pd.DataFrame:
    df_previsao.data = df_previsao.data.dt.tz_convert(None)
    df_previsao = df_previsao.pivot(index='data', columns='regiao', values='carga')
    df_previsao.columns = [f'{x.lower()}_carga_prevista' for x in df_previsao.columns]
    df_previsao.insert(0, 'sin_carga_prevista', df_previsao.sum(axis=1))
    return df_previsao


def salva_parquet(df_previsao: pd.DataFrame): 
    condicao_data = (max(df_previsao.index.date) == datetime.now().date()) and (min(df_previsao.index.date) == datetime.now().date())
    condicao_quantidade_de_dados = df_previsao.shape[0] > 100
    if condicao_quantidade_de_dados and condicao_data:
        try:
            df_previsao.to_parquet('data/silver/carga_previsao.parquet')
        except: print(f'[processamento/carga_prevista.py] {datetime.now().strftime("%Y-%m-%d %H:%M")} Nao foi possivel salvar o arquivo de previsao de carga caminho atual = ({os.getcwd()})')


def main():    
    QUERY_PREVISAO = """
    SELECT carga, TO_TIMESTAMP(SUBSTRING(id FROM '(\d{14})$'), 'YYYYMMDDHH24MISS') AS data, SPLIT_PART(id, '_', 1) as regiao FROM carga_prevista;
    """
    rds = RDS('ons', os.getenv('db_pssd'), host='database-chuva-vazao-exp.c7c6qwyy8ksr.us-east-1.rds.amazonaws.com')
    df_previsao = rds.read(QUERY_PREVISAO)
    df_previsao = arruma_colunas(df_previsao)
    df_previsao = arruma_data_range(df_previsao)
    salva_parquet(df_previsao)