import os, sys
import pandas as pd
from datetime import datetime

cwd = os.getcwd()
os.chdir('../chuva-vazao')
sys.path.append(os.getcwd())

from cloud.RDS import RDS

os.chdir(cwd)
REGIOES = ['seco', 's', 'n', 'ne']
FONTES = ['eolica', 'hidraulica', 'nuclear', 'solar', 'termica']


def arruma_minutos(dt: datetime) -> datetime:
    rounded_minutes = round(dt.minute, -1)
    if rounded_minutes == 60:
        dt = dt + pd.DateOffset(minutes=60 - dt.minute)
        return dt.replace(minute=0, second=0, microsecond=0)
    return dt.replace(minute=rounded_minutes, second=0, microsecond=0)


def insere_carga_liq(df: pd.DataFrame) -> pd.DataFrame:
    df.insert(0,'sin_carga_liq', df.sin_carga_verif - df[['sin_g_eolica', 'sin_g_solar']].sum(axis=1))
    for regiao in REGIOES: 
        df.insert(
            0, 
            f'{regiao}_carga_liq', 
            df[f'{regiao}_carga_verif'] - df[[f'{regiao}_g_eolica', f'{regiao}_g_solar']].sum(axis=1)
        )
    return df


def insere_carga_total(df: pd.DataFrame) -> pd.DataFrame:
    df.insert(0, 'sin_carga_verif', df[[x for x in df.columns if '_carga_verif' in x]].sum(axis=1))
    return df


def insere_geracao_total(df: pd.DataFrame) -> pd.DataFrame:
    df = insere_geracao_sin_por_fonte(df.copy())
    df['sin_g_total'] = df[[x for x in df.columns if '_g_total' in x]].sum(axis=1)
    return df


def insere_geracao_sin_por_fonte(df: pd.DataFrame) -> pd.DataFrame:
    for fonte in FONTES:
        df[f'sin_g_{fonte}'] = df[[x for x in df.columns if f'_g_{fonte}' in x]].sum(axis=1)
        if fonte == 'hidraulica': df[f'sin_g_{fonte}'] += df[[x for x in df.columns if ('itaipu50hzbrasil' in x) or ('itaipu60hz' in x)]].sum(axis=1)
    return df


def insere_totais(df: pd.DataFrame) -> pd.DataFrame:
    df = insere_geracao_total(df.copy())
    return insere_carga_total(df.copy())


def salva_parquet(df: pd.DataFrame): 
    condicao_data_index = (df.index.date.min() == df.index.date.max())
    condicao_data = condicao_data_index and (df.index.date.min() == datetime.now().date())
    if condicao_data: df.to_parquet('data/bronze/carga_agora.parquet')
    

def main():
    QUERY = """SELECT * FROM acompanhamento_carga_e_geracao WHERE DATE_TRUNC('day',  data AT TIME ZONE 'America/Sao_Paulo') = CURRENT_DATE ORDER BY data ASC;"""
    rds = RDS('ons', os.getenv('db_pssd'), host='database-chuva-vazao-exp.c7c6qwyy8ksr.us-east-1.rds.amazonaws.com')
    df = rds.read(QUERY)
    df.data = pd.to_datetime(df.data.dt.tz_convert('Etc/GMT+3').dt.strftime('%Y-%m-%d %H:%M')).apply(arruma_minutos)
    df = insere_totais(df.loc[df.data <= datetime.now()].set_index('data'))
    salva_parquet(insere_carga_liq(df))
