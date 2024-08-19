import os, sys, pandas as pd, requests
if len(sys.argv) > 1: os.chdir(sys.argv[1])
sys.path.append(os.getcwd())

from datetime import datetime, timedelta
from cloud.RDS import RDS, inserir
try:
    from ons.api.autenticar import token
except: 
    from autenticar import token


def arruma_horario_utc(df: pd.DataFrame, data_ref: datetime, data_previsao) -> pd.DataFrame:
    df.data = pd.to_datetime(df.data) + pd.DateOffset(hours=-3)
    df.data = pd.to_datetime(df.data.dt.strftime('%Y-%m-%d %H:%M:%S'))
    data_previsao = datetime.strptime(data_previsao, '%Y-%m-%d').date()
    df.insert(0, 'data_previsao', data_ref)
    return df[df.data.dt.date == data_previsao].reset_index(drop=True)


def arruma_colunas(df: pd.DataFrame) -> pd.DataFrame:
    novos_nomes = {'cod_areacarga': 'area', 'din_referenciautc': 'data', 'val_cargaprogramada': 'carga'}
    return df.drop(columns=['dat_programacao']).rename(columns=novos_nomes)


def carga_prevista_regiao(area: str, headers: dict[str, str], data_ref: datetime, data_previsao: datetime) -> list | dict:
    URL = f'{os.getenv("SINTEGRE_URL")}/cargaglobal/cargap'
    query = f'cod_areacarga={area}&dat_referencia_ini={data_ref.strftime("%Y-%m-%d")}&dat_referencia_fim={data_previsao}'
    url = f'{URL}?{query}'
    resposta = requests.get(url, headers=headers)
    if resposta.status_code == 200: 
        return resposta.json()
              

def carga_prevista(headers: dict[str, str], data_ref: datetime, data_previsao: datetime) -> pd.DataFrame:
    AREAS = ['SECO', 'S', 'N', 'NE']
    resultados = []
    for area in AREAS: resultados.extend(carga_prevista_regiao(area, headers, data_ref, data_previsao))
    return pd.DataFrame(resultados)


def prepara_dados(df: pd.DataFrame, data_ref: datetime, data_previsao: datetime) -> pd.DataFrame:
    df = arruma_colunas(df)
    df = arruma_horario_utc(df, data_ref, data_previsao)
    df.insert(0, 'id', df.area + df.data_previsao.dt.strftime('_%Y%m%d') + df.data.dt.strftime('_%Y%m%d%H%M%S'))
    return df.drop(columns=['data_previsao', 'data', 'area'])


def salva_no_banco(host: str, pssd: str, headers: dict[str, str], data_ref: datetime, data_previsao: datetime) -> None:
    previsao = carga_prevista(headers, data_ref, data_previsao)
    df = prepara_dados(previsao, data_ref, data_previsao)
    if df.shape[0] >= 4:
        rds = RDS('ons', pssd, host)
        inserir(
            data=df, 
            nome_tabela='carga_prevista', 
            rds=rds, 
            transform=False
        )
    else: print(f'Dados de carga prevista insuficientes {datetime.now()} // data_previsao: {data_previsao} // data_referencia: {data_ref}')


def main():
    DATA_REF = datetime.now()
    DATA_PREVISAO = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    HOST = 'database-chuva-vazao-exp.c7c6qwyy8ksr.us-east-1.rds.amazonaws.com'
    TOKEN = token()
    HEADERS = {'accept': 'application/json', 'Authorization': TOKEN}
    salva_no_banco(HOST, os.getenv('db_pssd'), HEADERS, DATA_REF, DATA_PREVISAO)


if __name__ == '__main__': main()