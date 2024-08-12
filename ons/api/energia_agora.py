"""
Coleta da situacao energetica instantanea
Todos os valores da API estao em MW
"""

import os, sys
os.chdir(sys.argv[1])
sys.path.append(os.getcwd())
from cloud.RDS import RDS, inserir
import pandas as pd
from autenticar import token
import os, requests


def carga_e_geracao_agora():
    TOKEN = token()
    URL = f'{os.getenv("SINTEGRE_URL")}/energiaagora/GetBalancoEnergetico/null'
    HEADER = {'accept': 'application/json', 'Authorization': TOKEN}
    return requests.get(URL, headers=HEADER).json()


def prepara_dados(content: dict):
    df = pd.json_normalize(content)
    df.columns = [x.replace('.', '_').lower().replace('_geracao_', '_g_').replace('cargaverificada', 'carga_verif').replace('sudesteecentrooeste', 'seco').replace('nordeste', 'ne').replace('norte', 'n').replace('sul', 's') for x in df.columns]
    df.data = pd.to_datetime(df.data)
    return df


def salva_no_banco(host: str, pssd: str):
    energia_agora = carga_e_geracao_agora()
    df = prepara_dados(energia_agora)
    rds = RDS('ons', pssd, host)
    inserir(
        data=df, 
        nome_tabela='acompanhamento_carga_e_geracao', 
        rds=rds, 
        transform=False
    )


if __name__ == '__main__': 
    HOST = 'database-chuva-vazao-exp.c7c6qwyy8ksr.us-east-1.rds.amazonaws.com'
    salva_no_banco(HOST, os.getenv('db_pssd'))