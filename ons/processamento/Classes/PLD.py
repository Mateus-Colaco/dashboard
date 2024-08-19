import boto3
from datetime import datetime, date
import sys, os
cwd = os.getcwd()
os.chdir('../chuva-vazao')
sys.path.append('../chuva-vazao')
from cloud.S3 import ObjectS3
os.chdir(cwd)
import boto3.resources
import pandas as pd
from io import BytesIO

def arruma_data_range(df: pd.DataFrame) -> pd.DataFrame:
    novo_index = pd.date_range(start=datetime.now().strftime('%Y-%m-%d 00:00:00'), end=datetime.now().strftime('%Y-%m-%d 23:59:00'), freq='10min')
    df = df.reindex(novo_index).ffill().dropna()
    df.index.name = 'data'
    return df


def formata_data(hora: int) -> datetime:
    return datetime.now().replace(
        hour=int(hora), 
        minute=0, 
        second=0, 
        microsecond=0
    )


class DadosGoldPLD:
    colunas_a_remover = ['media_diaria', 'max_diaria', 'min_diaria']
    bucket = 'dados-gerais'
    pasta = 'pld'
    
    def __init__(self, hoje: datetime=datetime.now()) -> None:
        self.hoje = hoje.date()
        self.data = hoje.strftime('%Y%m%d')
        self.arquivo = f'{self.data}_PLD.csv'
        self.inicializa_conexao_s3()

    def inicializa_conexao_s3(self):
        self.conn = boto3.resource('s3')
        self.objeto = ObjectS3(self.conn, self.bucket, f'{self.pasta}/{self.arquivo}')

    def inicializa_arquivo(self) -> pd.DataFrame:
        obj = BytesIO(self.objeto.get())
        df = pd.read_csv(obj, sep=';', parse_dates=['Unnamed: 0'])
        coluna_data = df[df.columns[0]].dt.date
        df.drop(columns=self.colunas_a_remover, inplace=True)
        return df, coluna_data

    def gera_dados_dashboard(self): 
        df, coluna_data = self.inicializa_arquivo()
        self.transforma_arquivo(df, coluna_data)

    def transforma_arquivo(self, df: pd.DataFrame, coluna_data: pd.Series) -> pd.DataFrame:
        df = df.loc[coluna_data == self.hoje].reset_index(drop=True)
        df = df.melt(id_vars=df.columns[0]).rename(columns={'value': 'pld'}).drop(columns=df.columns[0])
        df.variable = df.variable.apply(lambda x: formata_data(int(x)))
        self.dados = arruma_data_range(df.set_index('variable'))
        self.dados['Media'] = self.dados['pld'].mean()
        return self.dados

    


