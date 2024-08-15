from graficos.configs import *
from graficos.Grafico import Visualizacao
from pandas import DataFrame
import plotly.graph_objects as go
from warnings import warn


class Carga(Visualizacao):
    def inicializa_grafico(self, dados: DataFrame, colunas: list[str]):
        self.graficos = [
            go.Scatter(
                x=dados.index, 
                y=dados[coluna], 
                name=coluna, 
                marker_color=PALETA[colunas.index(coluna)],
                hovertemplate=self.hovertemplate
            ) 
            for coluna in colunas
        ]

    def plot(self, dados: DataFrame, colunas: list[str], show: bool=True, nomes: list[str]=[]):
        if (len(nomes) != len(colunas)): 
            warn('Tamanho da lista de Nomes != Colunas, nome original das colunas sera utilizado')
            nomes = colunas
        dados.rename(columns=dict(zip(colunas, nomes)), inplace=True)

        return super().plot(dados, nomes, show)
