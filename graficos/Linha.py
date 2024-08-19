from plotly.graph_objects import Figure
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


class PLD(Visualizacao):
    def inicializa_grafico(self, dados: DataFrame, colunas: list[str]):
        self.graficos = [go.Scatter(
            x=dados.index,
            y=dados[coluna],
            name=coluna,
            marker_color=PALETA[::-1][colunas.index(coluna)],
            hovertemplate=self.hovertemplate,
            line={'dash': 'dash'} if colunas.index(coluna) == 1 else None ,
            yaxis='y2',
            fill='tozeroy' if colunas.index(coluna) == 0 else None
        ) for coluna in colunas]

    def plot(self, dados: DataFrame, colunas: list[str], show: bool=False, nomes:list[str]=[]):
        if (len(nomes) != len(colunas)): 
            warn('Tamanho da lista de Nomes != Colunas, nome original das colunas sera utilizado')
            nomes = colunas
        dados.rename(columns=dict(zip(colunas, nomes)), inplace=True)
        return super().plot(dados, nomes, show)


    def plot_sec(self, dados: DataFrame, colunas: list[str], fig: Figure, nomes:list[str]=[]):
        if (len(nomes) != len(colunas)): 
            warn('Tamanho da lista de Nomes != Colunas, nome original das colunas sera utilizado')
            nomes = colunas
        dados.rename(columns=dict(zip(colunas, nomes)), inplace=True)
        self.inicializa_grafico(dados, nomes)
        fig.add_traces(self.graficos)
        fig.update_layout(
            yaxis2={'overlaying': 'y', 'side': 'right', 'showgrid': False},
            legend={'x': 0.5, 'y': -0.2, 'xanchor': 'center', 'yanchor': 'top'},
            legend_orientation='h'
        )
        
        return fig