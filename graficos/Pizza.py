from graficos.Grafico import Visualizacao
import plotly.graph_objects as go
from graficos.configs import * 
from pandas import DataFrame

class Geracao(Visualizacao):
    def inicializa_grafico(self, dados: DataFrame):
        regiao = dados.columns[1]
        labels = dados.columns[0]
        self.graficos = [go.Pie(
            labels=dados[labels], values=dados[regiao],
            hole=0.65, name='',
            hovertemplate=self.hovertemplate,
            marker_colors=PALETA[:dados[labels].unique().shape[0]]
        )]
    
    def plot(self, dados: DataFrame, show: bool=True):
        self.inicializa_grafico(dados)
        self.figura.add_traces(self.graficos)
        if show: self.figura.show()
        return self.figura