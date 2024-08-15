from pandas import DataFrame
from graficos.configs import *
from typing import Literal
import plotly.graph_objects as go


class Visualizacao:
    """
    Padroniza a criacao dos graficos para o dashboard
    """
    def __init__(self, cor_de_fundo_figura: str='rgba(0, 0, 0, 0)', figsize: tuple[int, int]=(800, 600), hovertemplate: str='', extra_args: dict={}) -> None:
        self.extra_args_customizacao = extra_args
        self.hovertemplate = hovertemplate
        self.inicializa_figura(cor_de_fundo_figura, figsize)

    def customizacoes_figura(self, cor_de_fundo_figura, figsize: tuple[int, int], font_family: str='Montserrat') -> go.Layout:
        return go.Layout(
            autosize=False,
            paper_bgcolor=cor_de_fundo_figura,
            plot_bgcolor=cor_de_fundo_figura,
            font={'family': font_family, 'size': 15},
            width=figsize[0], height=figsize[1],
            yaxis=LINHA_DE_GRADE,
            margin=MARGEM,
            hoverlabel={'font': {'family': 'Montserrat Semibold'}},
            hovermode='x unified',
            **self.extra_args_customizacao
        )

    def inicializa_figura(self, cor_de_fundo_figura: str, figsize: tuple[int, int]): 
        layout = self.customizacoes_figura(cor_de_fundo_figura, figsize)
        self.figura = go.Figure(layout=layout)

    def inicializa_grafico(self, dados: DataFrame, colunas: list[str]): pass

    def titulo(self, texto: str, bold: bool=True, font_family: str='', font_size: int=30, posicao_x: float=0.5, posicao_y: float=0.975):
        if bold: texto = f'<b>{texto}</b>\n'
        if not font_family: font_family = 'sans-serif'

        self.figura.update_layout(
            title=texto,
            title_x=posicao_x, title_y=posicao_y,
            titlefont={'size': font_size, 'family': font_family},
            
        )

    def titulo_eixo(self, texto: str, eixo: Literal['x', 'y'], bold: bool=False, font_family: str=''):
        if bold: texto = f'<b>{texto}</b>'
        args = {f'{eixo}axis_title': texto, f'{eixo}axis_title_font_family': font_family}
        self.figura.update_layout(**args)
            
    def plot(self, dados: DataFrame, colunas: list[str], show: bool=True):
        self.inicializa_grafico(dados, colunas)
        self.figura.add_traces(self.graficos)
        if show: self.figura.show()
        return self.figura

