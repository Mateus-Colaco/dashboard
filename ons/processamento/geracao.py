import os
import pandas as pd


class DadosGoldGeracao:
    """
    Classe Base para criar os dados de geracao
    """
    def __init__(self, caminho: str='data/silver', caminho_dest: str='data/gold'):
        self.caminho = caminho
        self.caminho_dest = caminho_dest
        self.df = pd.read_parquet(f'{caminho}/carga_agora.parquet')
        
    def checa_pastas(self):
        os.makedirs(self.caminho, exist_ok=True)
        os.makedirs(f'{self.caminho_dest}/geracao', exist_ok=True)

    def inicializa_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        - Filtra somente as colunas com dados referentes a geracao
        - 'Unpivot' a tabela para 3 colunas [data, categoria, valor]
        - Separa a coluna categoria em 3 novas colunas com a regiao, tipo (geracao) e descr (fonte de energia, ou total)
        - Ordena os valores com base nas colunas de data, regiao e descricao e remove as colunas variable (categoria) e tipo (pois tem somente um valor, 'g')
        """
        regiao_colunas = list(filter(lambda x: (x.split('_')[0] in ['seco', 's', 'ne', 'n', 'sin']) and ('_g_' in x), df.columns))
        df_geracao_regiao = df[regiao_colunas].reset_index().melt(id_vars='data')
        df_geracao_regiao[['regiao', 'tipo', 'descr']] = df_geracao_regiao.variable.str.split('_', expand=True)
        df_geracao_regiao = df_geracao_regiao.sort_values(['data', 'regiao', 'descr']).drop(columns=['variable', 'tipo']).reset_index(drop=True)
        return df_geracao_regiao

    def gera_dados_dashboard(self, coluna: str, novos_nomes: dict[str, str], coluna_grupo: str):
        self.checa_pastas()
        df_geracao_categoria = self.inicializa_dataframe(self.df.copy())
        categorias = df_geracao_categoria[coluna].str.upper().unique().tolist()
        for categoria in categorias: self.salva_dados(categoria, df_geracao_categoria, coluna, novos_nomes, coluna_grupo)

    def salva_dados(self, categoria: str, df_geracao_categoria: pd.DataFrame, coluna: str, novos_nomes: dict[str, str], coluna_grupo: str):
        """
        Salva os dados de geracao por fonte da regiao especificada
        """
        nome = novos_nomes.get(categoria, categoria)
        df_geracao: pd.DataFrame = df_geracao_categoria.loc[(df_geracao_categoria[coluna] == categoria.lower()) & (df_geracao_categoria.descr != 'total')]
        df_geracao = (df_geracao.groupby(coluna_grupo).sum(numeric_only=True) / 1000).reset_index()
        df_geracao.columns = ['FONTE', nome]
        df_geracao.to_parquet(f'{self.caminho_dest}/geracao/grafico_fontes_{nome}.parquet', index=False)

class ByRegiao(DadosGoldGeracao):
    novos_nomes = {'N': 'NORTE', 'SECO': 'SUDESTE e CENTRO OESTE', 'S': 'SUL', 'NE': 'NORDESTE'}
    """
    Gera os dados para criacao do grafico de pizza para cada regiao
    Labels = Fonte de Geracao
    Values = Quantidade Gerada (MW)
    """
    def gera_dados_dashboard(self, coluna='regiao'):
        super().gera_dados_dashboard(coluna, self.novos_nomes, 'descr')


class ByFonte(DadosGoldGeracao):
    novos_nomes = {'eolica': 'EOLICA', 'hidraulica': 'HIDRAULICA', 'nuclear': 'NUCLEAR', 'solar': 'SOLAR', 'termica': 'TERMICA'}
    """
    Gera os dados para criacao do grafico de pizza para cada fonte
    Labels = Regiao
    Values = Quantidade Gerada (MW)
    """
    def gera_dados_dashboard(self, coluna='descr'):
        super().gera_dados_dashboard(coluna, self.novos_nomes, 'regiao')
