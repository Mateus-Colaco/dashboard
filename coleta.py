from datetime import datetime
import shutil, os, itertools
import pandas as pd
from ons.api import carga_prevista as api_carga_prevista, energia_agora as api_energia_agora
from ons.processamento import carga_prevista as proc_carga_prevista, energia_agora as proc_energia_agora 

cwd = os.path.abspath(os.path.dirname(__file__))


def carga_prevista():
    execucao = datetime.now()
    manha = datetime.now().replace(hour=13, minute=0, second=0, microsecond=0)
    noite = datetime.now().replace(hour=22, minute=45, second=0, microsecond=0)
    if not((execucao > manha) and (execucao < noite)) or (not os.path.exists('./data/silver/carga_previsao.parquet')):
        api_carga_prevista.main()
        proc_carga_prevista.main()
        print(f"[coleta.py] {datetime.now().strftime('%Y-%m-%d %H:%M')} 'Carga Prevista atualizada.'")


def encontra_todos_pycache(root_dir):
    return list(
                filter(
                    lambda x: x.endswith('__pycache__'), 
                    itertools.chain.from_iterable(
                        [os.path.join(path, file) for file in files] + [os.path.join(path, p) for p in dirs]
                        for path, dirs, files in os.walk(root_dir) 
                    )
                )
            )


def energia_agora():
    try:
        api_energia_agora.main()
        proc_energia_agora.main()
    except Exception as err:
        print(f'Erro ao atualizar dados energeticos atuais: {err}')


def remove_pycache():
    paths = encontra_todos_pycache(os.getcwd())
    for path in paths:
        if '__pycache__' in path: shutil.rmtree(path, ignore_errors=True)


def dados_carga_para_dashboard():
    df = pd.read_parquet('data/silver/carga_agora.parquet')
    df_previsao = pd.read_parquet('data/silver/carga_previsao.parquet')
    df_join = df_previsao.join(df)
    df_join.to_parquet('data/gold/carga_e_geracao.parquet')
    return df_join

def main():
    os.chdir(cwd)
    remove_pycache()
    carga_prevista()
    energia_agora()
    dados_carga_para_dashboard()
    

if __name__ == '__main__': main()