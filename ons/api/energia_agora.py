from autenticar import token
import os, requests


def carga_e_geracao_agora():
    TOKEN = token()
    URL = f'{os.getenv("SINTEGRE_URL")}/energiaagora/GetBalancoEnergetico/null'
    HEADER = {'accept': 'application/json', 'Authorization': TOKEN}
    return requests.get(URL, headers=HEADER).json()