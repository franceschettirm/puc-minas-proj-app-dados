from datetime import datetime
import psycopg2 as pg
import getpass
import pandas as pd
import yaml
import numpy as np
from sqlalchemy import create_engine
from itertools import product


def log(text: str, break_line: bool = True) -> None:
    if break_line:
        print(f"{datetime.now()} - " + text + "\n")
    else:
        print(f"{datetime.now()} - " + text)


#  Parâmetros de configuração para o banco de dados:
FILE_PATH = "D:\Merovingian\Data Projects\PUC\config.yaml"
with open(FILE_PATH, "r") as file:
    config = yaml.safe_load(file)

HOST = config["database"]["host"]
DBNAME = config["database"]["dbname"]
USER = config["database"]["user"]
PASSWORD = "Sholmes@159943"  # getpass.getpass()
PORT = config["database"]["port"]
CONNECTION_STRING = f"postgresql://postgres:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"

log("Estabelecendo a conexão com o banco de dados:")
engine = create_engine(CONNECTION_STRING)
log("Conexão estabelecida!")

#  Lendo os dados:
ano = ["2018", "2020"]
semestre = ["01", "02"]

periodos = list(product(ano, semestre))

#  Gatilho para iniciar a concatenação dos CSVs.
log("Lendo os arquivos CSV para carregar os dados na memória:")
TRIGGER = True

for periodo in periodos:
    TIME_MARK = f"{periodo[0]}-{periodo[1]}"
    print("Lendo dataset do período: ", TIME_MARK)
    if TRIGGER:
        df = pd.read_csv(config["datasets"][TIME_MARK], header=0, sep=";")
        df["periodo"] = TIME_MARK
        TRIGGER = False
    else:
        df_ = pd.read_csv(config["datasets"][TIME_MARK], header=0, sep=";")
        df_["periodo"] = TIME_MARK
        df = pd.concat([df, df_])

#  Ingestão da tabela não tratada:
log("Fazendo a ingestão dos arquivos CSVs para o PostgreSQL:")

TB_SOR = "dados_combustiveis_sor"
df.to_sql(f"{TB_SOR}", con=engine, if_exists="replace")

log("Ingestão concluída")
log("Iniciando tratamento dos dados:")

venda = "Valor de Venda"
compra = "Valor de Compra"

log(
    "Substituindo vírgula por ponto na separação decimal dos valores das colunas 'valor de compra' e 'valor de venda':"
)

df[venda] = df[venda].apply(lambda x: x.replace(",", ".") if type(x) != float else x)
df[compra] = df[venda].apply(lambda x: x.replace(",", ".") if type(x) != float else x)

log("Convertendo as colunas para o tipo float:")

#  Simplificação dos nomes das colunas para manipulação dos dados
log("Formatando os nomes das colunas:")
df.columns = list(map(lambda x: x.lower(), df.columns))
df.columns = list(
    map(lambda x: x.strip(" - sigla") if ("- sigla" in x) else x, df.columns)
)


#  Selecionando as colunas de interesse:
log(
    "Selecionando as colunas de interesse: 'regiao', 'estado', 'revenda', 'produto', 'valor de venda', 'bandeira', 'periodo'"
)
log(
    "A coluna 'valor de compra' não foi incluída por ter aproximadamente 70% de valores nulos"
)
COLUMNS = [
    "regiao",
    "estado",
    "revenda",
    "produto",
    "valor de venda",
    "valor de compra",
    "bandeira",
    "periodo",
]

df = df.loc[:, COLUMNS]

log("Ingerindo os dados tratados e harmonizados para o database:")
TB_SOT = "dados_combustiveis_sot"
df.to_sql(f"{TB_SOT}", con=engine, if_exists="replace")

log("Dados ingeridos")
