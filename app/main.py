from datetime import datetime
import psycopg2 as pg
import getpass
import pandas as pd
import yaml
import numpy as np
import os
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from definitions import CONFIG_FILE_PATH, DATASET_FOLDER_PATH
from itertools import product


def log(text: str, break_line: bool = True) -> None:
    if break_line:
        print(f"{datetime.now()} - " + text + "\n")
    else:
        print(f"{datetime.now()} - " + text)


#  Parâmetros de configuração para o banco de dados:
FILE_PATH = (
    "/Users/rafaelmacedo/Documents/Code/puc-minas-proj-app-dados/app/config.yaml"
)
with open(CONFIG_FILE_PATH, "r") as file:
    config = yaml.safe_load(file)

HOST = config["database"]["host"]
DBNAME = config["database"]["dbname"]
USER = config["database"]["user"]
PASSWORD = getpass.getpass()
PORT = config["database"]["port"]
CONNECTION_STRING = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"

log("Estabelecendo a conexão com o banco de dados:")
engine = create_engine(CONNECTION_STRING)
if not database_exists(engine.url):
    log(f"Database {DBNAME} não existe...")
    log(f"Criando database {DBNAME}...")
    create_database(engine.url)
    log(f"Database {DBNAME} criado!")

log("Conexão estabelecida!")

#  Lendo os dados:
ano = ["2018", "2020"]
semestre = ["01", "02"]

periodos = list(product(ano, semestre))

#  Gatilho para iniciar a concatenação dos CSVs.
log("Lendo os arquivos CSV para carregar os dados na memória...")
TRIGGER = True

for periodo in periodos:
    TIME_MARK = f"{periodo[0]}-{periodo[1]}"
    log(f"Lendo dataset do período: {TIME_MARK}")
    if TRIGGER:
        df = pd.read_csv(
            os.path.join(DATASET_FOLDER_PATH, config["datasets"][TIME_MARK]),
            header=0,
            sep=";",
        )
        df["periodo"] = TIME_MARK
        TRIGGER = False
    else:
        df_ = pd.read_csv(
            os.path.join(DATASET_FOLDER_PATH, config["datasets"][TIME_MARK]),
            header=0,
            sep=";",
        )
        df_["periodo"] = TIME_MARK
        df = pd.concat([df, df_])

#  Ingestão da tabela não tratada:
log("Fazendo a ingestão dos arquivos CSVs para o PostgreSQL...")

TB_SOR = "dados_combustiveis_sor"
df.to_sql(f"{TB_SOR}", con=engine, if_exists="replace", index=False)

log("Ingestão concluída")
log("Iniciando tratamento dos dados...")

venda = "Valor de Venda"
compra = "Valor de Compra"

log(
    "Substituindo vírgula por ponto na separação decimal dos valores das colunas 'valor de compra' e 'valor de venda'..."
)

df[venda] = df[venda].apply(lambda x: x.replace(",", ".") if type(x) != float else x)
df[compra] = df[venda].apply(lambda x: x.replace(",", ".") if type(x) != float else x)

#  Simplificação dos nomes das colunas para manipulação dos dados
log("Formatando os nomes das colunas...")
df.columns = list(map(lambda x: x.lower(), df.columns))
df.columns = list(
    map(lambda x: x.strip(" - sigla") if ("- sigla" in x) else x, df.columns)
)

log("Convertendo as colunas com os valores de compra e venda para o tipo float...")
df[[col for col in df.columns if col.startswith("valor")]] = df[
    [col for col in df.columns if col.startswith("valor")]
].astype(np.float32)

#  Selecionando as colunas de interesse:
log(
    "Selecionando as colunas de interesse: 'regiao', 'estado', 'revenda', 'produto', 'valor de venda', 'bandeira', 'periodo'..."
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
    "bandeira",
    "periodo",
]

df = df.loc[:, COLUMNS]

log("Ingerindo os dados tratados e harmonizados para o database...")
TB_SOT = "dados_combustiveis_sot"
df.to_sql(f"{TB_SOT}", con=engine, if_exists="replace", index=False)

log("Camada SoT ingerida ingeridos")

log("Iniciando agregação dos dados...")
tabela_agregada = (
    df.groupby(["regiao", "estado", "produto", "bandeira", "periodo"])["valor de venda"]
    .mean()
    .reset_index()
)

log(f"Ingerindo tabela especializada no database {DBNAME}...")
TB_SPEC = "dados_combustiveis_spec"
df.to_sql(f"{TB_SPEC}", con=engine, if_exists="replace", index=False)
