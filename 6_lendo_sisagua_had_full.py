from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from hdfs import InsecureClient
import pandas as pd
import requests
import logging
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time

# Configuração HDFS
hdfs_url = 'http://master:9870'
hdfs_user = 'alexandre'

# Função para extrair e salvar dados por ano
def extrair_e_salvar_por_ano(**kwargs):
    headers = {"accept": "application/json"}
    uf = "CE"
    ano_atual = datetime.now().year

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Conectar ao HDFS
    client = InsecureClient(hdfs_url, user=hdfs_user)

    for ano in range(2020, ano_atual + 1):
        offset = 0
        limit = 300
        dados = []

        logging.info(f"Iniciando extração para o ano {ano}...")

        while True:
            params = {"uf": uf, "ano": str(ano), "limit": limit, "offset": offset}
            response = session.get("https://apidadosabertos.saude.gov.br/sisagua/vigilancia-parametros-basicos",
                                   headers=headers, params=params, timeout=30)
            if response.status_code != 200:
                logging.error(f"Erro {response.status_code} ao buscar dados do ano {ano}")
                break

            registros = response.json().get("parametros", [])
            if not registros:
                break

            dados.extend(registros)
            offset += limit
            time.sleep(0.5)

        if not dados:
            logging.warning(f"Nenhum dado encontrado para o ano {ano}")
            continue

        # Transformar em DataFrame
        df = pd.DataFrame(dados)

        # Montar caminho HDFS com o ano no nome do arquivo
        data_execucao = datetime.now().strftime('%Y_%m_%d')
        hdfs_path = f'sisagua/dados_{ano}_{data_execucao}.csv'

        # Escrever no HDFS
        with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            df.to_csv(writer, index=False)

        logging.info(f"Arquivo para {ano} salvo no HDFS em: {hdfs_path}")

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG
with DAG(
    dag_id='sisagua_extrair_por_ano',
    default_args=default_args,
    description='Extrai dados da API do SISAGUA por ano e salva no HDFS',
    schedule_interval='0 3 30 * *',  # Executa no dia 30 de cada mês às 03h
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=['sisagua'],
) as dag:

    task_extrair_e_salvar = PythonOperator(
        task_id='extrair_e_salvar_por_ano',
        python_callable=extrair_e_salvar_por_ano
    )
