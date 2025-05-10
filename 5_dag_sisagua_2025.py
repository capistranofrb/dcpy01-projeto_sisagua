from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import time
from hdfs import InsecureClient
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configurações do HDFS
HDFS_URL = "http://master:9870"
HDFS_USER = "maxmoreira"
HDFS_DIR = "/dadostrabalhocd/sisagua/2025"
client = InsecureClient(HDFS_URL, user=HDFS_USER)

def extrair_dados(**kwargs):
    url = "https://apidadosabertos.saude.gov.br/sisagua/vigilancia-parametros-basicos"
    headers = {"accept": "application/json"}
    offset = 0
    limit = 300
    dados = []

    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    while True:
        params = {"uf": "CE", "ano": "2025", "limit": limit, "offset": offset}
        response = session.get(url, headers=headers, params=params, timeout=30)
        if response.status_code != 200:
            break
        registros = response.json().get("parametros", [])
        if not registros:
            break
        dados.extend(registros)
        offset += limit
        time.sleep(0.5)

    kwargs['ti'].xcom_push(key='dados_extraidos', value=dados)

def transformar_dados(**kwargs):
    dados = kwargs['ti'].xcom_pull(key='dados_extraidos', task_ids='extrair_dados')
    df = pd.json_normalize(dados)
    colunas = ['municipio', 'ano', 'tipo_da_forma_de_abastecimento', 'mes',
               'parametro', 'data_da_coleta', 'resultado']
    df = df[colunas].copy()
    df = df[df['tipo_da_forma_de_abastecimento'] == 'SAA']
    df['resultado_num'] = pd.to_numeric(df['resultado'].str.replace(',', '.'), errors='coerce')

    def classificar_conformidade(row):
        param = row['parametro'].strip().upper()
        valor = row['resultado'].strip().upper()
        num = row['resultado_num']
        try:
            if param == 'TURBIDEZ (UT)': return 'CONFORME' if num < 5 else 'NÃO CONFORME'
            elif param == 'ESCHERICHIA COLI': return 'CONFORME' if valor == 'AUSENTE' else 'NÃO CONFORME'
            elif param == 'COLIFORMES TOTAIS': return 'CONFORME' if valor == 'AUSENTE' else 'NÃO CONFORME'
            elif param == 'CLORO RESIDUAL LIVRE (MG/L)': return 'CONFORME' if 0.2 <= num <= 5.0 else 'NÃO CONFORME'
            elif param == 'PH': return 'CONFORME' if 6 <= num <= 9.5 else 'NÃO CONFORME'
            elif param == 'COR APARENTE (UH)': return 'CONFORME' if num < 15 else 'NÃO CONFORME'
            elif param == 'FLUORETO (MG/L)': return 'CONFORME' if num <= 1.5 else 'NÃO CONFORME'
        except:
            return 'ERRO'
        return 'IGNORADO'

    def tipo_analise(param):
        p = param.strip().upper()
        if p in ['COLIFORMES TOTAIS', 'ESCHERICHIA COLI']: return 'Microbiológica'
        if p in ['TURBIDEZ (UT)', 'CLORO RESIDUAL LIVRE (MG/L)', 'PH', 'COR APARENTE (UH)', 'FLUORETO (MG/L)']: return 'Físico-Química'
        return 'Outros'

    df['conformidade'] = df.apply(classificar_conformidade, axis=1)
    df['tipo_analise'] = df['parametro'].apply(tipo_analise)

    kwargs['ti'].xcom_push(key='df_tratado', value=df.to_json(orient='records'))

def salvar_dados(**kwargs):
    df_json = kwargs['ti'].xcom_pull(key='df_tratado', task_ids='transformar_dados')
    df = pd.read_json(df_json, orient='records')

    hoje = datetime.today().strftime("%Y%m%d")
    nome_arquivo = f"sisagua_ce_2025_{hoje}.csv"
    caminho = f"{HDFS_DIR}/{nome_arquivo}"

    with client.write(caminho, encoding='utf-8', overwrite=True) as writer:
        df.to_csv(writer, index=False)
    print(f"✅ Arquivo salvo no HDFS: {caminho}")

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    dag_id='sisagua_pipeline_2025',
    default_args=default_args,
    description='Pipeline SISAGUA 2025 com tarefas separadas',
    schedule_interval='@daily',
    start_date=datetime(2025, 4, 24),
    catchup=False
) as dag:

    task_extrair = PythonOperator(
        task_id='extrair_dados',
        python_callable=extrair_dados
    )

    task_transformar = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados
    )

    task_salvar = PythonOperator(
        task_id='salvar_dados_hdfs',
        python_callable=salvar_dados
    )

    task_extrair >> task_transformar >> task_salvar