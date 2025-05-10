import os
import pandas as pd
import requests
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from airflow.models import Variable

def extrair_dados(execution_date, **context):
    """Versão adaptada para Airflow com checkpoint no HDFS"""
    uf = Variable.get("SISAGUA_UF", default_var="CE")
    ano = execution_date.strftime('%Y')
    data_ref = execution_date.strftime('%Y%m')
    
    url = "https://apidadosabertos.saude.gov.br/sisagua/vigilancia-parametros-basicos"
    headers = {"accept": "application/json"}
    
    # Configuração do cliente HDFS
    from sisagua.utils.hdfs_client import get_hdfs_client
    hdfs_client = get_hdfs_client()
    hdfs_checkpoint = f"/sisagua/checkpoints/{uf}_{data_ref}_checkpoint.csv"
    
    # Tenta resumir de checkpoint no HDFS
    try:
        if hdfs_client.status(hdfs_checkpoint, strict=False):
            with hdfs_client.read(hdfs_checkpoint) as reader:
                dados = pd.read_csv(reader).to_dict('records')
            offset = len(dados)
            print(f"🔁 Retomando do checkpoint: {offset} registros")
        else:
            dados = []
            offset = 0
            print("🔄 Iniciando nova extração")
    except Exception as e:
        print(f"⚠️ Erro ao verificar checkpoint: {e}")
        dados = []
        offset = 0

    # Configuração de resiliência
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))

    # Loop de extração
    while True:
        try:
            params = {"uf": uf, "ano": ano, "limit": 300, "offset": offset}
            response = session.get(url, headers=headers, params=params, timeout=30)

            if response.status_code != 200:
                raise Exception(f"Erro HTTP {response.status_code}")

            registros = response.json().get("parametros", [])
            if not registros:
                print("✅ Extração concluída")
                break

            dados.extend(registros)
            offset += 300

            # Checkpoint a cada 3000 registros
            if offset % 3000 == 0:
                temp_file = f"/tmp/sisagua_checkpoint_{offset}.csv"
                pd.DataFrame(dados).to_csv(temp_file, index=False)
                hdfs_client.upload(hdfs_checkpoint, temp_file)
                os.remove(temp_file)
                print(f"💾 Checkpoint salvo no HDFS: offset {offset}")

            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Falha na extração: {e}")
            raise

    # Salva os dados completos temporariamente
    output_path = f"/tmp/sisagua_{uf}_{data_ref}.csv"
    pd.DataFrame(dados).to_csv(output_path, index=False)
    
    return output_path