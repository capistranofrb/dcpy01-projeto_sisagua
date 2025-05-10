from airflow.models import Variable
from sisagua.utils.hdfs_client import get_hdfs_client
import os

def salvar_dados(input_path, **context):
    """Versão adaptada para Airflow com versionamento no HDFS"""
    execution_date = context['execution_date']
    uf = Variable.get("SISAGUA_UF", default_var="CE")
    data_ref = execution_date.strftime('%Y%m')
    
    client = get_hdfs_client()
    
    # Define caminhos no HDFS
    hdfs_raw_path = f"/sisagua/raw/uf={uf}/ano_mes={data_ref}/dados.csv"
    hdfs_processed_path = f"/sisagua/processed/uf={uf}/ano_mes={data_ref}/dados.csv"
    
    # Faz upload para área raw (dados originais)
    client.upload(hdfs_raw_path, input_path)
    
    # Se for o caminho transformado, envia para processed
    if "_transformado" in input_path:
        client.upload(hdfs_processed_path, input_path)
        print(f"✅ Dados salvos em: {hdfs_processed_path}")
        return hdfs_processed_path
    
    print(f"✅ Dados brutos salvos em: {hdfs_raw_path}")
    return hdfs_raw_path