import pandas as pd
import os
from datetime import datetime
from airflow.models import Variable

def tratar_dados(input_path, **context):
    """Versão adaptada para Airflow com metadados"""
    execution_date = context['execution_date']
    uf = Variable.get("SISAGUA_UF", default_var="CE")
    data_ref = execution_date.strftime('%Y%m')
    
    # Carrega os dados
    df = pd.read_csv(input_path)
    
    # Transformações (adaptar conforme necessidade)
    df['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['ano_mes_referencia'] = data_ref
    df['uf'] = uf
    
    # Adiciona metadados como colunas
    df['pipeline_version'] = '1.0'
    df['airflow_run_id'] = context['run_id']
    
    # Remove duplicatas (exemplo)
    df = df.drop_duplicates()
    
    # Salva os dados transformados
    output_path = f"/tmp/sisagua_{uf}_{data_ref}_transformado.csv"
    df.to_csv(output_path, index=False)
    
    return output_path