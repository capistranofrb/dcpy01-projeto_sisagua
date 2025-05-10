from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {
    'owner': 'alexandre',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': Variable.get("ALERT_EMAIL", default_var="alexandre@empresa.com")
}

with DAG(
    'sisagua_data_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados SISAGUA',
    schedule_interval='0 0 5 * *',  # Dia 5 de cada mês às 00:00
    catchup=True,
    max_active_runs=1,
    tags=['water_quality', 'sisagua']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable='sisagua.helpers.extract.extrair_dados',
        provide_context=True,
        op_kwargs={
            'execution_date': '{{ ds }}'
        }
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable='sisagua.helpers.transform.tratar_dados',
        provide_context=True,
        op_kwargs={
            'input_path': '{{ ti.xcom_pull(task_ids="extract_data") }}'
        }
    )

    load_raw_task = PythonOperator(
        task_id='load_raw_data',
        python_callable='sisagua.helpers.load.salvar_dados',
        provide_context=True,
        op_kwargs={
            'input_path': '{{ ti.xcom_pull(task_ids="extract_data") }}'
        }
    )

    load_processed_task = PythonOperator(
        task_id='load_processed_data',
        python_callable='sisagua.helpers.load.salvar_dados',
        provide_context=True,
        op_kwargs={
            'input_path': '{{ ti.xcom_pull(task_ids="transform_data") }}'
        }
    )

    # Ordem de execução
    extract_task >> transform_task >> [load_raw_task, load_processed_task]