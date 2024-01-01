import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from cricket_data_etl import extract, load, transform

def create_connection() -> None:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        pg_hook = PostgresHook(postgres_conn_id="my_postgres")
        pg_conn = pg_hook.get_conn()
        print("Created postgres connection")
        return pg_conn
    except:
        print("Postgres connection error")
    return conn

def send_to_postgres():
    conn = create_connection()
    load.load(conn)
    return None

dag = DAG(
    dag_id="cricket_data_etl",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None,
)

extract_data = PythonOperator(
    task_id="extract_data",
    python_callable=extract.main,
    dag=dag,
)

load_data = PythonOperator(
    task_id="load_data",
    python_callable = send_to_postgres,
    dag=dag,
)

extract_data >> load_data


