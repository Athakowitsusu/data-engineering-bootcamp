from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone


with DAG(
    dag_id="my_first_dag",
    schedule="0 17 1,16 * *",
    start_date=timezone.datetime(2023, 8, 27),
    catchup=False,
    tags=["DEB", "2023"],
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2

