from airflow.example_dags.spider import Cls

from datetime import timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

spider = Cls.spider
read_db = Cls.read_db
db_to_s3 = Cls.db_to_s3
report = Cls.db_to_s3


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
dag = DAG(
    'blog_example',
    default_args=default_args,
    description='Example dag for airflow blog',
    schedule_interval="0 10 * * *",
    start_date=days_ago(2),
    tags=['example'],
)

p1 = PythonOperator(
    task_id='spider',
    python_callable=spider,
    dag=dag,
)

p2 = PythonOperator(
    task_id='read_db',
    python_callable=read_db,
    dag=dag,
)

p3 = PythonOperator(
    task_id='db_to_s3',
    python_callable=db_to_s3,
    dag=dag,
)

p4 = PythonOperator(
    task_id='report',
    python_callable=report,
    dag=dag,
)

b1 = BashOperator(
    task_id='log',
    bash_command='echo "DAG basarili" > logfile.txt',
    dag=dag,
)


p1 >> p2 >> p3 >> p4 >> b1