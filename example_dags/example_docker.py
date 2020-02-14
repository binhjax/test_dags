# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['binh.nt@teko.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'queue': 'queue2',
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
    'test_docker_image',
    default_args=default_args,
    description='Test run docker image',
    schedule_interval=timedelta(days=1),
)
dag.doc_md = __doc__

# Define checking tasks
check_commands = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""
check_task = BashOperator(
    task_id='checking',
    depends_on_past=False,
    bash_command=check_commands,
    params={'my_param': 'Parameter I passed in queue default'},
    dag=dag
)

# Define main docker tasks
main_task = DockerOperator(
    task_id='main_docker_task',
    image='teko_spark_env:latest',
    api_version='auto',
    depends_on_past=False,
    auto_remove=True,
    environment= {
    },
    command=['/bin/sh', '-c', 'echo','"hello world"' ],
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    tty=True,
    dag=dag,
)

# Define after finishing tasks
finish_commands = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""
finish_task = BashOperator(
    task_id='finishing',
    depends_on_past=False,
    bash_command=finish_commands,
    params={'my_param': 'Parameter I passed in queue default'},
    dag=dag
)
email = EmailOperator(
        task_id='send_email',
        to='binh.nt@teko.vn',
        subject='Finished Task',
        html_content=""" <h3>Finished Task</h3> """,
        dag=dag
)
check_task >>  main_task >> finish_task >> email
