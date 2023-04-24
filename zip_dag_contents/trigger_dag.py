from datetime import datetime
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from subdag import subdag_parallel_dag
from smart_file_sensor import SmartFileSensor


with DAG(
    "dag_trigger",
    default_args={"start_date": datetime(2023, 1, 11), 'schedule_interval': "0 * * * *"}
) as dag:

    # Check file at specified location
    check_file = SmartFileSensor(task_id="check_file", poke_interval=10, fs_conn_id="my_filesystem", filepath="data.csv",
                                 timeout=60 * 5)

    # Trigger external DAG to 'process' file
    trigger_target = TriggerDagRunOperator(task_id="trigger_target", trigger_dag_id="dag_1",
                                           execution_date='{{ ds }}', reset_dag_run=True, wait_for_completion=True)

    # Process results subdag
    process_results = SubDagOperator(task_id="process_results", subdag=subdag_parallel_dag('dag_trigger',
                                     'process_results', default_args={"start_date": datetime(2023, 1, 11),
                                                                      'schedule_interval': "0 * * * *"}))

    # Notify slack
    notify_slack = BashOperator(task_id="notify_slack", bash_command="python3 /opt/airflow/dags/slack_call.py")

    check_file >> trigger_target >> process_results >> notify_slack





