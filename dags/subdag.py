from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable


def subdag_parallel_dag(parent_dag_id, child_dag_id, default_args):
    """Factory function for SubDag in trigger dag."""
    with DAG(
            f"{parent_dag_id}.{child_dag_id}",
            default_args=default_args
    ) as dag:

        # Get path to test file
        path = Variable.get('path', default_var=None)

        def get_exec_date(execution_date, **kwargs):
            """ Gets execution date from xcom to enable ExternalTaskSensor."""
            context = kwargs
            execution_date = str(context['ti'].xcom_pull(key="exec_date", dag_id="dag_1", task_ids="push_exec_date",
                                                         include_prior_dates=True))
            # One of the following - depends on whether DAG is running on schedule or was triggered manually
            new_execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S%z")
            # new_execution_date = datetime.strptime(execution_date, "%Y-%m-%dT%H:%M:%S.%f%z")
            return new_execution_date

        # Check status of target DAG
        target_dag_status = ExternalTaskSensor(task_id="target_dag_status", external_dag_id="dag_1", execution_date_fn=get_exec_date,
                                               external_task_id=None, allowed_states=["success"], poke_interval=10)

        @task(task_id="print_statement")
        def print_statement(**kwargs):
            """Prints statement using xcom data."""
            context = kwargs
            received_id = str(context['ti'].xcom_pull(key="dag_run_id", dag_id="dag_1", task_ids="push_run_dag_id",
                                                      include_prior_dates=True))
            return f"{received_id} pulled"

        print_statement = print_statement()

        remove_file = BashOperator(task_id="remove_file", bash_command=f"rm {path}data.csv")

        # Create finished file
        timestamp = str("{{ ts_nodash }}")
        create_finished_file = BashOperator(task_id="create_finished_file",
                                            bash_command=f"touch {path}finished_{timestamp}")

        target_dag_status >> print_statement >> remove_file >> create_finished_file

        return dag

