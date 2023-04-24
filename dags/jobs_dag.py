from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from custom_operator import PostgreSQLCountRows
import uuid


config = {
    'dag_1': {"start_date": datetime(2023, 1, 11), "database": "dags", "table_name": "table_1"},
    'dag_2': {'schedule_interval': "0 * * * *", "start_date": datetime(2023, 1, 5), "database": "dags", "table_name": "table_2"},
    'dag_3': {'schedule_interval': "0 0 * * *", "start_date": datetime(2022, 12, 29), "database": "dags", "table_name": "table_3"}
}


def create_dag(dag_id: str, dag_config: dict):
    with DAG(
        dag_id,
        default_args=dag_config
    ) as dag:

        # Push DAG execution date to xcom
        @task(task_id="push_exec_date", queue='jobs_queue')
        def push_exec_date(**kwargs):
            """ Pushes execution date to xcom to enable working with ExternalTaskSensor."""
            context = kwargs
            date = str(context['execution_date'])
            context['ti'].xcom_push(key='exec_date', value=date)

        push_exec_date = push_exec_date()

        # Using task decorator instead of PythonOperator
        @task(task_id="printing_start_logs", queue='jobs_queue')
        def print_logs():
            """Prints starting logs for each database."""
            return f"File received - {dag_id} start processing tables in {dag_config['database']}"

        print_start = print_logs()

        # Check if table exists - helper function
        def check_table_exists_helper(sql_to_check_table_exist, table_name):
            """ Helper function to return True if table exists and False otherwise."""
            hook = PostgresHook(postgres_conn_id='postgres_localhost')
            query = hook.get_first(sql=sql_to_check_table_exist.format(table_name))
            print(query)
            if query:
                return True
            return False

        # Adding branch to check if table exists
        @task.branch(task_id="check_table_exists", queue='jobs_queue')
        def check_table_exists_function(table_exists):
            """ Method to check if table exists."""
            if table_exists:
                return "skip_create_table"
            return "create_table"

        check_table = check_table_exists_function(check_table_exists_helper(sql_to_check_table_exist="""
                                                     SELECT table_name FROM information_schema.tables
                                                     WHERE table_schema = 'public'
                                                     AND table_name = '{}';""", table_name=dag_config['table_name']))

        # Check user
        get_current_user = BashOperator(task_id="get_current_user", queue='jobs_queue', bash_command="whoami",
                                        do_xcom_push=True)

        # Create table task
        create_table = PostgresOperator(task_id="create_table", queue='jobs_queue', postgres_conn_id='postgres_localhost',
                                        sql="""CREATE TABLE if not exists {}
                                        (custom_id integer NOT NULL, 
                                        user_name VARCHAR (50) NOT NULL, 
                                        timestamp TIMESTAMP NOT NULL); """.format(dag_config['table_name']))

        # Skip create table task - to make sure that path is not empty
        skip_create_table = DummyOperator(task_id="skip_create_table", queue='jobs_queue')

        # Insert row into db
        insert_query = f"INSERT INTO {dag_config['table_name']} (custom_id, user_name, timestamp) VALUES " \
                       f"({uuid.uuid4().int % 123456789}, " + "'" + \
                       "{{ ti.xcom_pull(task_ids='get_current_user', key='return_value')}} " + "'," \
                       " CURRENT_TIMESTAMP);"

        insert_new_row = PostgresOperator(task_id="insert_new_row", queue='jobs_queue', trigger_rule="none_failed",
                                          postgres_conn_id='postgres_localhost',
                                          sql=insert_query)

        # Query table with custom operator
        query_table = PostgreSQLCountRows(task_id="query_table", queue='jobs_queue', conn='postgres_localhost',
                                          table_name=dag_config['table_name'])

        # Push data to xcom
        @task(task_id="push_run_dag_id", queue='jobs_queue')
        def push_run_dag_id(**kwargs):
            """Pushes dag run details to xcom to be printed by SubDag."""
            context = kwargs
            value_to_push = str(context['dag_run'])
            context['ti'].xcom_push(key="dag_run_id", value=value_to_push)

        push_run_dag_id = push_run_dag_id()

        push_exec_date >> print_start >> get_current_user >> check_table >> [create_table, skip_create_table] >> \
        insert_new_row >> query_table >> push_run_dag_id

    return dag


for dag_id, config_data in config.items():
    globals()[dag_id] = create_dag(dag_id, config_data)
