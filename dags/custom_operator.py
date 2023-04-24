from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class PostgreSQLCountRows(BaseOperator):
    """ Custom operator that returns number of rows for given table."""

    @apply_defaults
    def __init__(self, table_name, conn, *args, **kwargs):
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.conn = conn

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn)
        query = hook.get_first(sql="SELECT COUNT(*) FROM {}".format(self.table_name))
        number = query[0]
        print(f"Number of rows in {self.table_name}: {number}")
        return number

