from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads data to the given fact table by running the provided sql statement.
    :param redshift_conn_id: reference to a specific redshift cluster hook
    :type redshift_conn_id: str
    :param table: destination fact table on redshift.
    :type table: str
    :param columns: columns of the destination fact table
    :type columns: str containing column names in csv format.
    :param sql_stmt: sql statement to be executed.
    :type sql_stmt: str
    :param append: if False, a delete-insert is performed.
        if True, a append is performed.
        (default value: False)
    :type append: bool
    """
    
    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",                 
                 table="",
                 sql_query="",
                 #delete_first=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        #self.delete_first = delete_first

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading fact table in Redshift")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_query
        )
        redshift.run(formatted_sql)