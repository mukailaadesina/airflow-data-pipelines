from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    table_insert = """
        INSERT INTO {} {}
    """
    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        if self.append == False :
            sql_statement = LoadDimensionOperator.truncate_sql.format(self.table)
            sql_statement += LoadDimensionOperator.table_insert.format(self.table, self.sql_query)
            operation = 'truncate'
        else:
            sql_statement = LoadDimensionOperator.table_insert.format(self.table, self.sql_query)
            operation = 'append'
            
        redshift.run(sql_statement)
        
        self.log.info(f"Ending LoadDimensionOperator {self.table} with a Success on Operation  {operation}")
