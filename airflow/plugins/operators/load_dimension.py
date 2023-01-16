from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append_only="",
                 load_dimension_sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_only = append_only
        self.load_dimension_sql = load_dimension_sql

    def execute(self, context):
        self.log.info('Starting to load into dimension table ' + self.table)
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # Truncate table first if append_only is False (default for dimension tables)
        
        if not self.append_only:
            self.log.info("Truncating table {}".format(self.table))
            redshift.run("TRUNCATE TABLE {}".format(self.table))
            load_dimension_sql = (f"INSERT INTO {self.table} {self.load_dimension_sql}")
            redshift.run(load_dimension_sql)
            
        
        if self.append_only:
            load_dimension_sql = (f"INSERT INTO {self.table} {self.load_dimension_sql}")
            redshift.run(load_dimension_sql)
        
        self.log.info('Loaded data into dimension table ' + self.table)