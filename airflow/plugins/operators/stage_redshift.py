from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION 'us-west-2'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json = json
        self.region=region


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('Clearing data from Redshift staging tables')
        redshift.run('DELETE FROM {}'.format(self.table))
        self.log.info('Copying data from S3 to Redshift')

        s3_path = ""
        
        if self.table == 'public.staging_events':
            """
            Rendering S3 path for staging_events table
            s3_path for staging_events rendered output path should be
            "s3://udacity-dend/log_data/2018/11/*.json"
            """
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_key)
            # Note: s3_key = "log_data/{execution_date.year}/{execution_date.month}"

        if self.table == 'public.staging_songs':
            """
            Rendering S3 path for staging_songs table
            s3_path for staging_songs rendered output path should be
            "s3://udacity-dend/song_data/*.json"
            """
            rendered_key = self.s3_key
            s3_path = "s3://{}/{}".format(self.s3_bucket,rendered_key)
      
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json,
            self.region
        )

       
        redshift.run(formatted_sql)
        self.log.info('Loaded data into staging tables')




