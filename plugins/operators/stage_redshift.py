from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
import logging

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 s3_bucket='',
                 s3_key_path='',
                 s3_file_type='json', # csv or json
                 aws_conn_id='',
                 redshift_conn_id='',
                 redshift_iam_role='',
                 table='', # Staging table to be saved in database.
                 create_query='', # Query to run to create the table.
                 copy_query='',
                 jsonpath='',
                 active=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key_path = s3_key_path
        self.s3_file_type = s3_file_type
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.redshift_iam_role = redshift_iam_role
        self.table = table
        self.create_query = create_query
        self.copy_query = copy_query
        self.jsonpath = jsonpath
        self.active = active
        
    def execute(self, context):
        if self.active:
            # Hook to access S3 to grab the raw datafiles.
            s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

            # Hook to access Amazon RedShift for the copy operation.
            redshift_hook = PostgresHook(self.redshift_conn_id)

            # Recreate table.
            redshift_hook.run(self.create_query)

            # Use AWS hook to access S3 path that contains JSON files.
            # We will then copy these files onto our tables in RedShift.
            keys = s3_hook.list_keys(self.s3_bucket, prefix=self.s3_key_path)
            cnt=0
            for key in keys:
                s3_path = f"s3://{self.s3_bucket}/{key}"
                #jsonpath = f"s3://{self.s3_bucket}/{self.jsonpath}"
                logging.info(f"Found {s3_path}")
                logging.info(f"Comparing {s3_path[-len(self.s3_file_type):].upper()} and {self.s3_file_type.upper()}")
                # Check if extension is the same with specified file type.
                cnt=cnt+1
                if s3_path[-len(self.s3_file_type):].upper() == self.s3_file_type.upper():
                    if (cnt < 20):  #since song_data is too many, it takes forever to load, so limiting to 20 JSON files
                        logging.info("copying...")
                        # Copy to RedShift
                        redshift_hook.run(self.copy_query.format(
                            s3_path, 
                            Variable.get(self.redshift_iam_role), 
                            self.jsonpath))
                    else:
                        break






