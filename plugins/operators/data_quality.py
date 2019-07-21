from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 tables="",                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        logging.info(f"Listing tables self.tables from {self.tables}")
        #list_tbl = [self.tables["t1"],self.tables["t2"],self.tables["t3"],self.tables["t4"],self.tables["t5"]}
        #logging.info(f"Listing list {list_tbl}")
        logging.info(f"Listing tables ADMIN-DATA from {self.tables}")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.tables}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.tables} returned no results")                
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.tables} contained 0 rows")
        logging.info(f"Data quality on table {self.tables} check passed with {records[0][0]} records")
