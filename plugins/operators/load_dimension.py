from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    query_recreate = """
        BEGIN;
        DROP TABLE IF EXISTS {};
        {};
        INSERT INTO {}
        {};
        COMMIT;
    """
    query_append_only = """
        BEGIN;
        INSERT INTO {}
        {};
        COMMIT;
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 redshift_iam_role='',
                 table='', # Staging table to be saved in database.
                 create_query='',
                 insert_query='', # Query to run to insert into the table.
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_iam_role = redshift_iam_role
        self.table = table
        self.create_query = create_query
        self.insert_query = insert_query
        self.append_only = append_only

    def execute(self, context):
        # Hook to access Amazon RedShift for the copy operation.
        redshift_hook = PostgresHook(self.redshift_conn_id)
        if self.append_only:
            redshift_hook.run(LoadDimensionOperator.query_append_only.format(
                self.table,
                self.insert_query
            ))
        else:
            # Recreate table.
            redshift_hook.run(LoadDimensionOperator.query_recreate.format(
                self.table,
                self.create_query,
                self.table,
                self.insert_query
            ))


        