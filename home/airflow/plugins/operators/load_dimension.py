from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    truncate_sql_template = """TRUNCATE {table}
                   """
    dimension_sql_template = """
                        INSERT INTO {table} ({insert_columns})
                        SELECT DISTINCT {select_columns}
                        FROM {staging_table}
                        """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_columns="",
                 select_columns="",
                 staging_table="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_columns = insert_columns
        self.select_columns = select_columns
        self.staging_table = staging_table
        self.truncate = truncate

    def execute(self, context):
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            if self.truncate == True:
                truncate_sql = LoadDimensionOperator.truncate_sql_template.format(
                    table=self.table)
                redshift.run(truncate_sql)

                dimension_sql = LoadDimensionOperator.dimension_sql_template.format(
                    table=self.table,
                    insert_columns=self.insert_columns,
                    select_columns=self.select_columns,
                    staging_table=self.staging_table
                )
                redshift.run(dimension_sql)




