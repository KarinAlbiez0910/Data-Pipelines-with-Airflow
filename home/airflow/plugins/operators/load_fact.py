from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    truncate_sql_template = """TRUNCATE {table}
                   """
    fact_sql_template = """
                            INSERT INTO {fact_table} ({insert_columns})
                            SELECT DISTINCT {select_columns}                
                            FROM {staging_tables}
                            WHERE {condition}
                        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 insert_columns="",
                 select_columns="",
                 staging_tables="",
                 condition="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.insert_columns = insert_columns
        self.select_columns = select_columns
        self.staging_tables = staging_tables
        self.condition= condition
        self.truncate = truncate

    def execute(self, context):
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            if self.truncate == True:
                truncate_sql = LoadFactOperator.truncate_sql_template.format(
                    table=self.fact_table)
                redshift.run(truncate_sql)

                fact_sql = LoadFactOperator.fact_sql_template.format(
                    fact_table=self.fact_table,
                    insert_columns=self.insert_columns,
                    select_columns=self.select_columns,
                    staging_tables=self.staging_tables,
                    condition=self.condition
                )
                redshift.run(fact_sql)
