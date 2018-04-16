from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

import logging


class RedshiftUnloadOperator(BaseOperator):
    template_fields = ('dst_uri', 'query',)
    ui_color = '#ededed'
    query = """
        UNLOAD ('{{ task.select_sql|replace("'", "\\'") }}')
        TO '{{ task.dst_uri }}/'
        {% if task.iam_role -%}
        IAM_ROLE '{{ task.iam_role }}'
        {%- endif -%}
        {% for option in task.unload_options %}
        {{ option }}
        {%- endfor -%};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 dst_uri,
                 select_sql,
                 unload_options,
                 iam_role=None,
                 *args, **kwargs):
        super(RedshiftUnloadOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dst_uri = dst_uri
        self.select_sql = select_sql
        self.unload_options = unload_options
        self.iam_role = iam_role

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.hook.run(self.query, False)
        logging.info("UNLOAD command complete...")


class RedshiftQueryOperator(BaseOperator):
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 query,
                 *args, **kwargs):
        super(RedshiftQueryOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.hook.run(self.query, False)
        logging.info("QUERY command complete...")


class RedshiftPlugin(AirflowPlugin):
    name = "redshift_plugin"
    operators = [RedshiftUnloadOperator, RedshiftQueryOperator]
