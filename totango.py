from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
import os
import json
import logging


class TotangoRedshiftOperator(BaseOperator):
    """
    This operator will curl data from Totango and move the data to Redshift

    Param totango_curl_request:     The totango curl request to use
    Param rs_conn_id:               The RS connection id to use
    Param rs_table:                 The RS table to insert the data into
    Param rs_schema:                The RS schema to insert the data into
    """
    # Dictionary for Totango and Postgres type mapping
    totango_postgre_mapping = {
        'string': 'VARCHAR(256)',
        'number': 'NUMERIC(10,1)',
        'date_attribute': 'BIGINT',
        'string_attribute': 'VARCHAR(64)',
        'number_attribute': 'VARCHAR(16)'
    }

    totango_data_replacement_map = {
        "Dict": "Replacement"
    }

    @apply_defaults
    def __init__(
        self,
        totango_curl_request,
        totango_curl_header,
        totango_curl_footer,
        totango_object,
        rs_conn_id,
        rs_table,
        rs_schema,
        *args,
        **kwargs
    ):
        super(TotangoRedshiftOperator, self).__init__(*args, **kwargs)
        self.totango_curl_request = totango_curl_request
        self.totango_curl_header = totango_curl_header
        self.totango_curl_footer = totango_curl_footer
        self.totango_object = totango_object
        self.rs_conn_id = rs_conn_id
        self.rs_table = rs_table
        self.rs_schema = rs_schema

    def build_curl_query(self, curl_header, curl_query, curl_footer):
        """
        Returns the complete curl query
        """
        json_curl_query = curl_header + curl_query + curl_footer
        return json_curl_query


    def build_col_dict(self, curl_query):
        """
        This function creates the query to build totango postgres mapping
        columns
        """
        col_dict = {}
        for json_col_info in curl_query['fields']:
            col_dict[json_col_info['field_display_name']] = json_col_info['type']
        return col_dict

    def build_create_query(self, col_dict, rs_table):
        """
        This function builds the create query to be executed to create rs table
        """
        query = """
        CREATE TABLE IF NOT EXISTS {rs_table} \n (
        """.format(rs_table=rs_table)

        for key, value in col_dict.items():
            query += """
            {key} {type},
            """.format(key=key.replace(" ", "_")
                              .replace(":","")
                              .replace("(","")
                              .replace(")","")
                              .replace("%","percentage")
                              .strip(),
                       type=self.totango_postgre_mapping[value])
        query = query.strip()[:-1] + ");"

        query += """
        TRUNCATE {rs_table};
        """.format(rs_table=rs_table)

        return query

    def build_insert_query(self, col_dict, json_data, schema, table):
        query = "INSERT INTO {schema}.{table} VALUES".format(schema = schema, table = table)

        for json_row in json_data:
            row_str = str(json_row['selected_fields']).replace("[","").replace("]","").replace("None","null").replace('"',"'").strip()
            for key, value in self.totango_data_replacement_map.items():
                if key in row_str:
                    row_str = row_str.replace(key, value)
            query +="""
            ({row_str}),
            """.format(row_str = row_str)

        query = query.strip()[:-1] + ";"
        return query

    def rs_run_totango(self, rs_create_query, rs_insert_query):
        logging.info("Connecting to Redshift.")
        rs_conn = PostgresHook(self.rs_conn_id)
        logging.info("Connection Successful. Creating Table.")
        rs_conn.run(rs_create_query, False)
        logging.info("Table creation Complete")

        if rs_insert_query:
            logging.info("Inserting Totango Data")
            rs_conn.run(rs_insert_query, False)
            logging.info("Data Insertion Complete")
        else:
            logging.info("No data to be inserted")

    def execute(self, context):

        # Get curl query
        totango_curl_query = self.build_curl_query(self.totango_curl_header, self.totango_curl_request, self.totango_curl_footer)

        # Get curl object
        totango_json_string = os.popen(totango_curl_query).read()
        totango_json = json.loads(totango_json_string)
        totango_json_cleaned = totango_json['response'][self.totango_object]['hits']

        # Get col mapping
        col_dict = self.build_col_dict(json.loads(self.totango_curl_request))

        # Get create query
        rs_create_query = self.build_create_query(col_dict, self.rs_table)

        # Get insert query
        rs_insert_query = self.build_insert_query(col_dict, totango_json_cleaned, self.rs_schema, self.rs_table)

        # Execute create and insert query
        self.rs_run_totango(rs_create_query, rs_insert_query)


class TotangoPlugin(AirflowPlugin):
    name = "totango_plugin"
    operators = [TotangoRedshiftOperator]
