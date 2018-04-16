from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

import pandas as pd
import logging


class SalesforceToFileOperator(BaseOperator):
    """
    Make a query against Salesforce and
    write the resulting data to a file.
    """
    template_fields = ("query", )

    @apply_defaults
    def __init__(
        self,
        conn_id,
        obj,
        output,
        fields=None,
        fmt="csv",
        query=None,
        relationship_object=None,
        record_time_added=False,
        coerce_to_timestamp=False,
        *args,
        **kwargs
    ):
        """
        Initialize the operator
        :param conn_id:             name of the Airflow connection that has
                                    your Salesforce username, password and
                                    security_token
        :param obj:                 name of the Salesforce object we are
                                    fetching data from
        :param output:              name of the file where the results
                                    should be saved
        :param fields:              *(optional)* list of fields that you want
                                    to get from the object.
                                    If *None*, then this will get all fields
                                    for the object
        :param fmt:                 *(optional)* format that the output of the
                                    data should be in.
                                    *Default: CSV*
        :param query:               *(optional)* A specific query to run for
                                    the given object.  This will override
                                    default query creation.
                                    *Default: None*
        :param relationship_object: *(optional)* Some queries require
                                    relationship objects to work, and
                                    these are not the same names as
                                    the SF object.  Specify that
                                    relationship object here.
                                    *Default: None*
        :param record_time_added:   *(optional)* True if you want to add a
                                    Unix timestamp field to the resulting data
                                    that marks when the data was
                                    fetched from Salesforce.
                                    *Default: False*.
        :param coerce_to_timestamp: *(optional)* True if you want to convert
                                    all fields with dates and datetimes
                                    into Unix timestamp (UTC).
                                    *Default: False*.
        """

        super(SalesforceToFileOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.output = output

        self.object = obj
        self.fields = fields
        self.fmt = fmt.lower()
        self.query = query
        self.relationship_object = relationship_object
        self.record_time_added = record_time_added
        self.coerce_to_timestamp = coerce_to_timestamp

    def special_query(self, query, sf_hook, relationship_object=None):
        if not query:
            raise ValueError("Query is None.  Cannot query nothing")

        sf_hook.sign_in()

        results = sf_hook.make_query(query)
        if relationship_object:
            records = []
            for r in results['records']:
                if r.get(relationship_object, None):
                    records.extend(r[relationship_object]['records'])
            results['records'] = records

        return results

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Salesforce model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Salesforce")

        # load the SalesforceHook
        # this is what has all the logic for
        # conencting and getting data from Salesforce
        hook = SalesforceHook(
            conn_id=self.sf_conn_id,
            output=self.output
        )

        # attempt to login to Salesforce
        # if this process fails, it will raise an error and die right here
        # we could wrap it
        hook.sign_in()

        # get object from salesforce
        # if fields were not defined,
        # then we assume that the user wants to get all of them
        if not self.fields:
            self.fields = hook.get_available_fields(self.object)

        if self.query:
            query = self.special_query(
                self.query,
                hook,
                relationship_object=self.relationship_object
            )
        else:
            query = hook.get_object_from_salesforce(self.object, self.fields)

        # output the records from the query to a file
        # the list of records is stored under the "records" key
        hook.write_object_to_file(
            query['records'],
            filename=self.output,
            fmt=self.fmt,
            coerce_to_timestamp=self.coerce_to_timestamp,
            record_time_added=self.record_time_added
        )

        logging.info("Query finished!")

class SalesforceSchemaToRedshiftOperator(BaseOperator):
    """
    Reconcile Schema between salesforce API objects and Redshift
        Leverages the Salesforce API function to describe source objects
        and push the object attributes and datatypes to Redshift tables
        1 table per object
        Ignores Compound Fields as they are already broken out into their
        components else where in the
        :param sf_conn_id:      The conn_id for your Salesforce Instance
        :param rs_conn_id:      The conn_id for your Redshift Instance
        :param sf_object:       The Salesforce object you wish you reconcile
                                the schema for.
                                Examples includes Lead, Contacts etc.
        :param rs_schema:       The schema where you want to put the renconciled
                                schema
        :param rs_table:        The table inside of the schema where you want to
                                put the renconciled schema
        .. note::
            Be aware that JSONPath files are used for the column mapping of source
            objects to destination tables
            Datatype conversiona happen via the dt_conv dictionary
    """


    # Field type conversion
    dt_conv = {
        'boolean': lambda x: 'boolean',
        'date': lambda x: 'VARCHAR(256)',
        'dateTime': lambda x: 'VARCHAR(256)',
        'double': lambda x: 'VARCHAR(256)',
        'email': lambda x: 'varchar(80)',
        'id': lambda x: 'varchar(100)',
        'ID': lambda x: 'varchar(100)',
        'int': lambda x: 'float',
        'picklist': lambda x: 'varchar(MAX)',
        'phone': lambda x: 'varchar(40)',
        'string': lambda x: 'varchar(MAX)',
        'textarea': lambda x: 'varchar(MAX)',
        'url': lambda x: 'varchar(256)'
    }

    def __init__(self,
                 sf_conn_id,
                 rs_conn_id, # Connection Ids
                 sf_object, # SF Configs
                 rs_table, # RS Configs
                 sf_fields=None,
                 query=None,
                 rs_schema='public', # Default schema
                 *args, **kwargs):
        super(SalesforceSchemaToRedshiftOperator, self).__init__(*args, **kwargs)
        self.sf_conn_id = sf_conn_id
        self.rs_conn_id = rs_conn_id
        self.sf_object = sf_object
        self.rs_schema = rs_schema
        self.rs_table = rs_table
        self.sf_fields = sf_fields
        self.query = query

    def get_sf_object_cols(self, sf_conn_id, sf_object, sf_fields):
        """
        Uses Salesforce describe() method to fetch columns from
        Salesforce instance. Compound columns are filtered out.
        """
        sf_conn = SalesforceHook(sf_conn_id).sign_in()
        logging.info("Signing in successfully. Fetching SFDC columns")

        # Dynamically Fetch the simple_salesforce query method
        # ie. sf_conn.Lead.describe() | sf_conn.Contact.describe()
        fields = sf_conn.__getattr__(sf_object).describe()['fields']

        # Get compound fields & Links
        k1 = 'compoundFieldName'
        compound_fields = [f[k1] for f in fields] # Get all compound fields across all fields
        compound_fields = set(compound_fields)
        compound_fields.remove(None)

        def build_dict(x): return {
            'rs_name': x['name'].lower(),
            'sf_name': x['name'],
            'path': [x['name']],
            'type': x['soapType'].split(':')[-1],
            'length': x['length'],
            'precision': x['precision']
        }

        # Loop through fields and grab columns we want
        if sf_fields:
            fields = [build_dict(field) for field in fields if field['name'] in sf_fields if field['name'] not in compound_fields]
        else:
            fields = [build_dict(field) for field in fields if field['name'] not in compound_fields]

        dummy = [name['sf_name'] for name in fields]
        logging.info(dummy)
        return fields

    # Generate SQL insert statement
    def get_sql_insert_query(self, df, rs_schema, rs_table):

        cols_str = ','.join(tuple(df.columns))
        for r in df.columns.values:
            df[r] = df[r].map(str)
            df[r] = df[r].map(str.strip)

        # Appending to a list
        values = []
        for value in df.values:
            sub_values = []
            for item in value:
                if item == 'None':
                    item = ''
                item_cleaned = item.replace("'","")
                sub_values.append(item_cleaned)
            values.append(tuple(sub_values))

        if values:
            # Initialize the insert query list
            insert_query_list = []

            # Grab n_rows to insert at a time
            n_rows = 5000

            for tuple_index in range(0, len(values), n_rows):
                q_append = """
                INSERT INTO {schema}.{table} ({cols}) VALUES
                """.format(schema=rs_schema, cols=cols_str, table=rs_table, values=values).rstrip()

                for item in values[tuple_index:tuple_index+n_rows]:
                    q_append+="""
                    \t {item},
                    """.format(item=item).rstrip()

                query = q_append[:-1]
                query += ';'

                insert_query_list.append(query)

            return insert_query_list
        else:
            return ''

    def conv_sf_to_df(self, sf_query_result):
        try:
            df = pd.DataFrame.from_records(sf_query_result, exclude=["attributes"])
        except:
            df = pd.DataFrame.from_records(sf_query_result)
        df.columns = [c.lower() for c in df.columns]
        return df


    def get_sf_object(self, sf_fields, sf_conn_id, sf_object):
        # Sign into Salesforce
        sf_conn = SalesforceHook(conn_id=sf_conn_id)
        sf_conn.sign_in()

        fields = [field['sf_name'] for field in sf_fields]

        logging.info(
            "Making request for {0} fields from {1}".format(len(fields), sf_object)
        )

        query = sf_conn.get_object_from_salesforce(sf_object, fields)
        return query


    def get_sql_create_query(self, rs_table, rs_schema, sf_cols):
        """
        Creates the Create Table DDL to be executed on the Redshift
        instance. Only run if table does not exist at time of first run.
        """

        ddl = """
        CREATE TABLE IF NOT EXISTS {table_schema}.{table_name}
        (
            {cols}
        );

        TRUNCATE {table_schema}.{table_name};
        """
        def make_col_ddl(sf_col):
            sf_type = sf_col['type']
            type_transform = self.dt_conv[sf_type] # Grab lambda type converter
            rs_type = type_transform(sf_col['length']) # Execute type converter

            return "{} {}".format(sf_col['rs_name'], rs_type)

        cols_ddl = [make_col_ddl(col) for col in sf_cols]
        cols_ddl = ', \n'.join(cols_ddl)

        query = ddl.format(table_name=rs_table, table_schema=rs_schema, cols=cols_ddl)
        return query

    def insert_into_redshift(self, create_query, insert_query_list, rs_conn_id):
        logging.info("Connecting to Redshift.")
        rs_conn = PostgresHook(rs_conn_id)
        logging.info("Connection Successful. Inserting Salesforce Data.")
        rs_conn.run(create_query, False)
        logging.info("Table creation Complete")

        if insert_query_list:
            for insert_query in insert_query_list:
                rs_conn.run(insert_query, False)
                logging.info("Data Insertion Complete")
        else:
            logging.info("No data to be inserted")

    def execute(self, context):
        # Get SF data
        # Establish SF hook
        logging.info("Prepping to gather data from Salesforce")
        logging.info("Signing into Salesforce")

        # --- Data Clean-up ---
        # Get SF cols
        sf_cols = self.get_sf_object_cols(self.sf_conn_id, self.sf_object, self.sf_fields)

        # Get SF Object Data
        sf_data = self.get_sf_object(sf_cols, self.sf_conn_id, self.sf_object)

        # Convert the Salesforce object to dataframe
        df = self.conv_sf_to_df(sf_data['records'])

        # --- Query format ---
        # Generate create table query
        create_query = self.get_sql_create_query(self.rs_table, self.rs_schema, sf_cols)

        # Translate dataframe object into insert query
        insert_query_list = self.get_sql_insert_query(df, self.rs_schema, self.rs_table)

        # --- SQL Execution ---
        # Insert into table
        self.insert_into_redshift(create_query, insert_query_list, self.rs_conn_id)


class SalesforcePlugin(AirflowPlugin):
    name = "salesforce_plugin"
    operators = [SalesforceToFileOperator, SalesforceSchemaToRedshiftOperator]
