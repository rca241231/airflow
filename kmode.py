from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from kmodes.kmodes import KModes
import pandas as pd
import numpy as np
import logging


class KModeSurveyRecOperator(BaseOperator):
    """
    This function calculates the centroid based on the data given using the
    K-mode method.

    https://arxiv.org/ftp/cs/papers/0603/0603120.pdf

    Param rs_conn_id:       Connection ID to Redshift
    Param rs_table:         Table to get data from
    Param features:         Column to be included as features
    Param n_cluster:        The clusters to classify users into
    Param n_iter:           The number of iterations to produce the centroid
    Param init_method:      The algorithm to use - Cao Method: Cao et al.
                            [2009] OR Huang Method: Huang [1997]

    This function returns the Centroid of the survey response and the dataframe
    with predictions
    """

    @apply_defaults
    def __init__(self,
                 cluster_name,
                 rs_conn_id,
                 rs_table,
                 rs_schema="public",
                 features=['age', 'gender', 'weight', 'existing_conditions','light_exercise'],
                 n_cluster=5,
                 n_iter=5,
                 init_method="Cao",
                 *args, **kwargs):
        super(KModeSurveyRecOperator, self).__init__(*args, **kwargs)
        self.cluster_name = cluster_name
        self.rs_conn_id = rs_conn_id
        self.rs_table = rs_table
        self.rs_schema = rs_schema
        self.features = features
        self.n_cluster = n_cluster
        self.n_iter = n_iter
        self.init_method = init_method

    def kmode_calculation(self, data):
        """
        This function calculates the centroid using the k-mode algorithm.

        This functiontakes in the cleaned data and returns:

        - Column element mapping dictionary
        - Centroids
        - The output data with classification
        """
        col_dict = {}

        for col in data.columns:
            data[col] = data[col].astype('category')
            col_dict.update({col: dict(enumerate(data[col].cat.categories))})

        # Get all the cols in the DataFrame
        cols = [col for col in data.columns]

        # Transform all values into categorical and numerical values
        for col in cols:
            data[col] = data[col].astype('category')
            data[col] = data[col].cat.codes

        # Run k-modes using the algorithm
        kmodes_method = KModes(n_clusters=self.n_cluster, init=self.init_method, n_init=self.n_iter, verbose=1)
        kmode_result = kmodes_method.fit_predict(data[cols])

        # Attach the output label for each data point
        data['classification'] = pd.Series(kmode_result, index=data.index)

        return col_dict, kmodes_method.cluster_centroids_, data

    def get_rs_cols(self):
        """
        This function will get all of the columns into a list generated from
        the questions in the last 6 days.
        """

        query = """
        SELECT
        DISTINCT question
        FROM survey_response
        WHERE 1=1
        AND response_time > (current_timestamp - interval '6 day')
        """.format(schema=self.rs_schema, table=self.rs_table)

        # Establish connection to Redshift
        self.rs_hook = PostgresHook(postgres_conn_id=self.rs_conn_id)

        # Get the cols in a list
        df = self.rs_hook.get_pandas_df(query)

        # Convert into list
        cols_list = df['question'].values.T.tolist()

        return cols_list


    def get_rs_query(self, cols_list):
        """
        This function will generate, using the column information to get all
        of the data
        """
        rs_query = """
        SELECT
        user_id
        """

        for question in cols_list:
            if question in self.features:
                rs_query += """
                ,COALESCE(CASE WHEN question = '{question}' THEN regexp_replace(response, '\\[|\\]|"', '') END, 'unspecified') AS {question_cleaned}
                """.format(question=question,
                           question_cleaned=question.replace(" ", "_"))

        rs_query += """
        FROM {schema}.{table}
        WHERE 1=1
        AND response_time > (current_timestamp - interval '7 day')
        AND user_id IS NOT NULL
        """.format(schema=self.rs_schema, table=self.rs_table)

        return rs_query

    def get_rs_data(self, query):
        """
        This function returns the survey data in the dataframe format
        """
        # Establish connection to Redshift
        self.rs_hook = PostgresHook(postgres_conn_id=self.rs_conn_id)

        # Get the data in dataframe
        survey_df = self.rs_hook.get_pandas_df(query)

        return survey_df

    def rs_execute(self, rs_query):
        """
        This function executes the query passed in.
        """
        logging.info("Connecting to Redshift.")
        rs_conn = PostgresHook(self.rs_conn_id)
        logging.info("Connection Successful. Executing query.")

        if rs_query:
            rs_conn.run(rs_query, False)
            logging.info("Query Execution Complete.")
        else:
            logging.info("No Query to Execute")

    def dict_to_sql(self, dict_obj):
        create_query = """
        CREATE TABLE IF NOT EXISTS {schema}.{table}_{name}_dict (
            question        VARCHAR(64),
            value           VARCHAR(128),
            value_mapping   int);

        TRUNCATE {schema}.{table}_{name}_dict;
        """.format(name=self.cluster_name, schema=self.rs_schema, table=self.rs_table)

        insert_query = """
        INSERT INTO {schema}.{table}_{name}_dict VALUES
        """.format(name=self.cluster_name, schema=self.rs_schema, table=self.rs_table)
        for question, sub_dict in dict_obj.items():
            for value_mapping, value in sub_dict.items():
                insert_query += """
                ('{question}','{value}',{value_mapping}),
                """.format(question=question, value=value, value_mapping=value_mapping)

        insert_query = insert_query.strip()[:-1] + ';'

        return create_query, insert_query

    def col_mapping(self, dataframe, col_dict):
        for key, value in col_dict.items():
            value.update({-1: 'null'})
            dataframe[key].replace(value, inplace=True)
        return dataframe

    def df_to_sql(self, dataframe):
        # Generate Create Query
        rs_df_create_query = """
            CREATE TABLE IF NOT EXISTS {schema}.{table}_{name}_cluster (
            """.format(name=self.cluster_name,
                       schema=self.rs_schema,
                       table=self.rs_table)

        for column in dataframe.columns:
            rs_df_create_query += """
                {column} VARCHAR(128),
                """.format(column=column)

        rs_df_create_query = rs_df_create_query.strip()[:-1] + ');'
        rs_df_create_query += """
        TRUNCATE {schema}.{table}_{name}_cluster;
        """.format(name=self.cluster_name,
                   schema=self.rs_schema,
                   table=self.rs_table)

        # Generate Insert Query
        rs_df_insert_query = """
            INSERT INTO {schema}.{table}_{name}_cluster VALUES
            """.format(name=self.cluster_name,
                       schema=self.rs_schema,
                       table=self.rs_table)

        rs_insert_list = []
        for index, row in dataframe.iterrows():
            rs_insert_list.append([dataframe[column][index] for column in dataframe.columns])

        for row in range(len(rs_insert_list)):
            if row % 500 == 0 and row != 0:
                rs_df_insert_query = rs_df_insert_query[:-1] + ';'
                rs_df_insert_query += """
                    INSERT INTO {schema}.{table}_{name}_cluster VALUES
                    """.format(name=self.cluster_name,
                               schema=self.rs_schema,
                               table=self.rs_table)
            rs_df_insert_query += str(rs_insert_list[row]).replace("[","(").replace("]",")")
            rs_df_insert_query += ','

        rs_df_insert_query = rs_df_insert_query[:-1] + ';'

        return rs_df_create_query, rs_df_insert_query

    def list_to_sql(self, list_obj, col_dict):
        create_query = """
        CREATE TABLE IF NOT EXISTS {schema}.{table}_{name}_centroids (
        """.format(name=self.cluster_name, schema=self.rs_schema, table=self.rs_table)

        for key in col_dict.keys():
            create_query += """
            {key} INT,
            """.format(key=key)

        create_query += """
        cluster INT);

        TRUNCATE {schema}.{table}_{name}_centroids;
        """.format(name=self.cluster_name, schema=self.rs_schema, table=self.rs_table)

        insert_query = """
        INSERT INTO {schema}.{table}_{name}_centroids VALUES
        """.format(name=self.cluster_name, schema=self.rs_schema, table=self.rs_table)

        iter = 0
        for item in list_obj:
            insert_query += str(np.append(item,iter).tolist()).replace("[","(").replace("]",")")
            insert_query += ","
            iter=+1

        insert_query = insert_query[:-1] + ";"
        return create_query, insert_query


    def execute(self, context):
        # Get the columns from Redshift
        cols_list = self.get_rs_cols()

        # Get the query to get the data
        data_query = self.get_rs_query(cols_list)

        # Get the data
        survey_df = self.get_rs_data(data_query)

        # Calculate clusters using kmodes clustering
        col_dict, kmodes_centroids, data_result_cat = self.kmode_calculation(survey_df)

        # Map the data back to original form
        data_result = self.col_mapping(data_result_cat, col_dict)

        # Convert the dict object to SQL insert queries and the list to insert query
        rs_df_create_query, rs_df_insert_query = self.df_to_sql(data_result)
        rs_col_dict_create_query, rs_col_dict_insert_query = self.dict_to_sql(col_dict)
        rs_centroid_create_query, rs_centroid_insert_query = self.list_to_sql(kmodes_centroids, col_dict)

        # Generate list of query to run
        create_sql_list = [rs_df_create_query, rs_col_dict_create_query, rs_centroid_create_query]
        insert_sql_list = [rs_df_insert_query, rs_col_dict_insert_query, rs_centroid_insert_query]

        # Insert the cluster data into Redshift
        for create_query in create_sql_list:
            self.rs_execute(create_query)

        for insert_query in insert_sql_list:
            self.rs_execute(insert_query)

class KModePlugin(AirflowPlugin):
    name = "kmode_plugin"
    operators = [KModeSurveyRecOperator]
