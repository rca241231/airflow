from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

import requests
import zipfile
import io
import pandas as pd
import os
import logging


class QualtricOperator(BaseOperator):
    """
    This operator downloads Qualtric files and loads into Redshift

    Param apiToken:     Qualtric API Token
    Param surveyId:     The survey ID's in Qualtric
    Param fileFormat:   The file format
    Param dataCenter:   The data center
    """

    # Need to be customized
    column_mapping = {
        'Key': 'Variable'
    }

    @apply_defaults
    def __init__(self,
                 apiToken,
                 surveyId,
                 fileFormat,
                 rs_table,
                 rs_conn_id,
                 dataCenter,
                 *args, **kwargs):
        super(QualtricOperator, self).__init__(*args, **kwargs)
        self.apiToken = apiToken
        self.surveyId = surveyId
        self.fileFormat = fileFormat
        self.dataCenter = dataCenter
        self.rs_table = rs_table
        self.rs_conn_id = rs_conn_id


        # Static parameters
        self.filepath = "./Tmp/QualtricResponses"
        self.baseUrl = "https://{0}.qualtrics.com/API/v3/responseexports/".format(dataCenter)

        self.progressStatus = "in progress"
        self.headers = {
            "content-type": "application/json",
            "x-api-token": self.apiToken,
        }

    def qualtric_download(self, surveyId):
        requestCheckProgress = 0

        # Step 1: Creating Data Export
        downloadRequestUrl = self.baseUrl
        downloadRequestPayload = '{"format":"' + self.fileFormat + '","surveyId":"' + surveyId + '"}'
        downloadRequestResponse = requests.request("POST", downloadRequestUrl, data=downloadRequestPayload, headers=self.headers)
        progressId = downloadRequestResponse.json()["result"]["id"]

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while requestCheckProgress < 100 and self.progressStatus is not "complete":
            requestCheckUrl = self.baseUrl + progressId
            requestCheckResponse = requests.request("GET", requestCheckUrl, headers=self.headers)
            requestCheckProgress = requestCheckResponse.json()["result"]["percentComplete"]
            print("Download is " + str(requestCheckProgress) + " complete")

        # Step 3: Downloading file
        requestDownloadUrl = self.baseUrl + progressId + '/file'
        requestDownload = requests.request("GET", requestDownloadUrl, headers=self.headers, stream=True)

        # Step 4: Unzipping the file
        zipfile.ZipFile(io.BytesIO(requestDownload.content)).extractall(self.filepath)
        print('Complete')

    def get_all_file_path(self):
        survey_list = []
        for root, dirs, files in os.walk(self.filepath):
            for file in files:
                if file.endswith(".csv"):
                    survey_list.append([os.path.join(root, file), file])

        return survey_list

    def get_create_query(self, df, table_name):
        query = """
            CREATE TABLE IF NOT EXISTS {table_name} (
        """.format(table_name=table_name)

        for col in df.columns:
            if col in self.column_mapping.keys():
                query += """
                {col} {col_type},
                """.format(col=col, col_type = self.column_mapping[col])
            else:
                query += """
                {col} INT,
                """.format(col=col)

        query = query.strip()[:-1] + ');'

        return query


    def get_insert_query(self, df, table_name):
        query = """
        INSERT INTO {table_name} VALUES
        """.format(table_name=table_name)


        for index, row in df.iterrows():
            value = []
            for element in list(row):
                if type(element) is str:
                    element = element.replace("'","`").replace('"','`')
                    value.append(element)
                else:
                    value.append(element)
            query += str(tuple(value)).replace('nan','null').replace('"',"'") + ',\n'

        query = query.strip()[:-1] + ';'

        return query

    def truncate_table(self):
        truncate_query = """
        TRUNCATE {table};
        """.format(table=self.rs_table)

        try:
            logging.info("Connecting to Redshift.")
            rs_conn = PostgresHook(self.rs_conn_id)
            logging.info("Connection Successful. Inserting Salesforce Data.")
            rs_conn.run(truncate_query, False)
            logging.info("Table truncate complete.")
        except:
            logging.info("Table does not exist. Nothing to truncate.")


    def insert_into_redshift(self, create_query, insert_query):
        logging.info("Connecting to Redshift.")
        rs_conn = PostgresHook(self.rs_conn_id)
        logging.info("Connection Successful. Inserting Salesforce Data.")
        rs_conn.run(create_query, False)
        logging.info("Table creation Complete")
        rs_conn.run(insert_query, False)
        logging.info("Data Insertion Complete")

    def execute(self, context):
        # Download all surveys
        for survey in self.surveyId:
            self.qualtric_download(survey)

        # Get all filepaths
        filepaths_names = self.get_all_file_path()

        # Truncate the table if exists
        self.truncate_table()

        # Export all files to Redshift
        for filepath_name in filepaths_names:
            df = pd.read_csv(filepath_name[0], skiprows=[1,2])
            if (df.shape[0] > 0 and df.shape[1] > 0):
                df['survey_type'] = pd.Series(filepath_name[1]
                                              .replace(".csv", ""),
                                              index=df.index)
                create_query = self.get_create_query(df, self.rs_table)
                insert_query = self.get_insert_query(df, self.rs_table)
                self.insert_into_redshift(create_query, insert_query)


class QualtricPlugin(AirflowPlugin):
    name = "qualtric_plugin"
    operators = [QualtricOperator]
