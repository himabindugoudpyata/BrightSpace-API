# !pip3 install fast_to_sql
# !pip install sqlalchemy
import pandas as pd
from datetime import datetime
from fast_to_sql import fast_to_sql as fts
import pyodbc
import json
import argparse
import collections
import csv
import datetime
import io
import json
import logging
import os
import time 
import zipfile
import pyodbc
import contextlib
import psycopg2
from psycopg2 import sql
import requests
from requests.auth import HTTPBasicAuth

DataSetMetadata = collections.namedtuple('DataSetMetadata', ['plugin', 'table'])
API_VERSION = '1.18'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config-sample.json'

FULL_DATA_SET_METADATA = [
    DataSetMetadata(
       plugin='847d2e44-4fc1-4060-a6ce-80b6c6f95f7d',
       table='SessionHistory'   
    ),
    DataSetMetadata(
       plugin='5813e618-49ec-4e5c-90e7-1fb4fe4b59c6',
       table='SystemAccessLog'   
    ),
    DataSetMetadata(
      plugin='1d6d722e-b572-456f-97c1-d526570daa6b',
       table='Users'   
    ),
    DataSetMetadata(
      plugin='20794201-b8fe-4010-9197-9f4997f91531',
       table='UserLogins'   
    ),
    DataSetMetadata(
      plugin='c437b117-16b3-46b8-bae9-ac64948c8882',
       table='Tools'   
    ),
    DataSetMetadata(
      plugin='bd61f20b-be91-4b93-b449-46361e2c323f',
       table='RoleDetails'   
    ),
    DataSetMetadata(
      plugin='eef7ca81-86bb-430c-96ee-382b83f5c0f9',
       table='QuizObjects'   
    ),
    DataSetMetadata(
      plugin='07a9e561-e22f-4e82-8dd6-7bfb14c91776',
       table='OrganizationalUnits'   
    ),
    DataSetMetadata(
      plugin='cb7caa4a-c35f-48d0-a9ae-59eefea299df',
       table='OrganizationalUnitParents'   
    ),
    DataSetMetadata(
      plugin='88cfcc22-ce8b-4dab-8d42-2b9da92f29cf',
       table='EnrollmentsAndWithdrawals'   
    ),
    DataSetMetadata(
      plugin='1c4add93-4905-4b24-b50d-a14fd10c971a',
       table='DiscussionTopicUserScores'   
    ),
    DataSetMetadata(
      plugin='0646bbe1-79af-48ef-89d9-91f677419259',
       table='DiscussionTopics'   
    ),
    DataSetMetadata(
      plugin='ac51124b-6038-4b04-a186-92eb4cef40b0',
       table='DiscussionPostsReadStatus'   
    ),
    DataSetMetadata(
      plugin='bce64f34-acee-415e-aceb-e3a38ddf476f',
       table='DiscussionPosts'   
    ),
    DataSetMetadata(
      plugin='8851ce21-6049-4004-9990-78c372bbd3b7',
       table='DiscussionForums'   
    ),
    DataSetMetadata(
      plugin='785d15d3-79d6-4724-9dad-8714ae0a1d1d',
       table='ChatSessionLog'   
    ),
    DataSetMetadata(
      plugin='11af4521-8ef4-4f76-9129-974a8009d8b9',
       table='ChatObjects'   
    ),
    DataSetMetadata(
      plugin='b12a4203-3169-4dbb-9e6b-e979fc1620a9',
       table='AssignmentSubmissionDetails'   
    ),
    DataSetMetadata(
     plugin='041dde83-3a29-4a37-97de-9ee615318111',
       table='AssignmentSubmissions'   
    ),
    DataSetMetadata(
      plugin='d9923de9-de6a-41ea-a63e-e8fd771b7b93',
       table='AssignmentSummary'   
    ),
    DataSetMetadata(
      plugin='e260902a-582c-48c9-8dd8-80aa7dfa6b76',
       table='Testing'   
    ),
    DataSetMetadata(
      plugin='5c0f2c70-4737-44ee-8780-be67bfa43594',
       table='QuestionLibrary'   
    ),
    DataSetMetadata(
      plugin='f1623581-c5d7-4562-93fe-6ad16010c96b',
       table='QuizAttempts'   
    ),
    DataSetMetadata(
      plugin='24d9051c-509a-4ea3-81bc-735f36bf94f0',
       table='QuizUserAnswerResponses'   
    ),
    DataSetMetadata(
      plugin='93d6063b-61d4-4629-a6af-b4fad71f8c55',
       table='QuizUserAnswers'   
    ),
    DataSetMetadata(
      plugin='e4b3d080-b4f8-4d6c-abf3-98bf887829bc',
       table='TurnItInSubmissions'   
    ),
    DataSetMetadata(
      plugin='e260902a-582c-48c9-8dd8-80aa7dfa6b76',
       table='CourseAccess'   
    )
]


def get_plugin_link_mapping(config, access_token):
    data_sets = []
    next_page_url = '{bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds'.format(
        bspace_url=config['bspace_url'],
        lp_version=API_VERSION
    )

    while next_page_url is not None:
        list_response = get_with_auth(next_page_url, access_token)
        list_json = list_response.json()

        data_sets += list_json['BrightspaceDataSets']
        next_page_url = list_json['NextPageUrl']

    return { d['PluginId']: d['DownloadLink'] for d in data_sets }
    
 
def get_config():
    with open(CONFIG_LOCATION, 'r') as f:
        return json.load(f)

def trade_in_refresh_token(config):
    # https://tools.ietf.org/html/rfc6749#section-6
    response = requests.post(
        '{}/core/connect/token'.format(config['auth_service']),
        # Content-Type 'application/x-www-form-urlencoded'
        data={
            'grant_type': 'refresh_token',
            'refresh_token': config['refresh_token'],
            'scope': 'datahub:dataexports:*'
        },
        auth=HTTPBasicAuth(config['client_id'], config['client_secret'])
    )

    if response.status_code != 200:
        logger.error('Status code: %s; content: %s', response.status_code, response.text)
        response.raise_for_status()

    return response.json()

def put_config(config):
    with open(CONFIG_LOCATION, 'w') as f:
        json.dump(config, f, sort_keys=True)

def get_with_auth(endpoint, access_token):
    headers = {'Authorization': 'Bearer {}'.format(token_response['access_token'])}
    response = requests.get(endpoint, headers=headers)

    if response.status_code != 200:
        logger.error('Status code: %s; content: %s', response.status_code, response.text)
        response.raise_for_status()

    return response
    
    
# Create a pyodbc connection
API_VERSION = '1.18'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config-sample.json'

def get_config():
    with open(CONFIG_LOCATION, 'r') as f:
        return json.load(f)

def put_config(config):
    with open(CONFIG_LOCATION, 'w') as f:
        json.dump(config, f, sort_keys=True)


def build_mssql_connection():
    return pyodbc.connect('DRIVER={' + config['dbdriver'] + '}' +
                        ';SERVER=' + config['dbserver'] +
                        ';DATABASE=' + config['dbdatabase'] +
                        ';Trusted_Connection=yes;',autocommit=True)


config = get_config()
db_conn_params = {'dbname': config['dbdatabase']}

import sqlalchemy
import urllib


def batch_update_db(dataframe,table,batch_size=10000):
    # Updates the dataframe in batchmode
    sql_connection = build_mssql_connection()
    cursor = sql_connection.cursor() 
    num_batches = len(dataframe) // batch_size + 1 
    
    params = urllib.parse.quote_plus("DRIVER={" + config['dbdriver'] + "};"
                                 "SERVER=" + config['dbserver'] +
                                 ";DATABASE=" + config['dbdatabase'] +
                                 ";Trusted_Connection=yes;")
                                 
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

    for i in range(num_batches):
        #create_statement = fts.fast_to_sql(
        print("Processing batch number : ",i," out of ",num_batches,"batches")
        dataframe[i * batch_size : (i+1) * batch_size].to_sql(
            table,
            engine,
            if_exists = "append",index=False)
        sql_connection.commit()
        
    sql_connection.close()
    print("Updated the table : ",table)
    
def unzip_and_update_db(response_content, db_conn_params, table):
    with io.BytesIO(response_content) as response_stream:
        with zipfile.ZipFile(response_stream) as zipped_data_set:
            files = zipped_data_set.namelist()
            assert len(files) == 1
            csv_name = files[0]
            
            with zipped_data_set.open(csv_name) as csv_file:
                tic = time.perf_counter()
                dataframe = pd.read_csv(csv_file)
                # print(dataframe.head())
                batch_update_db(dataframe,table)
                toc = time.perf_counter()
                print("Updated the table in ", (toc-tic) / 60.00 , "minutes")
                
table_name = 'QuestionLibrary'
# table_name = 'SystemAccessLog'

config = get_config()
config['auth_service'] = config.get('auth_service', AUTH_SERVICE)
token_response = trade_in_refresh_token(config)

# Store the new refresh token for getting a new access token next run
config['refresh_token'] = token_response['refresh_token']
put_config(config)

data_set_metadata = FULL_DATA_SET_METADATA
plugin_to_link = get_plugin_link_mapping(config, token_response['access_token'])
db_conn_params = {'dbname': config['dbdatabase']}
    
for plugin, table in data_set_metadata:
    # ToDo - remove this for full automation 
    if table == table_name:
        response = get_with_auth(
                   endpoint=plugin_to_link[plugin],
                   access_token=token_response['access_token'])
        print("Using plugin : ",plugin," for the table : ",table)
        unzip_and_update_db(response.content, db_conn_params, table)
    else:
        continue 
        
