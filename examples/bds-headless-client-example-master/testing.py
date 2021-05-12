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

logger = logging.getLogger(__name__)

API_VERSION = '1.18'
AUTH_SERVICE = 'https://auth.brightspace.com/'
CONFIG_LOCATION = 'config-sample.json'

parser = argparse.ArgumentParser(description='Script for downloading data sets.')
parser.add_argument('--differential',action='store_true',help='Use differential data sets instead of full datas sets')
parser.add_argument("--table",help="specify the table that you want to update",type=str)
parser.add_argument("--batch_size",help="Specify the batch size for the DB upload",default=10000,type=int)
parser.add_argument("--fastexecutemany",help="Turning on the fastexecutemany option",default=False,type=bool)
parser.add_argument("--writesamplerows",help="write a sample csv file to disk",default=False,type=bool)
args = parser.parse_args()

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

DIFF_DATA_SET_METADATA = [
    DataSetMetadata(
        plugin='a78735f2-7210-4a57-aac1-e0f6bd714349',
        table='user_enrollments'
    )
]

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
    
def build_mssql_connection():
    return pyodbc.connect('DRIVER={' + config['dbdriver'] + '}' +
                        ';SERVER=' + config['dbserver'] +
                        ';DATABASE=' + config['dbdatabase'] +
                        ';Trusted_Connection=yes;',autocommit=True)

def get_number_of_columns(table):
    sql_connection = build_mssql_connection()
    cursor = sql_connection.cursor() 
    # Get the column names of the target table 
    cursor.execute(
                    '''
                    SELECT top 0 *
                    FROM {table}
    '''.format(table=table))
    column_contents_raw = cursor.description
    # Iterate through the tuple to get the column names 
    cols = []
    for tup in column_contents_raw:
        cols.append(tup[0])
    # print(cols)
    
    return cols

def process_csv_stream(csv_input_stream,num_columns_in_table):

    '''
    Ignore excessive columns in the CSV due to additive changes / BDS minor
    changes by ignoring any columns in the CSV past the number of columns in the
    table 
    '''
    
    csv_rows = []
    #print(num_columns_in_table)
    csv_reader = csv.reader(csv_input_stream, quoting=csv.QUOTE_MINIMAL)
    
    for line in csv_reader:
        csv_rows.append(tuple(line[:num_columns_in_table]))
        #print(csv_rows.next())
   
    return csv_rows

def update_db(table, data_rows,cols):
    '''
    In a single transaction, update the table by:
    - Loading the CSV data into a temporary table
    - Run an update or insert query to update the main table with the data in
      the temporary table
    - Delete the temporary table
    '''
    # print(get_number_of_columns(table))
    sql_connection = build_mssql_connection()
    cursor = sql_connection.cursor() 
    cursor.fast_executemany = args.fastexecutemany
    # print("Fast Execute many =",args.fastexecutemany)
    tmp_table_id = 'tmp_' + table
    
    print("\n Table being written to : ",table)
    cols_string = ','.join(col for col in cols)
    vals_string = '?,'*len(cols)
    # print("Columns : ",cols_string)
    cursor.setinputsizes([(pyodbc.SQL_WVARCHAR, 0, 0)])
    
    cursor.executemany(
    """
    INSERT INTO {table} 
    ({columns}) 
    VALUES({values})
    """.format(table=table,columns=cols_string,values=vals_string[:-1]), data_rows[2:])
    # cursor.commit()
    sql_connection.commit()
    cursor.close()
        

#def bulk_insert(tmp_table_id, csv_data):
    #sql_connection = build_mssql_connection()
    #cursor = sql_connection.cursor()
    
    
    #upsert_query_file = os.path.join(
    #os.path.dirname(os.path.abspath(__file__)),
    #'schema',
    #'upserts',
    #'upsert_{table}.sql'.format(table=table)
      #      )
    #with open(upsert_query_file) as upsert_query:
    #    cur.execute(upsert_query.read())
    #    cur.execute(sql.SQL('DROP TABLE {tmp_table}').format(tmp_table=tmp_table_id))

    #conn.commit()

def batch_update_db(db_conn_params, table, csv_file, batch_size=args.batch_size):
    # Remove the first row, which contains the headers
    csv_file.readline()
    cols = get_number_of_columns(table)
    num_columns = len(cols)
    csv_input_stream = io.StringIO()

    def update_db_with_batch(input_stream):
        '''
        Helper method that forms a closure so we don't have to pass many of the
        values used in this method as arguments
        '''

        # Rewind the stream to the beginning before passing it on
        input_stream.seek(io.SEEK_SET)

        data_rows = process_csv_stream(input_stream, num_columns)
        
        #### Todo : Remove this later on 
        if args.writesamplerows == True:
            sample_csv_out_file = open('{file_name}_sample.csv'.format(file_name=table), 'w')
            writer = csv.writer(sample_csv_out_file,quoting=csv.QUOTE_MINIMAL)
            writer.writerow(data_rows)
        ######################################## 
        update_db(table, data_rows,cols)
        input_stream.close()

    i = 0
    cumulative = 0
    for line in csv_file:
        csv_input_stream.write(line.decode('utf-8'))
        i += 1

        if i == batch_size:
            update_db_with_batch(csv_input_stream)
            csv_input_stream = io.StringIO()
            cumulative += i
            print("Cumulative Number of records added so far : ",cumulative)
            i = 0
            # TODO : remove the below , this is for testing purpose only
            #return None 

    update_db_with_batch(csv_input_stream)
    print("Total Number of records added so far : ",cumulative+i)

def unzip_and_update_db(response_content, db_conn_params, table):
    with io.BytesIO(response_content) as response_stream:
        with zipfile.ZipFile(response_stream) as zipped_data_set:
            files = zipped_data_set.namelist()

            assert len(files) == 1
            csv_name = files[0]

            with zipped_data_set.open(csv_name) as csv_file:
                tic = time.perf_counter()
                batch_update_db(db_conn_params, table, csv_file)
                toc = time.perf_counter()
                print("Updated the table in ", (toc-tic) / 60.00 , "minutes")

if __name__ == '__main__':
    config = get_config()
    config['auth_service'] = config.get('auth_service', AUTH_SERVICE)

    token_response = trade_in_refresh_token(config)

    # Store the new refresh token for getting a new access token next run
    config['refresh_token'] = token_response['refresh_token']
    put_config(config)

    data_set_metadata = DIFF_DATA_SET_METADATA if args.differential else FULL_DATA_SET_METADATA
    plugin_to_link = get_plugin_link_mapping(config, token_response['access_token'])
    db_conn_params = {
        'dbname': config['dbdatabase']
    }
    
    for plugin, table in data_set_metadata:
        # ToDo - remove this for full automation 
        if table == args.table:
            response = get_with_auth(
                endpoint=plugin_to_link[plugin],
                access_token=token_response['access_token']
                )
            print("Using plugin : ",plugin," for the table : ",table)
            unzip_and_update_db(response.content, db_conn_params, table)
        else:
            continue 

        
