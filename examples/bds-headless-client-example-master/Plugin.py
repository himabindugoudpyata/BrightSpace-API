
##### Date Created   -- FEB-02-2021
##### Functaionality -- Retrieves a list of Brightspace Data Sets plugins that you have permission to see.
##### To run - python Plugin.py


##### First, we use the client ID and secret from the OAuth 2.0 application registration as well as the refresh token we obtained in the previous step to obtain an access token.
##### Refresh tokens are one-time use. However, we get a new refresh token with the access token response, so we store the new refresh token into config.json for next run.





import argparse
import collections
import csv
import datetime
import io
import json
import logging
import os
import zipfile
import pyodbc
import contextlib
import psycopg2
from psycopg2 import sql
import requests
from requests.auth import HTTPBasicAuth

CONFIG_LOCATION = 'config-sample.json'
AUTH_SERVICE = 'https://auth.brightspace.com/';

def get_config():
    with open(CONFIG_LOCATION, 'r') as f:
        return json.load(f)

def trade_in_refresh_token(config):
    # https://tools.ietf.org/html/rfc6749#section-6
    response = requests.post(
        '{}/core/connect/token'.format(AUTH_SERVICE),
        # Content-Type 'application/x-www-form-urlencoded'
        data={
            'grant_type': 'refresh_token',
            'refresh_token': config['refresh_token'],
            'scope': 'core:*:*'
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

config = get_config()
token_response = trade_in_refresh_token(config)

# Store the new refresh token for getting a new access token next run
config['refresh_token'] = token_response['refresh_token']
put_config(config)




endpoint = '{bspace_url}/d2l/api/lp/{lp_version}/dataExport/bds/list'.format(
    bspace_url=config['bspace_url'],
    lp_version=config['api_version'],
    
)
headers = {'Authorization': 'Bearer {}'.format(token_response['access_token'])}
response = requests.get(endpoint, headers=headers)
json_data = response.json()


# open a file for writing

csv_data = open('Plugins.csv', 'w')

# create the csv writer object

csvwriter = csv.writer(csv_data)

count = 0

for row in json_data:

    if count == 0:

        header = row.keys()

        csvwriter.writerow(header)

        count += 1

    csvwriter.writerow(row.values())



