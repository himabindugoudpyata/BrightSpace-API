import pandas as pd
import numpy as np 
from datetime import datetime
import argparse
import csv
import os
import time 
import sqlalchemy
import urllib
import json
import seaborn as sns
import matplotlib.pyplot as plt
%matplotlib inline

# Create a pyodbc connection
CONFIG_LOCATION = 'config-sample.json'

def get_config():
    with open(CONFIG_LOCATION, 'r') as f:
        return json.load(f)
    
def build_sql_engine():
    config = get_config()
    params = urllib.parse.quote_plus("DRIVER={" + config['dbdriver'] + "};"
                                 "SERVER=" + config['dbserver'] +
                                 ";DATABASE=" + config['dbdatabase'] +
                                 ";Trusted_Connection=yes;")
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
    return engine 
    
table_name = "tmp_data_combined_3"
sql_engine = build_sql_engine()
table_df = pd.read_sql_table(
    table_name,
    con=sql_engine
)

feature_list = ['NumAssignmentSubmissions','NumQuizDone','NumPosts','NumTopics','NumChatSessions']
table_df[feature_list].describe()

# add a zero row for baseline for all courses 
# Adding dummy records to force-fit 0 into the feature columns 
table_df = pd.concat([table_df,
    pd.DataFrame({
    'UserId' : '9999999999',
    'OrgUnitId' : pd.unique(table_df.OrgUnitId),
    'Organization' : 'NA',
    'Type' : 'NA',
    'CourseName' : 'NA',
    'EnrollmentDate' : max(table_df.EnrollmentDate),
    'NumAssignmentSubmissions' : 0,
    'NumQuizDone' : 0,
    'NumPosts' : 0,
    'NumTopics' : 0 })]
    )
    
table_df['NumAssignmentSubmissions_pct'] = table_df.\
                                           groupby('OrgUnitId')['NumAssignmentSubmissions'].\
                                           rank(pct=True,method='max')

table_df['NumQuizDone_pct'] = table_df.\
                              groupby('OrgUnitId')['NumQuizDone'].\
                              rank(pct=True,method='max')

table_df['NumPosts_pct'] = table_df.\
                              groupby('OrgUnitId')['NumPosts'].\
                              rank(pct=True,method='max')

table_df['NumTopics_pct'] = table_df.\
                              groupby('OrgUnitId')['NumTopics'].\
                              rank(pct=True,method='max')

table_df['NumChatSessions_pct'] = table_df.\
                              groupby('OrgUnitId')['NumChatSessions'].\
                              rank(pct=True,method='max')
                              
                              
table_df['EngagementIndex'] =  (table_df['NumAssignmentSubmissions_pct'] +
                              table_df['NumQuizDone_pct'] + 
                              table_df['NumPosts_pct'] + 
                              table_df['NumTopics_pct'] + 
                              table_df['NumChatSessions_pct']) / 5.0 * 100.00

table_df = table_df[table_df.UserId != '9999999999']
# datetime formatting
table_df['EnrollmentDate'] = table_df.EnrollmentDate.dt.date

table_df.loc[:,'CourseTerm'] = table_df.loc[:,'CourseName'].\
                         apply(lambda x : ' '.join(word for word in x.split(' ')[-3:-1]))

table_df.loc[:,'CourseTitle'] = table_df.loc[:,'CourseName'].\
                                 apply(lambda x : ' '.join(word for word in x.split(' ')[:-5]))

table_df.loc[:,'CourseSection'] = table_df.loc[:,'CourseName'].\
    apply(lambda x : ' '.join(word for word in x.split(' ')[-5:-3]))

# table_df.to_csv("EngagementIndexResult.csv")
table_df[['UserId','OrgUnitId','Organization','Type','EnrollmentDate',
                    'EngagementIndex','CourseTerm','CourseTitle','CourseSection']].\
                     to_excel("EngagementIndexResult.xlsx")

                     