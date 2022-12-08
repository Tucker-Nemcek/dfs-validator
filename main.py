import os
import pyspark
import boto3
import json
import datetime
import time
import sys
from botocore.exceptions import ClientError

client = boto3.client('athena', region_name='us-east-1' )
s3 = boto3.resource('s3')

query = """
select count(*) from dd.weblog_superset_dt_2021102922 where device is null;
"""
DATABASE = 'dd'
output = "s3://sovrn-data-working-prd/dfs-validator/"


#session = boto3.Session(
#    aws_access_key_id='ASIA242ZFWYZVULFQCAY',
#    aws_secret_access_key='111CzwGSh9U93UQpLLhjtP+5Oj7pmA/hAZ02Nh6d',
#)




def execute_query(): 
    response_query_execution_id = client.start_query_execution(
        QueryString = query,
        QueryExecutionContext = {
            'Database' : DATABASE
        },
        ResultConfiguration={
            'OutputLocation' : output,
        }
    )

    response_get_query_details = client.get_query_execution(QueryExecutionId = response_query_execution_id['QueryExecutionId'])

    status = 'RUNNING'
     #check for status of query execution every second for 5 seconds. TODO: adjust this based on needs
    iterations = 5
    while(iterations > 0):
        iterations = iterations -1
        status = response_get_query_details['QueryExecution']['Status']['State']
        print(status)
        if(status == 'FAILED') or (status == 'CANCELLED'):
            print('ansible job failed')
            return False
        elif status == 'SUCCEEDED':
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            #returns query results
            response_query_result = client.get_query_results(QueryExecutionId = response_query_execution_id['QueryExecutionId'])
            print('location ' + location)
            rowheaders = response_query_result['ResultSet']['Rows'][0]['Data'] #I think I should replace this with the data that I actually want to see.
            for row in response_query_result['ResultSet']['Rows']:
                print(row)

            return True
        else:
            time.sleep(1)

execute_query()