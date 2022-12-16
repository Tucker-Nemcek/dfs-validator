import boto3
import time
import get_time_values

query = 'select count(*) from dfs_validator_glue_table_' + get_time_values.get_todays_first_dt() + 'where device is null;'


DATABASE = 'dd'
output = "s3://athena-sovrn-data-working-prd/dfs-validator/outputs"

def execute_query():
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('s3://athena-sovrn-data-working-prd/dfs-validator/outputs/')
    exists = True
    
    client = boto3.client('athena', region_name='us-east-2' ) #east-1 OPS, #east-2 DEV
    response_query_execution_id = client.start_query_execution( #gets QueryExecutionId
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
     #check for status of query execution every second for 5 seconds.
    iterations = 5
    while(iterations > 0):
        iterations = iterations -1
        response_get_query_details=client.get_query_execution(QueryExecutionId=response_query_execution_id['QueryExecutionId'])
        status = response_get_query_details['QueryExecution']['Status']['State']
        error = response_get_query_details['QueryExecution']['Status']
        print(status)
        if(status == 'FAILED') or (status == 'CANCELLED'):
            print(error)
            return False
        elif status == 'SUCCEEDED':
            location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']
            #returns query results
            response_query_result = client.get_query_results(QueryExecutionId = response_query_execution_id['QueryExecutionId'])
            print('location ' + location)
            rowheaders = response_query_result['ResultSet']['Rows'][0]['Data']
            print('headers ' +str(rowheaders))
            for row in response_query_result['ResultSet']['Rows']:
                print(row)
    
            return True
        else:
            print ('sleeping for 10 seconds')
            time.sleep(10)