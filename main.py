import glue_actions
import athena_query
import time
import logging
import boto3
import botocore



def lambda_handler(event, context):
    try :
        glue_actions.create_crawler()
        print("crawler created")
        time.sleep(30)
        glue_actions.start_crawler()
        print("crawler started")
        time.sleep(300)
        athena_query.execute_query()
        print("query executed")
        time.sleep(120)
        glue_actions.delete_crawler()
        print("crawler deleted")
        print("see output at s3://athena-sovrn-data-working-prd/dfs-validator/outputs/ in OPS")
    except botocore.exceptions.ClientError as error:
        logging.warn(error.response['Error']['Code'])
        raise Exception(error.response['Error']['Code'])    
