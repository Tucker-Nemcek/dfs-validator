import glue_actions
import athena_query
import time


def lambda_handler(event, context):
    try 
    glue_actions.create_crawler()
    time.sleep(30)
    glue_actions.start_crawler()
    time.sleep(300)
    athena_query.execute_query()
    time.sleep(120)
    glue_actions.delete_crawler()
