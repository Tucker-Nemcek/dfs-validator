import boto3
import get_time_values

client = boto3.client('glue')

dt_path = get_time_values.get_todays_first_dt_path()

def create_crawler() :
    response = client.create_crawler(
        Name='dfs_validator_crawler',
        Role='df-glue-common-role-prd-ue1',
        DatabaseName='dd',
        Description='Crawler for validating Daas-Feeds-Spark',
        Targets={
            'S3Targets': [
                {
                    'Path': dt_path,
                    'SampleSize': 1
                }
            ]
        },
        TablePrefix='dfs_validator_glue_table_',
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
        },
        Configuration='{ "Version": 1.0, "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }'
    )
    print('crawler created')


def start_crawler():
    response = client.start_crawler(
        Name='dfs_validator_crawler'
    )
    print('crawler started')


def delete_crawler():
    response = client.delete_crawler(
    Name='dfs_validator_crawler'
)
    print('crawler deleted')
