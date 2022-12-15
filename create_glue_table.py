import boto3

client = boto3.client('glue')

#end result is: dfs-validator-crawl-tabledt_2022121500

def create_crawler() :
    response = client.create_crawler(
        Name='dfs_validator_crawler',
        Role='df-glue-common-role-prd-ue1',
        DatabaseName='dd',
        Description='Crawler for validating Daas-Feeds-Spark',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://sovrn-prd-ue2-general-data/weblog-superset/datasource=requests/dt=2022121500/',
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

create_crawler()

def start_crawler():
    response = client.start_crawler(
        Name='dfs_validator_crawler'
    )
    print('crawler started')
start_crawler()

# def delete_crawler():
#     response = client.delete_crawler(
#         Name="dfs_validator_crawler"
#     )
#     print('crawler deleted')
# delete_crawler()

# response = client.update_table(
#     DatabaseName='dd',
#     TableInput={
#         'Name': 'dfs_validator_crawler_',
#         'Description': 'Table Sales',
#         'StorageDescriptor': {
#             'SerdeInfo': {
#                 'Name': 'OpenCSVSerde',
#                 'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
#                 'Parameters': {
#                     'separatorChar': ','
#                 }
#             }
#         }
#     }
# )
