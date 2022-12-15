import boto3

client = boto3.client('glue')

def create_crawler() :
    response = client.create_crawler(
        Name='dfs-validator_crawler',
        Role='df-glue-common-role-prd-ue1',
        DatabaseName='dd',
        Description='Crawler for validating Daas-Feed-Spark',
        Targets={
            'S3Targets': [
                {
                    'Path': 's3://sovrn-prd-ue2-general-data/weblog-superset/datasource=requests/dt=2022121500/',
                    'SampleSize': 1
                }
            ]
        },
        TablePrefix='dfs-validator-crawl-table',
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
        },
        Configuration='{ "Version": 1.0, "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }'
    )
    print('crawler created')

create_crawler()

# response = client.start_crawler(
#     Name='SalesCSVCrawler'
# )

# response = client.update_table(
#     DatabaseName='dd',
#     TableInput={
#         'Name': 'dfs-validator-crawler',
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
