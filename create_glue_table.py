import boto3
import get_time_values

client = boto3.client('glue')

#end result is: dfs-validator-crawl-tabledt_2022121500

get_dt_path = get_time_values.get_todays_dt_path()
print(get_dt_path)

# def create_crawler() :
#     response = client.create_crawler(
#         Name='dfs_validator_crawler',
#         Role='df-glue-common-role-prd-ue1',
#         DatabaseName='dd',
#         Description='Crawler for validating Daas-Feeds-Spark',
#         Targets={
#             'S3Targets': [
#                 {
#                     'Path': 's3://sovrn-prd-ue2-general-data/weblog-superset/datasource=requests/dt=2022121500/',
#                     'SampleSize': 1
#                 }
#             ]
#         },
#         TablePrefix='dfs_validator_glue_table_',
#         SchemaChangePolicy={
#             'UpdateBehavior': 'UPDATE_IN_DATABASE',
#         },
#         Configuration='{ "Version": 1.0, "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }'
#     )
#     print('crawler created')

# create_crawler()

# def start_crawler():
#     response = client.start_crawler(
#         Name='dfs_validator_crawler'
#     )
#     print('crawler started')
# start_crawler()
