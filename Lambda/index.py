

#IMPORTS
import json
import boto3
import logging


#RESOURCES
s3_client = boto3.resource('s3')
glue_client = boto3.client('glue')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


#VARS
glue_job_name = "DQ_FIAP_PAN"
external_rules_key = "custom_rules.json"
external_rules_bucket = "fiap-pan-rules"
dq_metric_repository_bucket = "fiap-pan-dataquality"

def handler(event, context):
    
    dataframe_bucket = event['Records'][0]['s3']['bucket']['name']
    dataframe_key = event['Records'][0]['s3']['object']['key']
    dataframe_size = event['Records'][0]['s3']['object']['size']
    dataframe_uri = 's3://'+ dataframe_bucket + '/' + dataframe_key
    
    logger.info("- - DATAFRAM URI : " + dataframe_uri)
   
    response = glue_client.start_job_run(
             JobName = glue_job_name,
             Arguments = {
               '--dataframe_key': dataframe_uri,
               '--dataframe_size': str(dataframe_size),
               '--external_rules_key': external_rules_key,
               '--external_rules_bucket': external_rules_bucket,
               '--dq_metric_repository_bucket': dq_metric_repository_bucket } )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
