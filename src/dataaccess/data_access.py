import logging
import boto3
import json
from botocore.exceptions import ClientError
from datamodel.custom_exceptions import DataAccessError
from boto3.dynamodb.conditions import Key
from dataaccess import data_model_utils
from utility import utils
import os


class DataAccess:
    """ 
    Class for getting data and adding data to database and other sources

    """

    def __init__(self):
        self._upload_bucket = os.environ.get('s3_file_upload_bucket')
        self._prepared_products_bucket = os.environ.get('prepared_products_bucket')
        self._s3_client = boto3.client('s3')
        bulk_manager_table = os.environ.get('bulk_manager_table')
        self._dynamodb = boto3.resource('dynamodb')
        self._bulk_manager_table =  self._dynamodb.Table(bulk_manager_table) 
        self._sns_client = boto3.client('sns')


    def get_file(self, file_id):
        file_to_get = {'id': file_id}
        db_file = data_model_utils.convert_to_db_file(file_to_get)

        try:
            response = self._bulk_manager_table.get_item(Key=db_file)
            file_obj = None
            if 'Item' in response:
                db_file = response['Item']
                file_obj = data_model_utils.extract_file_details(db_file)
            return file_obj 
        except ClientError as error:
            raise DataAccessError(error)


    def get_job(self, job_id):
        job = utils.join_str('job#', job_id)
        prefix = 'user'

        try:
            response = self._bulk_manager_table.query(
                KeyConditionExpression=Key('PK').eq(job) & Key('SK').begins_with(prefix)
            )

            job = None
            if len(response['Items']) > 0:
                db_job = response['Items'][0]
                job = data_model_utils.extract_job_details(db_job)
            return job
        except ClientError as error:
            raise DataAccessError(error)

    
    def get_product_file (self, file_key):
        try:
            response = self._s3_client.get_object (
                Bucket=self._upload_bucket,
                Key=file_key
            )
            return response['Body'].read()
        except ClientError as error:
            raise DataAccessError(error)


    def basic_job_update (self, job):
        if 'id' not in job or 'user_id' not in job:
            raise KeyError('\'id\' and \'user_id\' value for job cannot be null')
        
        db_job = data_model_utils.convert_to_db_job(job)
        # assign primary key to Keys Attribute and remove primary keys 
        # from db_job since we don't intent to modify them
        primary_key = {'PK': db_job['PK'], 'SK': db_job['SK']}
        del db_job['PK']
        del db_job['SK']

        expression_attr_values = utils.get_expression_attr_values(db_job)
        update_expression = utils.get_update_expression(expression_attr_values)

        try:
            response = self._bulk_manager_table.update_item(
                Key=primary_key,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attr_values,
                ReturnValues='UPDATED_NEW'
            )

            logging.info('Updated job successfully: %s', response)
            return True
        except ClientError as error:
            raise DataAccessError(error)
        except Exception as error:
            raise DataAccessError(error)


    def save_prepared_products(self, file_key, file_content):
        try:
            response = self._s3_client.put_object (
                Bucket=self._prepared_products_bucket,
                Body=file_content,
                Key=file_key
            )
            return True
        except ClientError as error:
            raise DataAccessError(error)


    def publish_to_product_processor(self, message):
        import_topic = os.environ.get('import_topic_arn')
        try:
            response = self._sns_client.publish(
                TopicArn=import_topic,
                Message=json.dumps(message),
                MessageAttributes={
                    'process': {
                        'DataType': 'String',
                        'StringValue': 'process-product'
                    }
                }
            )
            if 'MessageId' in response:
                return True
        except Exception as error:
            raise Exception('Could not publish message successfully. Error:' + str(error))