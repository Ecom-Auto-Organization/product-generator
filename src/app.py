import json
import logging
from dataaccess.data_access import DataAccess
from datamodel.custom_enums import JobStatus, TaskType
from utility.product_generator import ProductGenerator


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    dataAccess = DataAccess()
    message_payload = json.loads(event['Records'][0]['Sns']['Message'])
    file_id = message_payload['fileId']
    job_id = message_payload['jobId']
    
    try:
        file_obj = dataAccess.get_file(file_id)
        job = dataAccess.get_job(job_id)
        dataAccess.basic_job_update({
                'id': job_id,
                'user_id': job['user_id'],
                'status': JobStatus.PREPARING.name
            })
        product_file_content = dataAccess.get_product_file(file_obj['s3_key'])

        product_generator_info = {
            'file_object': file_obj,
            'file_content': product_file_content, 
            'job_type': TaskType[job['type']],
            'options': job['options'] 
        }
        product_generator = ProductGenerator(product_generator_info)
        products = product_generator.get_products()
        prepared_products_file_key = 'products' + '_job_id_' + job_id + '.json'
        dataAccess.save_prepared_products(prepared_products_file_key, json.dumps(products))
        dataAccess.basic_job_update({
            'id': job_id,
            'user_id': job['user_id'],
            'total_products': len(products),
            'current_batch': 1,
            'input_products': prepared_products_file_key
        })
        dataAccess.publish_to_product_processor({
            'jobId': job_id
        })
    except Exception as error:
        logging.error('Job failed to prepare products. Details: %s', error)
        dataAccess.basic_job_update({
                'id': job_id,
                'user_id': job['user_id'],
                'status': JobStatus.FAILED.name
            })
    return None
