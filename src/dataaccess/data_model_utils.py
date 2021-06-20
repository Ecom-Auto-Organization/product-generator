from utility import utils

def convert_to_db_file (file_obj):
    """Converts file object into object that can be used in database"""
    
    db_file = {'SK': 'file'}

    db_file['PK'] = utils.join_str('file#', file_obj['id'])
    if 'idle' in file_obj:
        db_file['SK1'] = utils.join_str('idle#', file_obj['idle'])
    if 'file_name' in file_obj:
        db_file['file_name'] = file_obj['file_name']
    if 'file_type' in file_obj:
        db_file['file_type'] = file_obj['file_type']
    if 's3_key' in file_obj:
        db_file['s3_key'] = file_obj['s3_key']
    if 'actual_row_count' in file_obj:
        db_file['actual_row_count'] = file_obj['actual_row_count']
    if 'header_row' in file_obj:
        db_file['header_row'] = file_obj['header_row']
    if 'field_details' in file_obj:
        db_file['field_details'] = file_obj['field_details']

    return db_file


def extract_file_details(db_file):
    delimeter = '#'
    file = {}
    file['id'] = utils.extract_str(db_file['PK'], delimeter, 1)
    if 'SK1' in db_file:
        file['idle'] = utils.extract_str(db_file['SK1'], delimeter, 1)
    if 'file_name' in db_file:
        file['file_name'] = db_file['file_name']
    if 'file_type' in db_file:
        file['file_type'] = db_file['file_type']
    if 's3_key' in db_file:
        file['s3_key'] = db_file['s3_key']
    if 'actual_row_count' in db_file:
        file['actual_row_count'] = db_file['actual_row_count']
    if 'header_row' in db_file:
        file['header_row'] = db_file['header_row']
    if 'field_details' in db_file:
        file['field_details'] = db_file['field_details']
    
    return file


def convert_to_db_job (job):
    """Converts job object into object that can be used in database"""
    
    db_job = {'SK': 'file'}

    db_job['PK'] = utils.join_str('job#', job['id'])
    db_job['SK'] = utils.join_str('user#', job['user_id'])
    if 'status' in job:
        db_job['SK1'] = utils.join_str('status#', job['status'])
    if 'type' in job:
        db_job['SK2'] = utils.join_str('job_type#', job['type'])
    if 'total_products' in job:
        db_job['total_products'] = job['total_products']
    if 'total_success' in job:
        db_job['total_success'] = job['total_success']
    if 'total_failed' in job:
        db_job['total_failed'] = job['total_failed']
    if 'edit_rules' in job:
        db_job['edit_rules'] = job['edit_rules']
    if 'start_time' in job:
        db_job['start_time'] = job['start_time']
    if 'current_batch' in job:
        db_job['current_batch'] = job['current_batch']
    if 'input_products' in job:
        db_job['input_products'] = job['input_products']
    if 'options' in job:
        db_job['options'] = job['options']

    return db_job


def extract_job_details(db_job):
    delimeter = '#'
    job = {}
    job['id'] = utils.extract_str(db_job['PK'], delimeter, 1)
    job['user_id'] = utils.extract_str(db_job['SK'], delimeter, 1)
    if 'SK1' in db_job:
        job['status'] = utils.extract_str(db_job['SK1'], delimeter, 1)
    if 'SK2' in db_job:
        job['type'] = utils.extract_str(db_job['SK2'], delimeter, 1)
    if 'total_products' in db_job:
        job['total_products'] = db_job['total_products']
    if 'total_success' in db_job:
        job['total_success'] = db_job['total_success']
    if 'total_failed' in db_job:
        job['total_failed'] = db_job['total_failed']
    if 'edit_rules' in db_job:
        job['edit_rules'] = db_job['edit_rules']
    if 'start_time' in db_job:
        job['start_time'] = db_job['start_time']
    if 'current_batch' in db_job:
        job['current_batch'] = db_job['current_batch']
    if 'input_products' in db_job:
        job['input_products'] = db_job['input_products']
    if 'options' in db_job:
        job['options'] = db_job['options']
    
    return job