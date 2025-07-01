# data_pipeline_project/pipeline_framework/database/pipeline_operations.py

from typing import Dict, Any, Optional, List
import pendulum
import boto3
from botocore.exceptions import ClientError
from data_pipeline_project.pipeline_framework.database.query_executor import get_query_executor
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils import setup_pipeline_logger

log = setup_pipeline_logger(logger_name= "PipelineLogger_PendingJobs", max_depth=5)

def upload_record_to_database(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Insert or update record in pipeline_records table"""
    executor = get_query_executor(final_config['sf_creds'], final_config['sf_config'], final_config)
    
    try:
        drive_table = final_config['drive_table']
        
        if 'PIPELINE_ID' in record and record['PIPELINE_ID']:
            # Update existing record
            columns_to_update = [col for col in record.keys() if col != 'PIPELINE_ID']
            set_clauses = [f"{col} = %({col})s" for col in columns_to_update]
            
            query = f"""
            UPDATE {drive_table} 
            SET {', '.join(set_clauses)}
            WHERE PIPELINE_ID = %(PIPELINE_ID)s
            """
        else:
            # Insert new record
            columns = list(record.keys())
            placeholders = [f"%({col})s" for col in columns]
            
            query = f"""
            INSERT INTO {drive_table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            """
        
        result = executor.execute_query(query, record)
        return result['success']
        
    except Exception as e:
        log.error(f"Failed to upload record: {str(e)}", log_key="DatabaseUpload", status="FAILED")
        return False

def download_oldest_pending_record(final_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Get oldest pending record ordered by WINDOW_END_TIME"""
    executor = get_query_executor(final_config['sf_creds'], final_config['sf_config'], final_config)
    
    try:
        drive_table = final_config['drive_table']
        
        query = f"""
        SELECT * FROM {drive_table} 
        WHERE SOURCE_NAME = %(SOURCE_NAME)s
        AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s  
        AND SOURCE_SUBCATEGORY = %(SOURCE_SUBCATEGORY)s
        AND STAGE_NAME = %(STAGE_NAME)s
        AND TARGET_NAME = %(TARGET_NAME)s
        AND PIPELINE_STATUS = %(PIPELINE_STATUS)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
        ORDER BY WINDOW_END_TIME ASC
        LIMIT 1
        """
        
        params = {
            'SOURCE_NAME': final_config['record']['SOURCE_NAME'],
            'SOURCE_CATEGORY': final_config['record']['SOURCE_CATEGORY'],
            'SOURCE_SUBCATEGORY': final_config['record']['SOURCE_SUBCATEGORY'],
            'STAGE_NAME': final_config['record']['STAGE_NAME'],
            'TARGET_NAME': final_config['record']['TARGET_NAME'],
            'PIPELINE_STATUS': 'PENDING',
            'PIPELINE_PRIORITY': final_config['record']['PIPELINE_PRIORITY']            
        }
        
        result = executor.execute_query(query, params)
        
        if result['success'] and result['data']['records']:
            log.info(f"Record downloaded successfully", log_key="DatabaseDownload", 
                    status="SUCCESS", query_id=result['query_id'])
            return result['data']['records'][0]
        
        log.info(f"No pending records found", log_key="DatabaseDownload", 
                status="NO_RECORDS", query_id=result['query_id'])
        return None
        
    except Exception as e:
        log.error(f"Failed to download record: {str(e)}", log_key="DatabaseDownload", status="FAILED")
        return None

def latest_timestamp_available(final_config: Dict[str, Any]) -> Optional[pendulum.DateTime]:
    """Get latest processed timestamp from database"""
    executor = get_query_executor(final_config['sf_creds'], final_config['sf_config'], final_config)
    
    try:
        drive_table = final_config['drive_table']
        
        query = f"""
        SELECT MAX(WINDOW_END_TIME) FROM {drive_table} 
        WHERE SOURCE_NAME = %(SOURCE_NAME)s
        AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s  
        AND SOURCE_SUBCATEGORY = %(SOURCE_SUBCATEGORY)s
        AND STAGE_NAME = %(STAGE_NAME)s
        AND TARGET_NAME = %(TARGET_NAME)s  
        """
        
        params = {
            'SOURCE_NAME': final_config['record']['SOURCE_NAME'],
            'SOURCE_CATEGORY': final_config['record']['SOURCE_CATEGORY'],
            'SOURCE_SUBCATEGORY': final_config['record']['SOURCE_SUBCATEGORY'],
            'STAGE_NAME': final_config['record']['STAGE_NAME'],
            'TARGET_NAME': final_config['record']['TARGET_NAME']
        }
        
        result = executor.execute_query(query, params)
        
        if result['success'] and result['data']['value']:
            log.info(f"Latest timestamp retrieved", log_key="LatestTimestamp", 
                    status="SUCCESS", query_id=result['query_id'], 
                    timestamp=result['data']['value'])
            return result['data']['value'] 
        
        log.info(f"No completed records found", log_key="LatestTimestamp", 
                status="NO_RECORDS", query_id=result['query_id'])
        return None
        
    except Exception as e:
        log.error(f"Failed to get latest timestamp: {str(e)}", log_key="LatestTimestamp", status="FAILED")
        return None

def get_target_count(final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Get count from source system for time window"""
    sf_raw_config = final_config.get('sf_raw_config', {})
    executor = get_query_executor(final_config['sf_creds'], sf_raw_config, final_config)

    try:
        filepattern = record["TARGET_SUBCATEGORY"]
        raw_table = sf_raw_config['raw_table']
        
        query = f"""
        SELECT COUNT(*) FROM {raw_table} 
        WHERE FILENAME LIKE '{filepattern}%'
        """
        
        params = {}
        
        result = executor.execute_query(query, params)
        
        if result['success']:
            log.info(f"Source count retrieved", log_key="SourceCount", 
                    status="SUCCESS", query_id=result['query_id'], 
                    count=result['data']['value'], filepattern=filepattern)
        
        return result['data']['value'] if result['success'] else 0
        
    except Exception as e:
        log.error(f"Failed to get source count: {str(e)}", log_key="SourceCount", status="FAILED")
        return 0

def target_deletion(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Delete target data for cleanup"""
    sf_raw_config = final_config.get('sf_raw_config', {})
    executor = get_query_executor(final_config['sf_creds'], sf_raw_config, final_config)

    try:
        filepattern = record["TARGET_SUBCATEGORY"]
        raw_table = sf_raw_config['raw_table']

        query = f"""
        DELETE FROM {raw_table} 
        WHERE FILENAME LIKE '{filepattern}%'
        """
        
        params = {}
        
        result = executor.execute_query(query, params)
        
        if result['success']:
            log.info(f"Target deletion completed", log_key="TargetDeletion", 
                    status="SUCCESS", query_id=result['query_id'], 
                    rows_affected=result['data']['rows_affected'], filepattern=filepattern)
        
        return result['success']
        
    except Exception as e:
        log.error(f"Failed to delete target data: {str(e)}", log_key="TargetDeletion", status="FAILED")
        return False

def get_all_in_progress_records(final_config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get all records with IN_PROGRESS status for cleanup"""
    executor = get_query_executor(final_config['sf_creds'], final_config['sf_config'], final_config)
    
    try:
        drive_table = final_config['drive_table']
        
        query = f"""
        SELECT * FROM {drive_table} 
        WHERE SOURCE_NAME = %(SOURCE_NAME)s
        AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s  
        AND SOURCE_SUBCATEGORY = %(SOURCE_SUBCATEGORY)s
        AND STAGE_NAME = %(STAGE_NAME)s
        AND TARGET_NAME = %(TARGET_NAME)s
        AND PIPELINE_STATUS = %(PIPELINE_STATUS)s
        AND PIPELINE_PRIORITY = %(PIPELINE_PRIORITY)s
        ORDER BY WINDOW_END_TIME ASC
        """
        
        params = {
            'SOURCE_NAME': final_config['record']['SOURCE_NAME'],
            'SOURCE_CATEGORY': final_config['record']['SOURCE_CATEGORY'],
            'SOURCE_SUBCATEGORY': final_config['record']['SOURCE_SUBCATEGORY'],
            'STAGE_NAME': final_config['record']['STAGE_NAME'],
            'TARGET_NAME': final_config['record']['TARGET_NAME'],
            'PIPELINE_STATUS': 'IN_PROGRESS',
            'PIPELINE_PRIORITY': final_config['record']['PIPELINE_PRIORITY']
        }
        
        result = executor.execute_query(query, params)
        
        if result['success']:
            log.info(f"In-progress records retrieved", log_key="InProgressRecords", 
                    status="SUCCESS", query_id=result['query_id'], 
                    record_count=len(result['data']['records']))
            return result['data']['records']
        
        log.info(f"No in-progress records found", log_key="InProgressRecords", 
                status="NO_RECORDS", query_id=result['query_id'])
        return []
        
    except Exception as e:
        log.error(f"Failed to get in-progress records: {str(e)}", log_key="InProgressRecords", status="FAILED")
        return []

def update_record_in_database(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Update single record by PIPELINE_ID"""
    executor = get_query_executor(final_config['sf_creds'], final_config['sf_config'], final_config)
    
    try:
        drive_table = final_config['drive_table']
        pipeline_id = record['PIPELINE_ID']
        
        # Build dynamic update query for all columns except PIPELINE_ID
        columns_to_update = [col for col in record.keys() if col != 'PIPELINE_ID']
        set_clauses = [f"{col} = %({col})s" for col in columns_to_update]
        
        query = f"""
        UPDATE {drive_table} 
        SET {', '.join(set_clauses)}
        WHERE PIPELINE_ID = %(PIPELINE_ID)s
        """
        
        result = executor.execute_query(query, record)
        
        if result['success']:
            log.info(f"Record updated successfully", log_key="RecordUpdate", 
                    status="SUCCESS", query_id=result['query_id'], 
                    pipeline_id=pipeline_id)
        
        return result['success']
        
    except Exception as e:
        log.error(f"Failed to update record: {str(e)}", log_key="RecordUpdate", status="FAILED")
        return False


def stage_deletion(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Delete S3 stage data for cleanup"""

    
    try:
        s3_uri = record['STAGE_SUBCATEGORY']
        
        # Parse S3 URI
        if not s3_uri.startswith('s3://'):
            log.error(f"Invalid S3 URI format: {s3_uri}", log_key="StageDeletion", status="INVALID_URI")
            return False
        
        # Extract bucket and prefix from s3://bucket/f1/f2/f3/f4/f5/
        uri_parts = s3_uri[5:]  # Remove 's3://'
        bucket_name = uri_parts.split('/')[0]
        prefix = '/'.join(uri_parts.split('/')[1:])
        
        # Create S3 client with credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=final_config['aws_access_key_id'],
            aws_secret_access_key=final_config['aws_secret_access_key'],
            region_name=final_config.get('aws_region', 'us-east-1')
        )
        
        log.info(f"Starting S3 cleanup", log_key="StageDeletion", 
                status="STARTED", bucket=bucket_name, prefix=prefix)
        
        # List and delete objects
        deleted_count = 0
        paginator = s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                
                if objects_to_delete:
                    response = s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    deleted_count += len(response.get('Deleted', []))
        
        log.info(f"S3 stage deletion completed", log_key="StageDeletion", 
                status="SUCCESS", bucket=bucket_name, prefix=prefix, 
                deleted_objects=deleted_count)
        
        return True
        
    except ClientError as e:
        log.error(f"AWS S3 error during stage deletion: {str(e)}", log_key="StageDeletion", status="AWS_ERROR")
        return False
    except Exception as e:
        log.error(f"Failed to delete stage data: {str(e)}", log_key="StageDeletion", status="FAILED")
        return False



