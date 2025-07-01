# data_pipeline_project/pipeline_framework/airflow_tasks/source_to_stage_transfer.py

from typing import Dict, Any
import pendulum
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
from data_pipeline_project.pipeline_framework.database.pipeline_operations import stage_deletion, upload_record_to_database

def clean_stage_location(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Clean stage location using stage_deletion"""
    return stage_deletion(final_config, record)


def transfer_source_to_stage(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Main transfer function from source to stage using elasticdump"""
    return execute_elasticdump_transfer(final_config, record)


def reset_on_failure(final_config: Dict[str, Any], record: Dict[str, Any]) -> None:
    """Reset fields on failure - source_to_stage specific and pipeline fields"""
    timezone = final_config['timezone']
    
    # Reset source-to-stage specific fields
    record['SOURCE_TO_STAGE_INGESTION_START_TIME'] = None
    record['SOURCE_TO_STAGE_INGESTION_END_TIME'] = None
    record['SOURCE_TO_STAGE_INGESTION_STATUS'] = 'PENDING'
    
    # Reset pipeline fields
    record['PIPELINE_START_TIME'] = None
    record['PIPELINE_END_TIME'] = None
    record['PIPELINE_STATUS'] = 'PENDING'
    
    # Update retry and last updated time
    record['RETRY_ATTEMPT'] = record.get('RETRY_ATTEMPT', 0) + 1
    record['RECORD_LAST_UPDATED_TIME'] = pendulum.now(timezone).to_iso8601_string()
    
    log.info("Fields reset on failure", log_key="FailureReset", status="RESET_COMPLETE")

@log_execution_time
def source_to_stage_task(final_config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """Main source-to-stage orchestrator with error handling"""
    timezone = final_config['timezone']
    log.info("Starting source-to-stage task", log_key="SourceToStage", status="STARTED")
    
    try:
        # Step 1: Check if already completed (direct check)
        if record.get('SOURCE_TO_STAGE_INGESTION_STATUS') == 'COMPLETED':
            log.info("Transfer already completed - exiting", log_key="SourceToStage", status="ALREADY_COMPLETE")
            return {'success': True, 'reason': 'Already completed', 'record': record}
        
        # Step 2: Set start time
        record['SOURCE_TO_STAGE_INGESTION_START_TIME'] = pendulum.now(timezone).to_iso8601_string()
        
        # Step 3: Clean stage location
        if not clean_stage_location(final_config, record):
            raise Exception("Stage cleanup failed")
        
        # Step 4: Main transfer (YOUR FUNCTION - keeping placeholder)
        if not transfer_source_to_stage(final_config, record):
            raise Exception("Transfer failed")
        
        # Step 5: Set end time and update status/phase
        record['SOURCE_TO_STAGE_INGESTION_END_TIME'] = pendulum.now(timezone).to_iso8601_string()
        record['SOURCE_TO_STAGE_INGESTION_STATUS'] = 'COMPLETED'
        record['PHASE_COMPLETED'] = 'SOURCE_TO_STAGE'
        record['RECORD_LAST_UPDATED_TIME'] = pendulum.now(timezone).to_iso8601_string()
        
        # Step 6: Upload updated record
        upload_record_to_database(final_config, record)
        
        log.info("Source-to-stage task completed successfully", log_key="SourceToStage", status="SUCCESS")
        return {'success': True, 'reason': 'Transfer completed', 'record': record}
        
    except Exception as e:
        log.error(f"Source-to-stage task failed: {str(e)}", log_key="SourceToStage", status="FAILED")
        
        # Cleanup partial data
        clean_stage_location(final_config, record)
        
        # Reset all relevant fields on failure
        reset_on_failure(final_config, record)
        
        # Upload reset record
        upload_record_to_database(final_config, record)
        
        return {'success': False, 'reason': f'Transfer failed: {str(e)}', 'record': record}
    
    finally:
        log.info("Source-to-stage task finished", log_key="SourceToStage", status="FINISHED")



def build_elasticdump_command(final_config: Dict[str, Any], record: Dict[str, Any]) -> str:
    """Build elasticdump command for ES to S3 transfer"""
    
    # Extract details from config and record
    es_host = final_config['host_name']
    es_port = final_config['port']
    es_index = final_config['index_id']
    es_username = final_config['es_username']
    es_password = final_config['es_password']
    timestamp_field = final_config['timestamp_field']
    
    # AWS credentials from config
    aws_access_key = final_config['aws_access_key_id']
    aws_secret_key = final_config['aws_secret_access_key']
    aws_region = final_config.get('aws_region', 'us-east-1')
    
    # S3 details
    s3_uri = record['STAGE_SUBCATEGORY']  # s3://bucket/f1/f2/f3/f4/f5/
    
    # Generate filename: {index_id}_{epoch_time}.json using current time
    epoch_time = int(pendulum.now().timestamp())
    filename = f"{es_index}_{epoch_time}.json"
    s3_output_path = f"{s3_uri}{filename}"
    
    # Build ES source URL with auth
    es_source_url = f"http://{es_username}:{es_password}@{es_host}:{es_port}/{es_index}"
    
    # Build query for time window
    query = {
        "query": {
            "range": {
                timestamp_field: {
                    "gte": record['WINDOW_START_TIME'].to_iso8601_string(),
                    "lt": record['WINDOW_END_TIME'].to_iso8601_string()
                }
            }
        }
    }
    
    # Build elasticdump command with AWS credentials directly
    cmd = [
        "elasticdump",
        f"--input={es_source_url}",
        f"--output={s3_output_path}",
        f"--searchBody='{json.dumps(query)}'",
        f"--limit={final_config.get('max_file_size_limit', 5000)}",
        f"--retryAttempts={final_config.get('default_elasticdump_retries', 3)}",
        f"--retryDelayBase={final_config.get('retry_delay', 5000)}",
        "--type=data",
        f"--awsAccessKeyId={aws_access_key}",
        f"--awsSecretAccessKey={aws_secret_key}",
        f"--awsRegion={aws_region}"
    ]
    
    return " ".join(cmd)

def transfer_source_to_stage(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Execute elasticdump transfer from ES to S3"""
    
    try:
        # Build command (AWS creds included in command)
        cmd = build_elasticdump_command(final_config, record)
        
        log.info(f"Starting elasticdump transfer", log_key="ElasticDump", 
                status="STARTED", s3_output=record['STAGE_SUBCATEGORY'])
        
        # Execute command without environment variables
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        # Parse output
        if result.returncode == 0:
            # Success - parse output for transferred count
            output_lines = result.stdout.strip().split('\n')
            transferred_count = 0
            
            for line in output_lines:
                if 'got' in line and 'objects from source' in line:
                    try:
                        transferred_count = int(line.split()[1])
                    except:
                        pass
            
            log.info(f"Elasticdump transfer completed successfully", log_key="ElasticDump", 
                    status="SUCCESS", transferred_count=transferred_count,
                    s3_output=record['STAGE_SUBCATEGORY'])
            
            return True
        else:
            log.error(f"Elasticdump transfer failed", log_key="ElasticDump", 
                     status="FAILED", return_code=result.returncode,
                     error_output=result.stderr[:500])
            
            return False
            
    except subprocess.TimeoutExpired:
        log.error("Elasticdump transfer timed out", log_key="ElasticDump", status="TIMEOUT")
        return False
    except Exception as e:
        log.error(f"Elasticdump execution failed: {str(e)}", log_key="ElasticDump", status="EXECUTION_ERROR")
        return False









if __name__ == "__main__":
    # Test
    test_config = {
        'timezone': 'America/Los_Angeles',
        'sf_creds': {'user': 'test', 'password': 'test', 'account': 'test'},
        'sf_config': {'warehouse': 'test', 'database': 'test', 'schema': 'test'}
    }
    test_record = {
        'STAGE_SUBCATEGORY': 's3://test-bucket/test/path/',
        'SOURCE_TO_STAGE_INGESTION_STATUS': 'PENDING'
    }
    
    result = source_to_stage_task(test_config, test_record)
    print(f"Result: {result}")