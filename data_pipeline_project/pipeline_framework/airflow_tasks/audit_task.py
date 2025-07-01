# data_pipeline_project/pipeline_framework/airflow_tasks/audit_task.py

import time
from typing import Dict, Any
import pendulum
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
from data_pipeline_project.pipeline_framework.database.pipeline_operations import get_target_count as db_get_target_count, target_deletion, upload_record_to_database, stage_deletion

def get_source_count(final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Get source count from Elasticsearch using elasticsearch-py"""
    from elasticsearch import Elasticsearch
    from elasticsearch.exceptions import ConnectionError, AuthenticationException
    import base64
    
    try:
        # Extract ES connection details
        es_host = final_config['host_name']
        es_port = final_config['port']
        es_index = final_config['index_id']
        es_username = final_config['es_username']
        es_password = final_config['es_password']
        timestamp_field = final_config['timestamp_field']  # From config
        
        # Create ES client
        es_url = f"http://{es_host}:{es_port}"
        
        # Basic auth headers
        auth_string = f"{es_username}:{es_password}"
        encoded_auth = base64.b64encode(auth_string.encode()).decode()
        
        es_client = Elasticsearch(
            [es_url],
            headers={'Authorization': f'Basic {encoded_auth}'},
            timeout=30,
            retry_on_timeout=True,
            max_retries=final_config.get('default_elasticdump_retries', 3)
        )
        
        # Build query with time window using config timestamp field
        query_body = {
            "query": {
                "range": {
                    timestamp_field: {  # Dynamic timestamp field from config
                        "gte": record['WINDOW_START_TIME'].to_iso8601_string(),
                        "lt": record['WINDOW_END_TIME'].to_iso8601_string()
                    }
                }
            }
        }
        
        log.info(f"Querying Elasticsearch for count", log_key="ESCount", 
                status="STARTED", es_host=es_host, es_port=es_port, 
                es_index=es_index, timestamp_field=timestamp_field,
                time_window=f"{record['WINDOW_START_TIME']} to {record['WINDOW_END_TIME']}")
        
        # Execute count query
        response = es_client.count(index=es_index, body=query_body)
        count = response['count']
        
        log.info(f"Elasticsearch count retrieved successfully", log_key="ESCount", 
                status="SUCCESS", es_index=es_index, timestamp_field=timestamp_field,
                count=count, query_time_ms=response.get('took', 'unknown'))
        
        return count
        
    except AuthenticationException as e:
        log.error(f"Elasticsearch authentication failed: {str(e)}", log_key="ESCount", status="AUTH_FAILED")
        return 0
    except ConnectionError as e:
        log.error(f"Elasticsearch connection failed: {str(e)}", log_key="ESCount", status="CONNECTION_FAILED")
        return 0
    except Exception as e:
        log.error(f"Failed to get Elasticsearch count: {str(e)}", log_key="ESCount", status="FAILED")
        return 0

def get_target_count(source_count: int, final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Get target count with Snowpipe loading monitoring"""
    
    # Get initial target count
    current_target_count = db_get_target_count(final_config, record)
    
    # If source <= target, return immediately
    if source_count <= current_target_count:
        log.info(f"Target count sufficient", log_key="TargetCount", 
                status="SUFFICIENT", source_count=source_count, target_count=current_target_count)
        return current_target_count
    
    # Monitor for up to 10 minutes
    # max_wait_time = 600  # 10 minutes
    # check_interval = 120  # 2 minutes

    max_wait_time = final_config.get('snowpipe_max_wait_seconds', 600)
    check_interval = final_config.get('snowpipe_check_interval_seconds', 120)

    start_time = time.time()
    previous_target_count = current_target_count
    
    log.info(f"Starting Snowpipe monitoring", log_key="TargetCount", 
            status="MONITORING_START", source_count=source_count, 
            initial_target_count=current_target_count, max_wait_minutes=10)
    
    while time.time() - start_time < max_wait_time:
        time.sleep(check_interval)
        
        # Get updated target count
        current_target_count = db_get_target_count(final_config, record)
        current_difference = source_count - current_target_count
        previous_difference = source_count - previous_target_count
        
        log.info(f"Snowpipe check", log_key="TargetCount", 
                status="MONITORING", source_count=source_count,
                current_target_count=current_target_count,
                current_difference=current_difference,
                previous_difference=previous_difference,
                elapsed_minutes=round((time.time() - start_time) / 60, 1))
        
        # If difference is reducing, update previous and continue
        if current_difference < previous_difference:
            log.info("Difference reducing, continuing monitoring", log_key="TargetCount", status="IMPROVING")
            previous_target_count = current_target_count
        else:
            # Difference not reducing, stop monitoring
            log.info("Difference not reducing, stopping monitoring", log_key="TargetCount", status="STABLE")
            break
    
    final_difference = source_count - current_target_count
    log.info(f"Snowpipe monitoring completed", log_key="TargetCount", 
            status="MONITORING_END", source_count=source_count,
            final_target_count=current_target_count, final_difference=final_difference,
            total_wait_minutes=round((time.time() - start_time) / 60, 1))
    
    return current_target_count

def calculate_differences(source_count: int, target_count: int) -> Dict[str, Any]:
    """Calculate count difference and percentage difference"""
    count_difference = abs(source_count - target_count)
    
    if source_count == 0:
        percentage_difference = 0.0 if target_count == 0 else 100.0
    else:
        percentage_difference = (count_difference / source_count) * 100.0
    
    return {
        'count_difference': count_difference,
        'percentage_difference': round(percentage_difference, 2)
    }

def clean_stage_location(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Clean stage location using stage_deletion"""
    return stage_deletion(final_config, record)

def clean_target_location(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Clean target location using target_deletion"""
    return target_deletion(final_config, record)

def reset_on_process_failure(final_config: Dict[str, Any], record: Dict[str, Any]) -> None:
    """Reset fields on audit process failure - keep phase_completed unchanged"""
    timezone = final_config['timezone']
    
    # Reset audit specific fields only
    record['AUDIT_START_TIME'] = None
    record['AUDIT_END_TIME'] = None
    record['AUDIT_STATUS'] = 'PENDING'
    record['AUDIT_RESULT'] = None
    record['SOURCE_COUNT'] = None
    record['TARGET_COUNT'] = None
    record['COUNT_DIFFERENCE'] = None
    record['PERCENTAGE_DIFFERENCE'] = None
    
    # Reset pipeline fields
    record['PIPELINE_START_TIME'] = None
    record['PIPELINE_END_TIME'] = None
    record['PIPELINE_STATUS'] = 'PENDING'
    
    # Update retry and last updated time
    record['RETRY_ATTEMPT'] = record.get('RETRY_ATTEMPT', 0) + 1
    record['RECORD_LAST_UPDATED_TIME'] = pendulum.now(timezone).to_iso8601_string()
    
    log.info("Fields reset on audit process failure", log_key="ProcessFailureReset", status="RESET_AUDIT_ONLY")

def reset_on_audit_mismatch(final_config: Dict[str, Any], record: Dict[str, Any]) -> None:
    """Reset ALL fields on audit mismatch - complete pipeline restart required"""
    timezone = final_config['timezone']
    
    # Reset SOURCE TO STAGE fields
    record['SOURCE_TO_STAGE_INGESTION_START_TIME'] = None
    record['SOURCE_TO_STAGE_INGESTION_END_TIME'] = None
    record['SOURCE_TO_STAGE_INGESTION_STATUS'] = 'PENDING'
    
    # Reset STAGE TO TARGET fields
    record['STAGE_TO_TARGET_INGESTION_START_TIME'] = None
    record['STAGE_TO_TARGET_INGESTION_END_TIME'] = None
    record['STAGE_TO_TARGET_INGESTION_STATUS'] = 'PENDING'
    
    # Reset AUDIT fields
    record['AUDIT_START_TIME'] = None
    record['AUDIT_END_TIME'] = None
    record['AUDIT_STATUS'] = 'PENDING'
    record['AUDIT_RESULT'] = None
    record['SOURCE_COUNT'] = None
    record['TARGET_COUNT'] = None
    record['COUNT_DIFFERENCE'] = None
    record['PERCENTAGE_DIFFERENCE'] = None
    
    # Reset PIPELINE fields
    record['PIPELINE_START_TIME'] = None
    record['PIPELINE_END_TIME'] = None
    record['PIPELINE_STATUS'] = 'PENDING'
    
    # Reset PHASE_COMPLETED (audit mismatch resets everything)
    record['PHASE_COMPLETED'] = None
    
    # Reset RECORD timing fields
    record['RECORD_FIRST_CREATED_TIME'] = None
    record['RECORD_LAST_UPDATED_TIME'] = None
    
    # Update retry attempt only
    record['RETRY_ATTEMPT'] = record.get('RETRY_ATTEMPT', 0) + 1
    
    log.info("ALL phases reset on audit mismatch - data corruption detected", log_key="MismatchReset", status="RESET_ALL_PHASES")

@log_execution_time
def audit_task(final_config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """Main audit orchestrator with error handling"""
    timezone = final_config['timezone']
    log.info("Starting audit task", log_key="Audit", status="STARTED")
    
    try:
        # Step 1: Check if already completed (direct check)
        if record.get('AUDIT_STATUS') == 'COMPLETED':
            log.info("Audit already completed - exiting", log_key="Audit", status="ALREADY_COMPLETE")
            return {'success': True, 'reason': 'Already completed', 'record': record}
        
        # Step 2: Set start time
        record['AUDIT_START_TIME'] = pendulum.now(timezone).to_iso8601_string()
        
        # Step 3: Get source count from Elasticsearch
        source_count = get_source_count(final_config, record)
        record['SOURCE_COUNT'] = source_count
        
        # Step 4: Get target count from Snowflake with Snowpipe monitoring
        target_count = get_target_count(source_count, final_config, record)
        record['TARGET_COUNT'] = target_count
        
        # Step 5: Calculate differences
        differences = calculate_differences(source_count, target_count)
        record['COUNT_DIFFERENCE'] = differences['count_difference']
        record['PERCENTAGE_DIFFERENCE'] = differences['percentage_difference']
        
        # Step 6: Set end time
        record['AUDIT_END_TIME'] = pendulum.now(timezone).to_iso8601_string()
        
        # Step 7: Check if counts match
        if source_count == target_count:
            # Success path
            record['AUDIT_STATUS'] = 'COMPLETED'
            record['AUDIT_RESULT'] = 'PASS'
            record['PHASE_COMPLETED'] = 'AUDIT'
            record['RECORD_LAST_UPDATED_TIME'] = pendulum.now(timezone).to_iso8601_string()
            
            upload_record_to_database(final_config, record)
            
            log.info("Audit task completed successfully - counts match", log_key="Audit", status="SUCCESS")
            return {'success': True, 'reason': 'Audit passed - counts match', 'record': record}
        else:
            # Counts don't match - data corruption detected
            log.error(f"Audit mismatch detected: source={source_count}, target={target_count}", 
                     log_key="Audit", status="DATA_CORRUPTION")
            
            # Clean corrupted data immediately
            clean_stage_location(final_config, record)
            clean_target_location(final_config, record)
            
            # Reset ALL phases due to data corruption
            reset_on_audit_mismatch(final_config, record)
            
            upload_record_to_database(final_config, record)
            
            return {'success': False, 'reason': f'Data corruption - count mismatch: source={source_count}, target={target_count}', 'record': record}
        
    except Exception as e:
        log.error(f"Audit process failed: {str(e)}", log_key="Audit", status="PROCESS_FAILED")
        
        # Reset only audit fields on process failure
        reset_on_process_failure(final_config, record)
        
        upload_record_to_database(final_config, record)
        
        return {'success': False, 'reason': f'Audit process failed: {str(e)}', 'record': record}
    
    finally:
        log.info("Audit task finished", log_key="Audit", status="FINISHED")

if __name__ == "__main__":
    # Test
    test_config = {
        'timezone': 'America/Los_Angeles',
        'host_name': 'localhost',
        'port': 9200,
        'index_id': 'test_index',
        'es_username': 'elastic',
        'es_password': 'password',
        'timestamp_field': '@timestamp',
        'default_elasticdump_retries': 3,
        'sf_creds': {'user': 'test', 'password': 'test', 'account': 'test'},
        'sf_config': {'warehouse': 'test', 'database': 'test', 'schema': 'test'},
        'raw_table': 'test_table'
    }
    test_record = {
        'WINDOW_START_TIME': pendulum.now().subtract(hours=1),
        'WINDOW_END_TIME': pendulum.now(),
        'TARGET_SUBCATEGORY': 'test_pattern',
        'STAGE_SUBCATEGORY': 's3://test-bucket/test/',
        'AUDIT_STATUS': 'PENDING'
    }
    
    result = audit_task(test_config, test_record)
    print(f"Result: {result}")


