# data_pipeline_project/pipeline_framework/airflow_tasks/stage_to_target_transfer.py

from typing import Dict, Any
import pendulum
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
from data_pipeline_project.pipeline_framework.database.pipeline_operations import target_deletion, upload_record_to_database

def clean_target_location(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Clean target location using target_deletion"""
    return target_deletion(final_config, record)

def transfer_stage_to_target(final_config: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Execute Snowflake task to trigger Snowpipe"""
    from data_pipeline_project.pipeline_framework.database.query_executor import get_query_executor
    
    try:
        executor = get_query_executor(final_config['sf_creds'], final_config['sf_config'], final_config)
        task_name = final_config['snowflake_task_name']
        
        query = f"EXECUTE TASK {task_name}"
        result = executor.execute_query(query)
        
        if result['success']:
            log.info(f"Snowflake task executed successfully", log_key="SnowflakeTask", 
                    status="SUCCESS", task_name=task_name, query_id=result['query_id'])
            return True
        else:
            log.error(f"Snowflake task execution failed", log_key="SnowflakeTask", 
                     status="FAILED", task_name=task_name)
            return False
        
    except Exception as e:
        log.error(f"Failed to execute Snowflake task: {str(e)}", log_key="SnowflakeTask", status="FAILED")
        return False

def reset_on_failure(final_config: Dict[str, Any], record: Dict[str, Any]) -> None:
    """Reset fields on failure - stage_to_target specific and pipeline fields"""
    timezone = final_config['timezone']
    
    # Reset stage-to-target specific fields
    record['STAGE_TO_TARGET_INGESTION_START_TIME'] = None
    record['STAGE_TO_TARGET_INGESTION_END_TIME'] = None
    record['STAGE_TO_TARGET_INGESTION_STATUS'] = 'PENDING'
    
    # Reset pipeline fields
    record['PIPELINE_START_TIME'] = None
    record['PIPELINE_END_TIME'] = None
    record['PIPELINE_STATUS'] = 'PENDING'
    
    # Update retry and last updated time
    record['RETRY_ATTEMPT'] = record.get('RETRY_ATTEMPT', 0) + 1
    record['RECORD_LAST_UPDATED_TIME'] = pendulum.now(timezone).to_iso8601_string()
    
    log.info("Fields reset on failure", log_key="FailureReset", status="RESET_COMPLETE")

@log_execution_time
def stage_to_target_task(final_config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """Main stage-to-target orchestrator with error handling"""
    timezone = final_config['timezone']
    log.info("Starting stage-to-target task", log_key="StageToTarget", status="STARTED")
    
    try:
        # Step 1: Check if already completed (direct check)
        if record.get('STAGE_TO_TARGET_INGESTION_STATUS') == 'COMPLETED':
            log.info("Transfer already completed - exiting", log_key="StageToTarget", status="ALREADY_COMPLETE")
            return {'success': True, 'reason': 'Already completed', 'record': record}
        
        # Step 2: Set start time
        record['STAGE_TO_TARGET_INGESTION_START_TIME'] = pendulum.now(timezone).to_iso8601_string()
        
        # Step 3: Clean target location
        if not clean_target_location(final_config, record):
            raise Exception("Target cleanup failed")
        
        # Step 4: Execute Snowflake task to trigger Snowpipe
        if not transfer_stage_to_target(final_config, record):
            raise Exception("Snowflake task execution failed")
        
        # Step 5: Set end time and update status/phase
        record['STAGE_TO_TARGET_INGESTION_END_TIME'] = pendulum.now(timezone).to_iso8601_string()
        record['STAGE_TO_TARGET_INGESTION_STATUS'] = 'COMPLETED'
        record['PHASE_COMPLETED'] = 'STAGE_TO_TARGET'
        record['RECORD_LAST_UPDATED_TIME'] = pendulum.now(timezone).to_iso8601_string()
        
        # Step 6: Upload updated record
        upload_record_to_database(final_config, record)
        
        log.info("Stage-to-target task completed successfully", log_key="StageToTarget", status="SUCCESS")
        return {'success': True, 'reason': 'Transfer completed', 'record': record}
        
    except Exception as e:
        log.error(f"Stage-to-target task failed: {str(e)}", log_key="StageToTarget", status="FAILED")
        
        # Cleanup partial data
        clean_target_location(final_config, record)
        
        # Reset all relevant fields on failure
        reset_on_failure(final_config, record)
        
        # Upload reset record
        upload_record_to_database(final_config, record)
        
        return {'success': False, 'reason': f'Transfer failed: {str(e)}', 'record': record}
    
    finally:
        log.info("Stage-to-target task finished", log_key="StageToTarget", status="FINISHED")

if __name__ == "__main__":
    # Test
    test_config = {
        'timezone': 'America/Los_Angeles',
        'snowflake_task_name': 'MY_SNOWPIPE_TASK',
        'sf_creds': {'user': 'test', 'password': 'test', 'account': 'test'},
        'sf_config': {'warehouse': 'test', 'database': 'test', 'schema': 'test'},
        'raw_table': 'test_table'
    }
    test_record = {
        'TARGET_SUBCATEGORY': 'test_pattern',
        'STAGE_TO_TARGET_INGESTION_STATUS': 'PENDING'
    }
    
    result = stage_to_target_task(test_config, test_record)
    print(f"Result: {result}")