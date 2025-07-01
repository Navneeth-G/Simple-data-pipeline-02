# data_pipeline_project/pipeline_framework/airflow_tasks/store_new_and_pass_valid_record.py

import time
from typing import Dict, Any, Optional
from data_pipeline_project.pipeline_framework.core.record_generation import build_complete_pipeline_record
from data_pipeline_project.pipeline_framework.core.record_validation import validate_record_timing
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
import pendulum
from data_pipeline_project.pipeline_framework.database.pipeline_operations import upload_record_to_database, download_oldest_pending_record



def acquire_pipeline_lock(final_config: Dict[str, Any], record: Dict[str, Any], dag_run_id: str) -> bool:
    """Acquire lock by setting dag_run_id and pipeline_status"""
    
    # Check if already locked by another DAG
    if record.get('PIPELINE_STATUS') == 'IN_PROGRESS' and record.get('DAG_RUN_ID') != dag_run_id:
        log.info(f"Record locked by another DAG: {record.get('DAG_RUN_ID')}", 
                log_key="LockAcquisition", status="ALREADY_LOCKED")
        return False
    
    # Acquire lock
    record['DAG_RUN_ID'] = dag_run_id
    record['PIPELINE_STATUS'] = 'IN_PROGRESS'
    record['PIPELINE_START_TIME'] = pendulum.now(final_config['timezone']).to_iso8601_string()
    
    log.info(f"Pipeline lock acquired", log_key="LockAcquisition", status="LOCKED", dag_run_id=dag_run_id)
    return True

@log_execution_time
def store_new_and_pass_valid_record(final_config: Dict[str, Any], dag_run_id: str) -> Dict[str, Any]:
    """Enhanced with locking mechanism"""
    log.info("Starting store new and pass valid record with locking", log_key="StoreNewPassValid", status="STARTED")
    
    try:
        # Step 1: Generate new record
        new_record = build_complete_pipeline_record(final_config)
        if new_record:
            # Step 2: Upload new record
            if not upload_record_to_database(final_config, new_record):
                raise Exception("Failed to upload new record")
        
        # Step 3: Sleep to prevent race conditions
        log.info("Sleeping 30 seconds to prevent race conditions", log_key="StoreNewPassValid", status="SLEEPING")
        time.sleep(30)
        
        # Step 4: Download oldest pending record
        oldest_record = download_oldest_pending_record(final_config)
        if not oldest_record:
            log.info("No pending records", log_key="StoreNewPassValid", status="NO_PENDING_RECORDS")
            return {'exit_dag': True, 'record': None}
        
        # Step 5: Validate timing
        validation_result = validate_record_timing(final_config, oldest_record)
        if validation_result['exit_dag']:
            log.info("Validation failed - exiting without lock", log_key="StoreNewPassValid", status="VALIDATION_EXIT")
            return validation_result
        
        # Step 6: Acquire lock
        if not acquire_pipeline_lock(final_config, oldest_record, dag_run_id):
            log.info("Failed to acquire lock - record taken by another DAG", log_key="StoreNewPassValid", status="LOCK_FAILED")
            return {'exit_dag': True, 'record': oldest_record}
        
        # Step 7: Upload locked record
        if not upload_record_to_database(final_config, oldest_record):
            raise Exception("Failed to upload locked record")
        
        # Step 8: Return locked record
        log.info("Process completed with locked record", log_key="StoreNewPassValid", status="SUCCESS", dag_run_id=dag_run_id)
        return {'exit_dag': False, 'record': oldest_record}
        
    except Exception as e:
        log.error(f"Process failed: {str(e)}", log_key="StoreNewPassValid", status="FAILED")
        raise
    
    finally:
        log.info("Store new and pass valid record finished", log_key="StoreNewPassValid", status="FINISHED")

if __name__ == "__main__":
    test_config = {
        'timezone': 'America/Los_Angeles',
        'x_time_back': '1d',
        'granularity': '2h',
        'default_record': {'source_name': 'elasticsearch', 'target_name': 'snowflake'}
    }
    
    result = store_new_and_pass_valid_record(test_config, "test_dag_run_123")
    print(f"Result: {result}")


