# data_pipeline_project/pipeline_framework/airflow_tasks/cleanup_stale_locks.py

import pendulum
from typing import Dict, Any, List
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time

from data_pipeline_project.pipeline_framework.database.pipeline_operations import get_all_in_progress_records, update_record_in_database




def reset_stale_record(record: Dict[str, Any], final_config: Dict[str, Any]) -> None:
    """Reset stale record fields"""
    timezone = final_config['timezone']
    
    # Reset pipeline status and DAG info
    record['pipeline_status'] = 'PENDING'
    record['dag_run_id'] = None
    record['pipeline_locked_at'] = None
    
    # Reset pipeline timing fields
    record['pipeline_start_time'] = None
    record['pipeline_end_time'] = None
    
    # Increment retry attempt
    record['retry_attempt'] = record.get('retry_attempt', 0) + 1
    record['record_last_updated_time'] = pendulum.now(timezone).to_iso8601_string()

@log_execution_time
def cleanup_stale_locks(final_config: Dict[str, Any]) -> Dict[str, Any]:
    """Cleanup stale locked records"""
    log.info("Starting stale locks cleanup", log_key="StaleCleanup", status="STARTED")
    
    try:
        # Get threshold from config
        stale_threshold_hours = final_config.get('stale_threshold_hours', 2)
        timezone = final_config['timezone']
        
        # Calculate cutoff time
        cutoff_time = pendulum.now(timezone).subtract(hours=stale_threshold_hours)
        
        # Get all IN_PROGRESS records
        in_progress_records = get_all_in_progress_records(final_config)
        
        cleaned_count = 0
        for record in in_progress_records:
            # Check if record is stale
            locked_at = pendulum.parse(record.get('pipeline_locked_at', ''))
            
            if locked_at < cutoff_time:
                # Reset stale record
                reset_stale_record(record, final_config)
                
                # Update in database
                if update_record_in_database(record):
                    cleaned_count += 1
                    log.info(
                        f"Cleaned stale record",
                        log_key="StaleCleanup",
                        status="CLEANED",
                        dag_run_id=record.get('dag_run_id'),
                        retry_attempt=record['retry_attempt']
                    )
        
        log.info(f"Stale cleanup completed - {cleaned_count} records cleaned", 
                log_key="StaleCleanup", status="SUCCESS", cleaned_count=cleaned_count)
        
        return {'success': True, 'cleaned_count': cleaned_count}
        
    except Exception as e:
        log.error(f"Stale cleanup failed: {str(e)}", log_key="StaleCleanup", status="FAILED")
        return {'success': False, 'error': str(e)}
    
    finally:
        log.info("Stale cleanup finished", log_key="StaleCleanup", status="FINISHED")

if __name__ == "__main__":
    test_config = {
        'timezone': 'America/Los_Angeles',
        'stale_threshold_hours': 2
    }
    
    result = cleanup_stale_locks(test_config)
    print(f"Result: {result}")



