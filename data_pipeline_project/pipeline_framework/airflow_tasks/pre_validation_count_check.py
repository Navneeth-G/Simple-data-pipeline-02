# data_pipeline_project/pipeline_framework/airflow_tasks/pre_validation_count_check.py

from typing import Dict, Any
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time, retry

from data_pipeline_project.pipeline_framework.database.pipeline_operations import get_target_count

@retry(max_attempts=3, delay_seconds=5)
def get_target_count_with_retry(final_config: Dict[str, Any], record: Dict[str, Any]) -> int:
    """Get target count with retry logic"""
    return get_target_count(final_config, record)

@log_execution_time
def pre_validate_counts(final_config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare source vs target counts to determine if pipeline should run.
    
    Returns:
        Dict with exit_dag, reason, source_count, target_count
    """
    log.info("Starting pre-validation count check", log_key="PreValidation", status="STARTED")
    
    try:
        # Get counts with retry logic
        source_count = get_source_count(final_config, record)
        target_count = get_target_count(final_config, record)
        
        log.info(
            f"Counts retrieved: source={source_count}, target={target_count}",
            log_key="PreValidation",
            status="COUNTS_RETRIEVED",
            source_count=source_count,
            target_count=target_count
        )
        
        # Decision logic
        if source_count == target_count and both_greater_than_zero(source_count, target_count):
            return {
                'exit_dag': True,
                'reason': 'Already processed - counts match',
                'source_count': source_count,
                'target_count': target_count
            }
        
        return {
            'exit_dag': False, 
            'reason': 'Proceed with processing - counts differ or zero',
            'source_count': source_count,
            'target_count': target_count
        }
        
    except Exception as e:
        log.error(f"Pre-validation failed: {str(e)}", log_key="PreValidation", status="FAILED")
        raise
    
    finally:
        log.info("Pre-validation completed", log_key="PreValidation", status="COMPLETED")

def both_greater_than_zero(source_count: int, target_count: int) -> bool:
    """Check if both counts > 0"""
    return source_count > 0 and target_count > 0

if __name__ == "__main__":
    # Test
    test_config = {'test': 'config'}
    test_record = {'TARGET_DAY': 'test_day'}
    
    result = pre_validate_counts(test_config, test_record)
    print(f"Result: {result}")



