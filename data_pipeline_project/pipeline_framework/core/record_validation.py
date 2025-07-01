# data_pipeline_project/pipeline_framework/core/record_validation.py

import pendulum
from typing import Dict, Any, Optional
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
from data_pipeline_project.pipeline_framework.utils.time_utils import convert_to_pendulum, parse_time_to_seconds

from data_pipeline_project.pipeline_framework.database.pipeline_operations import download_oldest_pending_record


def is_field_empty(value):
   """Check if field is None or empty string"""
   return value is None or value == ""

def rebuild_record_from_time_fields(record: Dict[str, Any], final_config: Dict[str, Any]) -> Dict[str, Any]:
   """Rebuild record using time fields and project functions"""

   target_day = record['TARGET_DAY']
   window_start_time = record['WINDOW_START_TIME']
   window_end_time = record['WINDOW_END_TIME']
   time_interval = record['TIME_INTERVAL']

   record = recreate_pipeline_record(record, final_config, target_day, window_start_time, window_end_time, time_interval)

   log.info("Record rebuilt successfully", log_key="RecordValidator", status="RECORD_REBUILT")
   return record


def ensure_complete_record(record: Dict[str, Any], final_config: Dict[str, Any]) -> Dict[str, Any]:
   """Check if record is complete, rebuild if needed"""
   check_fields = final_config.get('check_fields', [])
   
   if not check_fields:
       return record  # No fields to check
   
   # Check if any field is None or empty
   needs_rebuild = any(is_field_empty(record.get(field)) for field in check_fields)
   
   if needs_rebuild:
       log.info("Incomplete record detected - rebuilding", log_key="RecordValidator", status="REBUILDING_RECORD")
       return rebuild_record_from_time_fields(record, final_config)
   
   return record

# Update validate_record_timing function
@log_execution_time
def validate_record_timing(final_config: Dict[str, Any], record) -> Dict[str, Any]:
   """Get oldest pending record, ensure complete, and validate timing."""
   log.info("Starting record validation", log_key="RecordValidator", status="STARTED")
   
   try:
       required_keys = ['timezone', 'x_time_back']
       missing_keys = [key for key in required_keys if key not in final_config]
       if missing_keys:
           log.error(f"Missing required keys: {missing_keys}", log_key="RecordValidator", status="CONFIG_ERROR")
           raise ValueError(f"Missing required keys: {missing_keys}")
       

       if not record:
           log.info("No pending records found", log_key="RecordValidator", status="NO_RECORDS")
           return {'exit_dag': True, 'record': None}
       
       # Ensure record is complete
       record = ensure_complete_record(record, final_config)
       
       # Calculate latest stable time
       current_time = convert_to_pendulum('now', final_config['timezone'])
       x_time_back_seconds = parse_time_to_seconds(final_config['x_time_back'])
       latest_stable_time = current_time.subtract(seconds=x_time_back_seconds)
       x_time_back_granularity_seconds = parse_time_to_seconds(final_config['granularity'])

       if x_time_back_seconds <= x_time_back_granularity_seconds:
           latest_stable_time = latest_stable_time.subtract(seconds=x_time_back_seconds)

       # Extract window times from record
       window_start = record['WINDOW_START_TIME']
       window_end = record['WINDOW_END_TIME']
       
       # Check for future data
       if (window_start > latest_stable_time or window_end > latest_stable_time):
           log.info(
               "Record contains future data - exiting",
               log_key="RecordValidator",
               status="FUTURE_DATA",
               latest_stable_time=latest_stable_time.to_iso8601_string(),
               window_start=window_start.to_iso8601_string(),
               window_end=window_end.to_iso8601_string()
           )
           return {'exit_dag': True, 'record': record}
       
       # Valid record
       log.info(
           "Record validated successfully",
           log_key="RecordValidator",
           status="VALID_RECORD",
           window_start=window_start.to_iso8601_string(),
           window_end=window_end.to_iso8601_string()
       )
       return {'exit_dag': False, 'record': record}
       
   except Exception as e:
       log.error(f"Record validation failed: {str(e)}", log_key="RecordValidator", status="VALIDATION_ERROR")
       raise
   
   finally:
       log.info("Record validation completed", log_key="RecordValidator", status="COMPLETED")




