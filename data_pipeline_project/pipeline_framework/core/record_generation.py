# data_pipeline_project/pipeline_framework/core/record_generation.py

import pendulum
from typing import Dict, Any, Optional, Tuple
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
from data_pipeline_project.pipeline_framework.utils.time_utils import calculate_day_from_timestamp, parse_time_to_seconds, seconds_to_time_string


from data_pipeline_project.pipeline_framework.database.pipeline_operations import latest_timestamp_available as db_latest_timestamp_available
import hashlib

def create_hash(*values) -> str:
    """Create SHA256 hash from multiple values"""
    combined = "|".join(str(v) for v in values)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]  # First 16 chars

def latest_timestamp_available(final_config: Dict[str, Any]) -> Optional[pendulum.DateTime]:
    """Get latest processed timestamp from database"""
    return db_latest_timestamp_available(final_config)

def get_continuation_timestamp(final_config: Dict[str, Any]) -> Optional[pendulum.DateTime]:
    """
    Get continuation timestamp from database.
    Returns None for new processing, or timestamp to continue from.
    """
    return latest_timestamp_available(final_config)


def s3_uri_creation(final_config: Dict[str, Any], target_day: pendulum.DateTime, 
                   window_start_time: pendulum.DateTime, window_end_time: pendulum.DateTime, 
                   time_interval: str) -> Dict[str, str]:
    """
    Generate S3 URI structure for staging
    Returns STAGE_CATEGORY and STAGE_SUBCATEGORY
    """
    
    # Get S3 prefix list and bucket from config
    s3_prefix_list = final_config['s3_prefix_list']
    bucket_name = final_config['bucket_name']
    
    # Join prefix list and do replacements
    joined_prefix = "/".join(s3_prefix_list)
    
    # Replace placeholders with actual values from final_config
    replaced_prefix = joined_prefix.format(
        env=final_config['env'],
        index_group=final_config['index_group'], 
        index_name=final_config['index_name'],
        index_id=final_config['index_id'],
        **final_config  # Any other placeholders in the prefix
    )
    
    # Generate date/time path from window_start_time
    date_str = window_start_time.format('YYYY-MM-DD')  # 2025-07-01
    time_str = window_start_time.format('HH-mm')       # 14-30
    
    # Build final S3 URI
    stage_category = bucket_name
    stage_subcategory = f"s3://{bucket_name}/{replaced_prefix}/{date_str}/{time_str}/"
    
    return {
        'STAGE_CATEGORY': stage_category,
        'STAGE_SUBCATEGORY': stage_subcategory
    }


def target_uri_creation(final_config: Dict[str, Any], target_day: pendulum.DateTime, 
                       window_start_time: pendulum.DateTime, window_end_time: pendulum.DateTime, 
                       time_interval: str, stage_subcategory: str) -> Dict[str, str]:
    """
    Generate Target URI structure for Snowflake
    Returns TARGET_CATEGORY and TARGET_SUBCATEGORY
    """
    
    # Get Snowflake details from config
    raw_database = final_config['raw_database']
    raw_schema = final_config['raw_schema'] 
    raw_table = final_config['raw_table']
    
    # Build target category as database.schema.table
    target_category = f"{raw_database}.{raw_schema}.{raw_table}"
    



    # Get S3 prefix list and bucket from config
    s3_prefix_list = final_config['s3_prefix_list']
    bucket_name = final_config['bucket_name']
    
    # Join prefix list and do replacements
    joined_prefix = "/".join(s3_prefix_list)
    
    # Replace placeholders with actual values from final_config
    replaced_prefix = joined_prefix.format(
        env=final_config['env'],
        index_group=final_config['index_group'], 
        index_name=final_config['index_name'],
        index_id=final_config['index_id']
        # , **final_config  # Any other placeholders in the prefix
    )
    
    # Generate date/time path from window_start_time
    date_str = window_start_time.format('YYYY-MM-DD')  # 2025-07-01
    time_str = window_start_time.format('HH-mm')       # 14-30
    
    # Build final S3 URI
    stage_category = bucket_name

    # Target subcategory exactly equals stage subcategory (for COPY INTO command)
    target_subcategory = f"{replaced_prefix}/{date_str}/{time_str}/"

    return {
        'TARGET_CATEGORY': target_category,
        'TARGET_SUBCATEGORY': target_subcategory
    }


def source_uri_creation(final_config: Dict[str, Any], target_day: pendulum.DateTime, 
                       window_start_time: pendulum.DateTime, window_end_time: pendulum.DateTime, 
                       time_interval: str) -> Dict[str, str]:
    """
    Generate Source URI structure for Elasticsearch
    Returns SOURCE_CATEGORY and SOURCE_SUBCATEGORY
    """
    
    # Get source details from config
    source_category = final_config['index_group']
    source_subcategory = final_config['index_name']
    
    return {
        'SOURCE_CATEGORY': source_category,
        'SOURCE_SUBCATEGORY': source_subcategory
    }

def generate_pipeline_ids(final_config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, str]:
    """
    Generate hash-based IDs for source, stage, target, and pipeline
    Returns dictionary with all ID fields
    """

    
    # Get required values from record and config
    source_name = record['SOURCE_NAME']
    source_category = record['SOURCE_CATEGORY'] 
    source_subcategory = record['SOURCE_SUBCATEGORY']
    window_start = record['WINDOW_START_TIME'].to_iso8601_string()
    window_end = record['WINDOW_END_TIME'].to_iso8601_string()
    
    stage_name = record['STAGE_NAME']
    stage_category = record['STAGE_CATEGORY']
    stage_subcategory = record['STAGE_SUBCATEGORY']
    
    target_name = record['TARGET_NAME']
    target_category = record['TARGET_CATEGORY']
    target_subcategory = record['TARGET_SUBCATEGORY']
    
    pipeline_name = record['PIPELINE_NAME']
    first_record_creation = record['RECORD_FIRST_CREATED_TIME']
    
    # Generate IDs
    source_id = create_hash(source_name, source_category, source_subcategory, window_start, window_end)
    stage_id = create_hash(stage_name, stage_category, stage_subcategory)
    target_id = create_hash(target_name, target_category, target_subcategory)
    pipeline_id = create_hash(source_id, stage_id, target_id, pipeline_name, first_record_creation)
    
    return {
        'SOURCE_ID': source_id,
        'STAGE_ID': stage_id,
        'TARGET_ID': target_id,
        'PIPELINE_ID': pipeline_id
    }

def calculate_time_window(target_day: pendulum.DateTime, 
                         granularity: str, 
                         continuation_timestamp: Optional[pendulum.DateTime] = None) -> Optional[Tuple[pendulum.DateTime, pendulum.DateTime, str]]:
    """
    Calculate WINDOW_START_TIME, WINDOW_END_TIME, and actual time_interval.
    
    Args:
        target_day: Target day (start of day)
        granularity: Time string like '1h', '30m'
        continuation_timestamp: Optional timestamp to continue from
        
    Returns:
        Tuple of (WINDOW_START_TIME, WINDOW_END_TIME, time_interval) or None if processing complete
    """
    
    granularity_seconds = parse_time_to_seconds(granularity)
    next_day_start = target_day.add(days=1)
    
    # Check if continuation timestamp indicates processing is complete
    if continuation_timestamp is not None:
        if continuation_timestamp < target_day:
            log.info(
                f"Continuation timestamp {continuation_timestamp.to_iso8601_string()} is before target day {target_day.to_date_string()}",
                log_key="TimeWindow",
                status="PROCESSING_COMPLETE_BEFORE"
            )
            return None
        
        if continuation_timestamp >= next_day_start:
            log.info(
                f"Continuation timestamp {continuation_timestamp.to_iso8601_string()} indicates target day {target_day.to_date_string()} processing is complete",
                log_key="TimeWindow", 
                status="PROCESSING_COMPLETE_AFTER"
            )
            return None
        
        log.info(
            f"Continuation timestamp validated: {continuation_timestamp.to_iso8601_string()}",
            log_key="TimeWindow",
            status="VALIDATION_SUCCESS"
        )
    
    # Determine WINDOW_START_TIME
    if continuation_timestamp is None:
        window_start_time = target_day
        log.info("Starting from beginning of target day", log_key="TimeWindow", status="NEW_PROCESSING")
    else:
        window_start_time = continuation_timestamp
        log.info(f"Continuing from timestamp: {window_start_time.to_iso8601_string()}", log_key="TimeWindow", status="CONTINUATION")
    
    # Calculate desired WINDOW_END_TIME
    desired_window_end_time = window_start_time.add(seconds=granularity_seconds)
    
    # Ensure WINDOW_END_TIME doesn't cross target day boundary
    if desired_window_end_time > next_day_start:
        window_end_time = next_day_start
        log.info("Window end time capped at next day boundary", log_key="TimeWindow", status="BOUNDARY_CAPPED")
    else:
        window_end_time = desired_window_end_time
    
    # Calculate actual time interval
    actual_interval_seconds = int((window_end_time - window_start_time).total_seconds())
    time_interval = seconds_to_time_string(actual_interval_seconds)
    
    log.info(
        f"Time window calculated", 
        log_key="TimeWindow", 
        status="SUCCESS",
        window_start=window_start_time.to_iso8601_string(),
        window_end=window_end_time.to_iso8601_string(),
        requested_granularity=granularity,
        actual_interval=time_interval
    )
    
    return window_start_time, window_end_time, time_interval


@log_execution_time  
def generate_pipeline_time_window_record(final_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Generate pipeline record with target day and processing time window. Returns None if processing complete."""
    
    log.info("Starting pipeline time window record generation", log_key="RecordGenerator", status="STARTED")
    
    time_window_details = {}
    target_day = None
    
    try:
        required_keys = ['timezone', 'x_time_back', 'granularity']
        missing_keys = [key for key in required_keys if key not in final_config]
        
        if missing_keys:
            log.error(f"Missing required keys: {missing_keys}", log_key="RecordGenerator", status="CONFIG_ERROR")
            raise ValueError(f"Missing required keys: {missing_keys}")
        
        # Calculate target day
        target_day = calculate_day_from_timestamp('now', final_config['x_time_back'], final_config['timezone'])
        log.info(f"Target day calculated: {target_day.to_date_string()}", log_key="RecordGenerator", status="TARGET_DAY_SUCCESS")
        
        # Get continuation timestamp
        continuation_timestamp = get_continuation_timestamp(final_config)
        
        # Calculate time window (returns None if processing complete)
        time_window_result = calculate_time_window(
            target_day, 
            final_config['granularity'], 
            continuation_timestamp
        )
        
        # Check if processing is complete
        if time_window_result is None:
            log.info(
                f"Target day {target_day.to_date_string()} processing is already complete", 
                log_key="RecordGenerator", 
                status="PROCESSING_COMPLETE"
            )
            return None
        
        window_start_time, window_end_time, time_interval = time_window_result
        
        time_window_details = {
            'target_day': target_day,
            'target_day_str': target_day.to_date_string(),
            'window_start_time': window_start_time,
            'window_end_time': window_end_time,
            'time_interval': time_interval
        }
        
        log.info(
            f"Time window details generated successfully", 
            log_key="RecordGenerator", 
            status="SUCCESS",
            target_day=time_window_details['target_day_str'],
            time_window=f"{window_start_time.to_time_string()} - {window_end_time.to_time_string()}",
            actual_interval=time_interval
        )
        
        return time_window_details
        
    except Exception as e:
        log.error(f"Failed to generate time window details: {str(e)}", log_key="RecordGenerator", status="CALCULATION_ERROR")
        raise
        
    finally:
        log.info("Time window details generation process completed", log_key="RecordGenerator", status="PROCESS_COMPLETED")



def recreate_pipeline_record(record: Dict[str, Any], final_config: Dict[str, Any], target_day: pendulum.DateTime, window_start_time: pendulum.DateTime, window_end_time: pendulum.DateTime, time_interval) -> Optional[Dict[str, Any]]:
    """Update the pipeline record with new time window details."""
    # Update time-related fields
    record['TARGET_DAY'] = target_day
    record['WINDOW_START_TIME'] = window_start_time
    record['WINDOW_END_TIME'] = window_end_time
    record['TIME_INTERVAL'] = time_interval

    # Create source info
    source_info = source_uri_creation(final_config, target_day, window_start_time, window_end_time, time_interval)
    record['SOURCE_CATEGORY'] = source_info['SOURCE_CATEGORY']
    record['SOURCE_SUBCATEGORY'] = source_info['SOURCE_SUBCATEGORY']

    # Create stage info
    stage_info = s3_uri_creation(final_config, target_day, window_start_time, window_end_time, time_interval)
    record['STAGE_CATEGORY'] = stage_info['STAGE_CATEGORY']
    record['STAGE_SUBCATEGORY'] = stage_info['STAGE_SUBCATEGORY']

    # Create target info
    target_info = target_uri_creation(final_config, target_day, window_start_time, window_end_time, time_interval, stage_info['STAGE_SUBCATEGORY'])
    record['TARGET_CATEGORY'] = target_info['TARGET_CATEGORY']
    record['TARGET_SUBCATEGORY'] = target_info['TARGET_SUBCATEGORY']

    record['RECORD_FIRST_CREATED_TIME'] = pendulum.now(timezone).to_iso8601_string()

    # Create source, stage, target info 
    source_info = source_uri_creation(final_config, target_day, window_start_time, window_end_time, time_interval)
    stage_info = s3_uri_creation(final_config, target_day, window_start_time, window_end_time, time_interval)
    target_info = target_uri_creation(final_config, target_day, window_start_time, window_end_time, time_interval, stage_info['STAGE_SUBCATEGORY'])

    # Update record with all URI info
    record.update(source_info)
    record.update(stage_info)
    record.update(target_info)

    # Generate all IDs
    id_info = generate_pipeline_ids(final_config, record)
    record.update(id_info)

    log.info(
        "Complete pipeline record built successfully",
        log_key="RecordBuilder",
        status="SUCCESS",
        target_day=target_day.to_date_string(),
        record_keys=list(record.keys())
    )
    return record


@log_execution_time
def build_complete_pipeline_record(final_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build complete pipeline record. Returns None if processing complete."""
    
    log.info("Starting complete pipeline record building", log_key="RecordBuilder", status="STARTED")
    
    try:
        # Validate required keys
        required_keys = ['timezone', 'x_time_back', 'granularity', 'default_record']
        missing_keys = [key for key in required_keys if key not in final_config]
        
        if missing_keys:
            log.error(f"Missing required keys: {missing_keys}", log_key="RecordBuilder", status="CONFIG_ERROR")
            raise ValueError(f"Missing required keys: {missing_keys}")
        
        # Get time window details
        time_window_details = generate_pipeline_time_window_record(final_config)
        
        # Check if processing is complete
        if time_window_details is None:
            log.info("Pipeline record building skipped - processing complete", log_key="RecordBuilder", status="PROCESSING_COMPLETE")
            return None
        
        target_day = time_window_details['target_day']
        window_start_time = time_window_details['window_start_time']
        window_end_time = time_window_details['window_end_time']
        time_interval = time_window_details['time_interval']
        

        updated_pipeline_record = final_config['default_record'].copy()

        updated_pipeline_record = recreate_pipeline_record(updated_pipeline_record, final_config, target_day, window_start_time, window_end_time, time_interval)
        
        return updated_pipeline_record
        
    except Exception as e:
        log.error(f"Failed to build complete pipeline record: {str(e)}", log_key="RecordBuilder", status="BUILD_ERROR")
        raise
        
    finally:
        log.info("Complete pipeline record building process completed", log_key="RecordBuilder", status="PROCESS_COMPLETED")



