# pipeline_framework/task_handlers/decide_pipeline_record_task.py
import pendulum
from typing import Dict, Any
from data_pipeline_project.pipeline_framework.utils.time_utils import (     convert_to_pendulum,    parse_time_to_seconds,)
from data_pipeline_project.pipeline_framework.drive_storage.drive_func import (
    get_the_oldest_record_from_drive,
    upload_record_to_drive_table,
)
from data_pipeline_project.pipeline_framework.core.record_generation import build_complete_pipeline_record
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time

@log_execution_time
def decide_pipeline_record(final_config: Dict[str, Any], dag_run_id: str) -> Dict[str, Any]:
    """
    Decides whether to process an existing pending record or create a new one.

    Returns:
        {
            'exit_dag': True/False,
            'record': dict or None
        }
    """
    timezone = final_config.get("timezone", "UTC")
    sf_drive_config = final_config.get("sf_drive_config", {})

    try:
        # Step 1: Try to fetch oldest pending record
        record = get_the_oldest_record_from_drive(sf_drive_config, final_config)

        if record:
            log.info("Pending record found", timezone=timezone, log_key="Decision", status="RECORD_FOUND")

            validation_result = validate_record_timing(final_config, record)

            if validation_result["exit_dag"]:
                log.info("Pending record too recent — exiting DAG", timezone=timezone, log_key="Decision", status="TOO_RECENT")
                return validation_result
            
            record = validation_result["record"]

            # Set status and start time
            record["PIPELINE_STATUS"] = "IN_PROGRESS"
            record["PIPELINE_START_TIME"] = pendulum.now(timezone)
            record["DAG_RUN_ID"] = dag_run_id

            # Write updated record back
            if not upload_record_to_drive_table(sf_drive_config, final_config, record):
                raise Exception("Failed to update record to IN_PROGRESS")

            return {"exit_dag": False, "record": record}

        # Step 2: No pending record → try to create one
        log.info("No pending record found — creating new one", timezone=timezone, log_key="Decision", status="BUILDING_NEW")

        new_record = build_complete_pipeline_record(final_config)

        if new_record is None:
            log.info("No new record generated — processing complete", timezone=timezone, log_key="Decision", status="NO_NEW_RECORD")
            return {"exit_dag": True, "record": None}

        # Step 3: Upload new record
        if not upload_record_to_drive_table(sf_drive_config, final_config, new_record):
            raise Exception("Failed to upload new record to drive table")

        # Step 4: Re-fetch and validate again
        record = get_the_oldest_record_from_drive(sf_drive_config, final_config)

        if not record:
            log.info("Newly inserted record not visible yet", timezone=timezone, log_key="Decision", status="RETRY_FAILED")
            return {"exit_dag": True, "record": None}

        validation_result = validate_record_timing(final_config, record)

        if validation_result["exit_dag"]:
            log.info("New record is too recent — exiting DAG", timezone=timezone, log_key="Decision", status="TOO_RECENT_NEW")
            return validation_result

        return {"exit_dag": False, "record": validation_result["record"]}

    except Exception as e:
        log.error("Fatal error in record decision process", timezone=timezone, log_key="Decision", status="ERROR", error=str(e))
        raise


@log_execution_time
def validate_record_timing(final_config: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate whether the record is eligible for processing based only on its WINDOW_END_TIME.

    Rule:
    - WINDOW_END_TIME must be less than or equal to (now - x_time_back)

    Returns:
        Dict with keys:
            - exit_dag: True if the DAG should stop
            - record: the input record
    """
    timezone = final_config.get("timezone", "UTC")
    x_time_back = final_config["x_time_back"]

    try:
        # Step 1: Get current time and calculate cutoff
        now = pendulum.now(timezone)
        x_back_sec = parse_time_to_seconds(x_time_back)
        max_allowed_end_time = now.subtract(seconds=x_back_sec)

        # Step 2: Convert and compare WINDOW_END_TIME
        window_end = convert_to_pendulum(record["WINDOW_END_TIME"])

        if window_end > max_allowed_end_time:
            log.info(
                "Record rejected: WINDOW_END_TIME is too recent.",
                timezone=timezone,
                log_key="RecordValidator",
                status="TOO_RECENT",
                window_end=window_end.to_iso8601_string(),
                max_allowed_end_time=max_allowed_end_time.to_iso8601_string()
            )
            return {"exit_dag": True, "record": record}

        # Step 3: Passed
        log.info(
            "Record validated successfully — within allowed time window.",
            timezone=timezone,
            log_key="RecordValidator",
            status="VALID"
        )
        return {"exit_dag": False, "record": record}

    except Exception as e:
        log.error(
            "Error during record validation.",
            timezone=timezone,
            log_key="RecordValidator",
            status="ERROR",
            error=str(e)
        )
        raise




