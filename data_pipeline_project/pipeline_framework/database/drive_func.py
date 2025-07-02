import snowflake.connector
import pendulum
from typing import Dict, Optional
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import log_execution_time
from data_pipeline_project.pipeline_framework.utils.time_utils import convert_to_pendulum



def create_drive_connection(sf_drive_config: Dict[str, str]):
    return snowflake.connector.connect(
        user=sf_drive_config['username'],
        password=sf_drive_config['password'],
        account=sf_drive_config['account'],
        warehouse=sf_drive_config['warehouse'],
        database=sf_drive_config['database'],
        schema=sf_drive_config['schema']
    )



def convert_drive_record_timestamps(record: Dict[str, any], final_config: Dict[str, str]) -> Dict[str, any]:
    """
    Convert timestamp columns in the drive record to Pendulum objects.
    
    Args:
        record: A single pipeline record (dict)
        final_config: Should include 'timestamp_columns' and 'timezone'

    Returns:
        Updated record with Pendulum datetime objects
    """
    timezone = final_config.get("timezone", "UTC")
    timestamp_columns = final_config.get("timestamp_columns", [])

    for column in timestamp_columns:
        if column in record and record[column] is not None:
            record[column] = convert_to_pendulum(record[column])

    return record


@log_execution_time
def get_the_oldest_record_from_drive(sf_drive_config: Dict[str, str], final_config: Dict[str, str]) -> Optional[Dict[str, any]]:
    timezone = final_config.get("timezone", "UTC")

    try:
        conn = create_drive_connection(sf_drive_config)
        cursor = conn.cursor()

        query = f"""
            SELECT * FROM {sf_drive_config['drive_table']}
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
            'SOURCE_NAME': final_config['SOURCE_NAME'],
            'SOURCE_CATEGORY': final_config['SOURCE_CATEGORY'],
            'SOURCE_SUBCATEGORY': final_config['SOURCE_SUBCATEGORY'],
            'STAGE_NAME': final_config['STAGE_NAME'],
            'TARGET_NAME': final_config['TARGET_NAME'],
            'PIPELINE_STATUS': final_config['PIPELINE_STATUS'],
            'PIPELINE_PRIORITY': final_config['PIPELINE_PRIORITY']
        }

        log.info("Running drive fetch query", timezone=timezone, log_key="DriveFetch", status="QUERYING", query=query, parameters=params)

        cursor.execute(query, params)
        row = cursor.fetchone()

        if not row:
            log.info("No matching record found in drive", timezone=timezone, log_key="DriveFetch", status="EMPTY_RESULT")
            return None

        columns = [col[0] for col in cursor.description]
        record = dict(zip(columns, row))

        #  Convert all timestamps to Pendulum
        record = convert_drive_record_timestamps(record, final_config)

        log.info("Fetched oldest record successfully", timezone=timezone, log_key="DriveFetch", status="SUCCESS", pipeline_id=record.get("PIPELINE_ID"))
        return record

    except Exception as e:
        log.error("Error fetching record from drive", timezone=timezone, log_key="DriveFetch", status="FAILURE", error=str(e))
        raise

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

@log_execution_time
def upload_record_to_drive_table(sf_drive_config: Dict[str, str], final_config: Dict[str, str], record: Dict[str, Any]) -> bool:
    """
    Insert a record into the Snowflake drive table.
    
    Args:
        sf_drive_config: Snowflake connection + table info
        final_config: Includes runtime values like timezone
        record: A dictionary representing the record to insert
        
    Returns:
        True if insert successful, False otherwise
    """
    timezone = final_config.get("timezone", "UTC")

    try:
        conn = create_drive_connection(sf_drive_config)
        cursor = conn.cursor()

        columns = ', '.join(record.keys())
        placeholders = ', '.join([f'%({key})s' for key in record.keys()])
        query = f"INSERT INTO {sf_drive_config['drive_table']} ({columns}) VALUES ({placeholders})"

        log.info("Uploading record to drive table", timezone=timezone, log_key="DriveUpload", status="UPLOADING", record_keys=list(record.keys()))

        cursor.execute(query, record)
        conn.commit()

        log.info("Record uploaded successfully", timezone=timezone, log_key="DriveUpload", status="SUCCESS", pipeline_id=record.get("PIPELINE_ID"))
        return True

    except Exception as e:
        log.error("Error uploading record to drive table", timezone=timezone, log_key="DriveUpload", status="FAILURE", error=str(e))
        return False

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


def latest_timestamp_available(sf_drive_config: Dict[str, str], final_config: Dict[str, str]) -> Optional[pendulum.DateTime]:
    """
    Get the latest processed WINDOW_END_TIME from the drive table for a given source-to-target route.

    Args:
        sf_drive_config: Contains Snowflake credentials and table info
        final_config: Contains filtering fields and timezone

    Returns:
        A pendulum datetime of the latest window_end_time, or None
    """
    timezone = final_config.get("timezone", "UTC")

    try:
        conn = create_drive_connection(sf_drive_config)
        cursor = conn.cursor()

        query = f"""
            SELECT MAX(WINDOW_END_TIME) FROM {sf_drive_config['drive_table']}
            WHERE SOURCE_NAME = %(SOURCE_NAME)s
            AND SOURCE_CATEGORY = %(SOURCE_CATEGORY)s  
            AND SOURCE_SUBCATEGORY = %(SOURCE_SUBCATEGORY)s
            AND STAGE_NAME = %(STAGE_NAME)s
            AND TARGET_NAME = %(TARGET_NAME)s
        """

        params = {
            'SOURCE_NAME': final_config['SOURCE_NAME'],
            'SOURCE_CATEGORY': final_config['SOURCE_CATEGORY'],
            'SOURCE_SUBCATEGORY': final_config['SOURCE_SUBCATEGORY'],
            'STAGE_NAME': final_config['STAGE_NAME'],
            'TARGET_NAME': final_config['TARGET_NAME']
        }

        cursor.execute(query, params)
        row = cursor.fetchone()

        if row and row[0] is not None:
            timestamp = convert_to_pendulum(row[0])
            log.info("Latest WINDOW_END_TIME retrieved successfully", timezone=timezone, log_key="LatestTimestamp", status="SUCCESS", timestamp=timestamp.to_iso8601_string())
            return timestamp

        log.info("No completed records found in drive table", timezone=timezone, log_key="LatestTimestamp", status="NO_RECORDS")
        return None

    except Exception as e:
        log.error("Error retrieving latest WINDOW_END_TIME", timezone=timezone, log_key="LatestTimestamp", status="ERROR", error=str(e))
        raise

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def update_record_in_drive_table(sf_drive_config: Dict[str, str], final_config: Dict[str, str], record: Dict[str, any]) -> bool:
    """
    Update an existing pipeline record in the drive table by PIPELINE_ID.

    Args:
        sf_drive_config: Snowflake connection + table info
        final_config: Context info including timezone
        record: A record dictionary to update (must include PIPELINE_ID)

    Returns:
        True if update was successful, False otherwise
    """
    timezone = final_config.get("timezone", "UTC")

    try:
        conn = create_drive_connection(sf_drive_config)
        cursor = conn.cursor()

        table = sf_drive_config["drive_table"]
        pipeline_id = record["PIPELINE_ID"]

        # Exclude PIPELINE_ID from update columns
        columns_to_update = [col for col in record.keys() if col != "PIPELINE_ID"]
        set_clause = ", ".join([f"{col} = %({col})s" for col in columns_to_update])

        query = f"""
            UPDATE {table}
            SET {set_clause}
            WHERE PIPELINE_ID = %(PIPELINE_ID)s
        """

        log.info("Running update for pipeline record", timezone=timezone, log_key="RecordUpdate", status="UPDATING", pipeline_id=pipeline_id)

        cursor.execute(query, record)
        conn.commit()

        log.info("Record updated successfully", timezone=timezone, log_key="RecordUpdate", status="SUCCESS", pipeline_id=pipeline_id)
        return True

    except Exception as e:
        log.error("Failed to update pipeline record", timezone=timezone, log_key="RecordUpdate", status="FAILED", error=str(e))
        return False

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


def get_all_in_progress_records(sf_drive_config: Dict[str, str], final_config: Dict[str, str]) -> List[Dict[str, any]]:
    """
    Fetch all drive records with status IN_PROGRESS based on source/stage/target filters.

    Args:
        sf_drive_config: Contains Snowflake credentials and table info
        final_config: Contains filter values and timezone

    Returns:
        A list of matching record dictionaries
    """
    timezone = final_config.get("timezone", "UTC")

    try:
        conn = create_drive_connection(sf_drive_config)
        cursor = conn.cursor()

        query = f"""
            SELECT * FROM {sf_drive_config['drive_table']}
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
            'SOURCE_NAME': final_config['SOURCE_NAME'],
            'SOURCE_CATEGORY': final_config['SOURCE_CATEGORY'],
            'SOURCE_SUBCATEGORY': final_config['SOURCE_SUBCATEGORY'],
            'STAGE_NAME': final_config['STAGE_NAME'],
            'TARGET_NAME': final_config['TARGET_NAME'],
            'PIPELINE_STATUS': 'IN_PROGRESS',
            'PIPELINE_PRIORITY': final_config['PIPELINE_PRIORITY']
        }

        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            log.info("No in-progress records found", timezone=timezone, log_key="InProgressRecords", status="NO_RECORDS")
            return []

        columns = [col[0] for col in cursor.description]
        records = [dict(zip(columns, row)) for row in rows]

        # Optional: Convert timestamps to Pendulum
        timestamp_columns = final_config.get("timestamp_columns", [])
        for record in records:
            for col in timestamp_columns:
                if col in record and record[col] is not None:
                    record[col] = convert_to_pendulum(record[col])

        log.info(
            "In-progress records retrieved",
            timezone=timezone,
            log_key="InProgressRecords",
            status="SUCCESS",
            record_count=len(records)
        )
        return records

    except Exception as e:
        log.error("Failed to retrieve in-progress records", timezone=timezone, log_key="InProgressRecords", status="FAILED", error=str(e))
        return []

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass




from typing import Dict
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.drive_storage.drive_func import create_drive_connection

def get_target_count(final_config: Dict[str, any], record: Dict[str, any]) -> int:
    """
    Get row count from Snowflake raw table using the FILENAME pattern from the record.
    """
    try:
        sf_raw_config = final_config["sf_raw_config"]
        timezone = final_config.get("timezone", "UTC")
        conn = create_drive_connection(sf_raw_config)
        cursor = conn.cursor()

        filepattern = record["TARGET_SUBCATEGORY"]
        raw_table = sf_raw_config["raw_table"]

        query = f"""
            SELECT COUNT(*) FROM {raw_table}
            WHERE FILENAME LIKE '{filepattern}%'
        """

        cursor.execute(query)
        row = cursor.fetchone()
        count = row[0] if row else 0

        log.info("Target row count retrieved", timezone=timezone, log_key="TargetCount", status="SUCCESS", count=count, filepattern=filepattern)
        return count

    except Exception as e:
        log.error("Failed to retrieve target row count", log_key="TargetCount", status="FAILED", error=str(e))
        return 0

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass


def target_deletion(final_config: Dict[str, any], record: Dict[str, any]) -> bool:
    """
    Delete rows from Snowflake raw table using FILENAME pattern.
    """
    try:
        sf_raw_config = final_config["sf_raw_config"]
        timezone = final_config.get("timezone", "UTC")
        conn = create_drive_connection(sf_raw_config)
        cursor = conn.cursor()

        filepattern = record["TARGET_SUBCATEGORY"]
        raw_table = sf_raw_config["raw_table"]

        query = f"""
            DELETE FROM {raw_table}
            WHERE FILENAME LIKE '{filepattern}%'
        """

        cursor.execute(query)
        rows_affected = cursor.rowcount
        conn.commit()

        log.info("Target deletion completed", timezone=timezone, log_key="TargetDeletion", status="SUCCESS", rows_affected=rows_affected, filepattern=filepattern)
        return True

    except Exception as e:
        log.error("Failed to delete target data", log_key="TargetDeletion", status="FAILED", error=str(e))
        return False

    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass












