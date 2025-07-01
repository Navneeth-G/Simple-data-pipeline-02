# data_pipeline_project/pipeline_framework/database/query_executor.py

import snowflake.connector
import pandas as pd
from typing import Dict, Any, List, Optional, Union
from data_pipeline_project.pipeline_framework.utils.log_generator import log
from data_pipeline_project.pipeline_framework.utils.log_retry_decorators import retry
from data_pipeline_project.pipeline_framework.utils.time_utils import convert_to_pendulum

class QueryExecutor:
   def __init__(self, sf_creds: Dict[str, Any], sf_config: Dict[str, Any], final_config: Dict[str, Any]):
       self.sf_creds = sf_creds
       self.sf_config = sf_config
       self.final_config = final_config
       
   def _get_connection(self):
       """Get Snowflake connection"""
       return snowflake.connector.connect(
           user=self.sf_creds['user'],
           password=self.sf_creds['password'], 
           account=self.sf_creds['account'],
           warehouse=self.sf_config['warehouse'],
           database=self.sf_config['database'],
           schema=self.sf_config['schema']
       )
   
   def _convert_timestamps_in_dict(self, record: Dict[str, Any], timestamp_columns: List[str]) -> Dict[str, Any]:
       """Convert timestamp columns in single record"""
       if not timestamp_columns:
           return record
           
       for col in timestamp_columns:
           if col in record and record[col] is not None:
               try:
                   record[col] = convert_to_pendulum(record[col], self.final_config.get('timezone'))
               except Exception as e:
                   log.warning(f"Failed to convert {col}: {e}", log_key="TimestampConversion", status="FAILED")
       return record
   
   @retry(max_attempts=3, delay_seconds=5)
   def execute_scalar(self, query: str, params: Dict[str, Any] = None) -> Union[int, float, str, None]:
       """Execute query returning single value (COUNT, MAX, MIN)"""
       log.info("Executing scalar query", log_key="ScalarQuery", status="STARTED")
       
       conn = None
       try:
           conn = self._get_connection()
           cursor = conn.cursor()
           cursor.execute(query, params or {})
           
           result = cursor.fetchone()
           value = result[0] if result else None
           
           log.info("Scalar query completed", log_key="ScalarQuery", status="SUCCESS", value=value)
           return value
           
       except Exception as e:
           log.error(f"Scalar query failed: {str(e)}", log_key="ScalarQuery", status="FAILED")
           raise
       finally:
           if conn:
               conn.close()
   
   @retry(max_attempts=3, delay_seconds=5)
   def execute_single_row(self, query: str, params: Dict[str, Any] = None, 
                         timestamp_columns: List[str] = None) -> Optional[Dict[str, Any]]:
       """Execute query returning single record as dict"""
       log.info("Executing single row query", log_key="SingleRowQuery", status="STARTED")
       
       conn = None
       try:
           conn = self._get_connection()
           cursor = conn.cursor()
           cursor.execute(query, params or {})
           
           df = cursor.fetch_pandas_all()
           
           if df.empty:
               log.info("Single row query - no results", log_key="SingleRowQuery", status="NO_RESULTS")
               return None
           
           # Convert to dict first, then convert timestamps
           record = df.iloc[0].to_dict()
           record = self._convert_timestamps_in_dict(record, timestamp_columns or [])
           
           log.info("Single row query completed", log_key="SingleRowQuery", status="SUCCESS")
           return record
           
       except Exception as e:
           log.error(f"Single row query failed: {str(e)}", log_key="SingleRowQuery", status="FAILED")
           raise
       finally:
           if conn:
               conn.close()
   
   @retry(max_attempts=3, delay_seconds=5)
   def execute_multiple_rows(self, query: str, params: Dict[str, Any] = None, 
                            timestamp_columns: List[str] = None) -> List[Dict[str, Any]]:
       """Execute query returning multiple records as list of dicts"""
       log.info("Executing multiple rows query", log_key="MultipleRowsQuery", status="STARTED")
       
       conn = None
       try:
           conn = self._get_connection()
           cursor = conn.cursor()
           cursor.execute(query, params or {})
           
           df = cursor.fetch_pandas_all()
           
           # Convert to list of dicts first
           records = df.to_dict('records')
           
           # Then convert timestamps in each dict
           if timestamp_columns:
               records = [self._convert_timestamps_in_dict(record, timestamp_columns) for record in records]
           
           log.info("Multiple rows query completed", log_key="MultipleRowsQuery", status="SUCCESS", row_count=len(records))
           return records
           
       except Exception as e:
           log.error(f"Multiple rows query failed: {str(e)}", log_key="MultipleRowsQuery", status="FAILED")
           raise
       finally:
           if conn:
               conn.close()
   
   @retry(max_attempts=3, delay_seconds=5)
   def execute_statement(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
       """Execute DDL/DML statements (CREATE, DELETE, INSERT, UPDATE, EXECUTE TASK)"""
       log.info("Executing statement", log_key="StatementQuery", status="STARTED")
       
       conn = None
       try:
           conn = self._get_connection()
           cursor = conn.cursor()
           cursor.execute(query, params or {})
           
           rows_affected = cursor.rowcount
           query_id = cursor.sfqid
           
           log.info("Statement executed successfully", log_key="StatementQuery", 
                   status="SUCCESS", rows_affected=rows_affected, query_id=query_id)
           
           return {
               'success': True,
               'rows_affected': rows_affected,
               'query_id': query_id
           }
           
       except Exception as e:
           log.error(f"Statement execution failed: {str(e)}", log_key="StatementQuery", status="FAILED")
           raise
       finally:
           if conn:
               conn.close()

def get_query_executor(sf_creds: Dict[str, Any], sf_config: Dict[str, Any], final_config: Dict[str, Any]) -> QueryExecutor:
   """Factory function"""
   return QueryExecutor(sf_creds, sf_config, final_config)