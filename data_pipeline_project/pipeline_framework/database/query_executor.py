# data_pipeline_project/pipeline_framework/database/query_executor.py

import snowflake.connector
from typing import Dict, Any
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
    
    def _convert_timestamps_to_pendulum(self, cursor, row) -> Dict[str, Any]:
        """Convert row to dict with timestamp columns as pendulum objects"""
        columns = [desc[0] for desc in cursor.description]
        row_dict = dict(zip(columns, row))
        
        # Detect timestamp columns by data type
        for i, desc in enumerate(cursor.description):
            column_name = desc[0]
            data_type = str(desc[1]).upper()
            
            if any(ts_type in data_type for ts_type in ['TIMESTAMP', 'DATE', 'TIME']) and row[i] is not None:
                try:
                    row_dict[column_name] = convert_to_pendulum(row[i], self.final_config.get('timezone'))
                except Exception as e:
                    log.warning(f"Failed to convert {column_name} to pendulum: {e}", 
                              log_key="TimestampConversion", status="FAILED")
                    # Keep original value if conversion fails
                    row_dict[column_name] = row[i]
        
        return row_dict
    
    @retry(max_attempts=3, delay_seconds=5)
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute SQL query with automatic timestamp conversion"""
        log.info("Executing query", log_key="QueryExecutor", status="STARTED")
        
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query, params or {})
            query_id = cursor.sfqid
            
            if cursor.description is None:
                result_type = "statement"
                data = {'rows_affected': cursor.rowcount}
            else:
                rows = cursor.fetchall()
                if len(rows) == 1 and len(rows[0]) == 1:
                    result_type = "scalar"
                    # Single scalar might be timestamp too
                    value = rows[0][0]
                    if cursor.description[0] and any(ts_type in str(cursor.description[0][1]).upper() 
                                                   for ts_type in ['TIMESTAMP', 'DATE', 'TIME']) and value is not None:
                        try:
                            value = convert_to_pendulum(value, self.final_config.get('timezone'))
                        except:
                            pass  # Keep original if conversion fails
                    data = {'value': value}
                else:
                    result_type = "records"
                    data = {'records': [self._convert_timestamps_to_pendulum(cursor, row) for row in rows]}
            
            log.info("Query executed successfully", log_key="QueryExecutor", 
                    status="SUCCESS", query_id=query_id, result_type=result_type)
            
            return {
                'success': True, 
                'query_id': query_id, 
                'result_type': result_type,
                'data': data
            }
            
        except Exception as e:
            log.error(f"Query failed: {str(e)}", log_key="QueryExecutor", status="FAILED")
            raise
        finally:
            if conn:
                conn.close()

def get_query_executor(sf_creds: Dict[str, Any], sf_config: Dict[str, Any], final_config: Dict[str, Any]) -> QueryExecutor:
    """Factory function"""
    return QueryExecutor(sf_creds, sf_config, final_config)


