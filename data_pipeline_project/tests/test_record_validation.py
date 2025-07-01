# data_pipeline_project/tests/test_record_validation.py

import pendulum
from data_pipeline_project.pipeline_framework.core.record_validation import validate_record_timing, get_oldest_pending_record

def test_valid_record():
   """Test record with stable data"""
   print("\n=== TEST 1: Valid Record (Stable Data) ===")
   
   # Mock record with past data
   original_get_record = get_oldest_pending_record
   def mock_past_record():
       return {
           'TARGET_DAY': pendulum.parse('2025-06-29T00:00:00'),
           'WINDOW_START_TIME': pendulum.parse('2025-06-29T10:00:00'),
           'WINDOW_END_TIME': pendulum.parse('2025-06-29T12:00:00'),
           'TIME_INTERVAL': '2h'
       }
   
   import data_pipeline_project.pipeline_framework.core.record_validation as rv
   rv.get_oldest_pending_record = mock_past_record
   
   config = {
       'timezone': 'America/Los_Angeles',
       'x_time_back': '1d'
   }
   
   result = validate_record_timing(config)
   print(f"Result: {result}")
   assert result['exit_dag'] == False
   assert result['record'] is not None
   print("✅ PASSED: Valid record")
   
   rv.get_oldest_pending_record = original_get_record

def test_future_data_record():
   """Test record with future data"""
   print("\n=== TEST 2: Future Data Record ===")
   
   original_get_record = get_oldest_pending_record
   def mock_future_record():
       now = pendulum.now('America/Los_Angeles')
       return {
           'TARGET_DAY': now.to_date_string(),
           'WINDOW_START_TIME': now.add(hours=1),
           'WINDOW_END_TIME': now.add(hours=3),
           'TIME_INTERVAL': '2h'
       }
   
   import data_pipeline_project.pipeline_framework.core.record_validation as rv
   rv.get_oldest_pending_record = mock_future_record
   
   config = {
       'timezone': 'America/Los_Angeles',
       'x_time_back': '2h'
   }
   
   result = validate_record_timing(config)
   print(f"Result: {result}")
   assert result['exit_dag'] == True
   assert result['record'] is not None
   print("✅ PASSED: Future data detected")
   
   rv.get_oldest_pending_record = original_get_record

def test_no_records():
   """Test when no records available"""
   print("\n=== TEST 3: No Records Available ===")
   
   original_get_record = get_oldest_pending_record
   def mock_no_record():
       return None
   
   import data_pipeline_project.pipeline_framework.core.record_validation as rv
   rv.get_oldest_pending_record = mock_no_record
   
   config = {
       'timezone': 'America/Los_Angeles',
       'x_time_back': '1d'
   }
   
   result = validate_record_timing(config)
   print(f"Result: {result}")
   assert result['exit_dag'] == True
   assert result['record'] is None
   print("✅ PASSED: No records handled")
   
   rv.get_oldest_pending_record = original_get_record

if __name__ == "__main__":
   test_valid_record()
   test_future_data_record()
   test_no_records()
   print("\n🎉 All record validation tests passed!")



