# data_pipeline_project/tests/test_record_validation_rebuild.py

import pendulum
from data_pipeline_project.pipeline_framework.core.record_validation import validate_record_timing, get_oldest_pending_record

def test_complete_record():
   """Test record that's already complete"""
   print("\n=== TEST 1: Complete Record (No Rebuild Needed) ===")
   
   original_get_record = get_oldest_pending_record
   def mock_complete_record():
       return {
           'TARGET_DAY': pendulum.parse('2025-06-29T00:00:00'),
           'WINDOW_START_TIME': pendulum.parse('2025-06-29T10:00:00'),
           'WINDOW_END_TIME': pendulum.parse('2025-06-29T12:00:00'),
           'TIME_INTERVAL': '2h',
           'key_x': 'existing_value_x',
           'key_y': 'existing_value_y',
           'key_z': 'existing_value_z'
       }
   
   import data_pipeline_project.pipeline_framework.core.record_validation as rv
   rv.get_oldest_pending_record = mock_complete_record
   
   config = {
       'timezone': 'America/Los_Angeles',
       'x_time_back': '1d',
       'check_fields': ['key_x', 'key_y', 'key_z']
   }
   
   result = validate_record_timing(config)
   print(f"Result: exit_dag={result['exit_dag']}")
   print(f"Record key_x: {result['record']['key_x']}")
   assert result['exit_dag'] == False
   assert result['record']['key_x'] == 'existing_value_x'
   print("✅ PASSED: Complete record - no rebuild")
   
   rv.get_oldest_pending_record = original_get_record

def test_incomplete_record_rebuild():
   """Test record with missing fields that needs rebuild"""
   print("\n=== TEST 2: Incomplete Record (Rebuild Required) ===")
   
   original_get_record = get_oldest_pending_record
   def mock_incomplete_record():
       return {
           'TARGET_DAY': pendulum.parse('2025-06-29T00:00:00'),
           'WINDOW_START_TIME': pendulum.parse('2025-06-29T10:00:00'),
           'WINDOW_END_TIME': pendulum.parse('2025-06-29T12:00:00'),
           'TIME_INTERVAL': '2h',
           'key_x': None,  # Missing
           'key_y': '',    # Empty
           'key_z': 'existing_value'
       }
   
   import data_pipeline_project.pipeline_framework.core.record_validation as rv
   rv.get_oldest_pending_record = mock_incomplete_record
   
   config = {
       'timezone': 'America/Los_Angeles',
       'x_time_back': '1d',
       'check_fields': ['key_x', 'key_y', 'key_z']
   }
   
   result = validate_record_timing(config)
   print(f"Result: exit_dag={result['exit_dag']}")
   print(f"Record key_x: {result['record']['key_x']}")
   print(f"Record key_y: {result['record']['key_y']}")
   assert result['exit_dag'] == False
   assert result['record']['key_x'] == 'placeholder_value_x'  # Rebuilt
   assert result['record']['key_y'] == 'placeholder_value_y'  # Rebuilt
   print("✅ PASSED: Incomplete record rebuilt")
   
   rv.get_oldest_pending_record = original_get_record

def test_no_check_fields():
   """Test when no check_fields specified"""
   print("\n=== TEST 3: No Check Fields (Skip Rebuild Check) ===")
   
   original_get_record = get_oldest_pending_record
   def mock_record_with_nulls():
       return {
           'TARGET_DAY': pendulum.parse('2025-06-29T00:00:00'),
           'WINDOW_START_TIME': pendulum.parse('2025-06-29T10:00:00'),
           'WINDOW_END_TIME': pendulum.parse('2025-06-29T12:00:00'),
           'TIME_INTERVAL': '2h',
           'key_x': None,  # These won't be checked
           'key_y': None
       }
   
   import data_pipeline_project.pipeline_framework.core.record_validation as rv
   rv.get_oldest_pending_record = mock_record_with_nulls
   
   config = {
       'timezone': 'America/Los_Angeles',
       'x_time_back': '1d'
       # No check_fields specified
   }
   
   result = validate_record_timing(config)
   print(f"Result: exit_dag={result['exit_dag']}")
   print(f"Record key_x: {result['record']['key_x']}")
   assert result['exit_dag'] == False
   assert result['record']['key_x'] is None  # Not rebuilt
   print("✅ PASSED: No check fields - no rebuild")
   
   rv.get_oldest_pending_record = original_get_record

if __name__ == "__main__":
   test_complete_record()
   test_incomplete_record_rebuild()
   test_no_check_fields()
   print("\n🎉 All record rebuild tests passed!")

