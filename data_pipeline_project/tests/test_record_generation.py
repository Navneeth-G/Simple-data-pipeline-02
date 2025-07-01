# data_pipeline_project/tests/test_record_generation.py

import pendulum
from data_pipeline_project.pipeline_framework.core.record_generation import (
    generate_pipeline_time_window_record,
    build_complete_pipeline_record
)

def test_time_window_generation():
    """Test basic time window generation."""
    print("=== Testing Time Window Generation ===")
    
    test_config = {
        'timezone': 'America/Los_Angeles',
        'x_time_back': '1d',
        'granularity': '2h'
    }
    
    try:
        result = generate_pipeline_time_window_record(test_config)
        print(f"✅ Success! Target day: {result['target_day_str']}")
        print(f"   Window: {result['window_start_time'].to_time_string()} - {result['window_end_time'].to_time_string()}")
        print(f"   Interval: {result['time_interval']}")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def test_complete_record_building():
    """Test complete record building with mock default_record."""
    print("\n=== Testing Complete Record Building ===")
    
    test_config = {
        'timezone': 'UTC',
        'x_time_back': '2h30m',
        'granularity': '1h',
        'default_record': {
            'source_name': 'test_source',
            'target_name': 'test_target',
            'pipeline_id': 'test_pipeline_123',
            'status': 'PENDING'
        }
    }
    
    try:
        result = build_complete_pipeline_record(test_config)
        print(f"✅ Success! Record keys: {list(result.keys())}")
        print(f"   TARGET_DAY: {result['TARGET_DAY'].to_date_string()}")
        print(f"   TIME_INTERVAL: {result['TIME_INTERVAL']}")
        print(f"   Source: {result['source_name']}")
        return True
    except Exception as e:
        print(f"❌ Failed: {e}")
        return False

def test_edge_cases():
    """Test edge cases like boundary conditions."""
    print("\n=== Testing Edge Cases ===")
    
    # Test granularity larger than remaining day
    test_config = {
        'timezone': 'UTC',
        'x_time_back': '23h',  # Very late in previous day
        'granularity': '5h'    # Would exceed day boundary
    }
    
    try:
        result = generate_pipeline_time_window_record(test_config)
        print(f"✅ Boundary test passed!")
        print(f"   Requested: 5h, Actual: {result['time_interval']}")
        return True
    except Exception as e:
        print(f"❌ Boundary test failed: {e}")
        return False

def run_all_tests():
    """Run all tests."""
    print("🚀 Starting Record Generation Tests\n")
    
    tests = [
        test_time_window_generation,
        test_complete_record_building,
        test_edge_cases
    ]
    
    passed = 0
    for test in tests:
        if test():
            passed += 1
    
    print(f"\n📊 Results: {passed}/{len(tests)} tests passed")
    if passed == len(tests):
        print("🎉 All tests passed!")
    else:
        print("⚠️  Some tests failed")

if __name__ == "__main__":
    run_all_tests()



