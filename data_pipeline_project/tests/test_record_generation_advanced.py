# data_pipeline_project/tests/test_record_generation_advanced.py

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pendulum
from pipeline_framework.core.record_generation import (
    generate_pipeline_time_window_record,
    build_complete_pipeline_record
)

# Mock function to replace latest_timestamp_available
def mock_latest_timestamp_available(scenario: str = "none") -> pendulum.DateTime:
    """
    Mock function that returns different timestamps for testing.
    Scenarios:
    - 'none': No previous data (returns None)
    - 'early': Early in target day
    - 'middle': Middle of target day  
    - 'late': Late in target day (near boundary)
    - 'exact_boundary': Exactly at next day boundary
    - 'beyond_boundary': Beyond target day (should trigger completion)
    - 'before_target': Before target day (should trigger completion)
    """
    
    if scenario == "none":
        return None
    
    # Base target day (assuming x_time_back='1d' from current time)
    base_target_day = pendulum.now('UTC').subtract(days=1).start_of('day')
    
    scenarios = {
        "early": base_target_day.add(hours=2),      # 02:00:00
        "middle": base_target_day.add(hours=12),    # 12:00:00  
        "late": base_target_day.add(hours=22),      # 22:00:00
        "exact_boundary": base_target_day.add(days=1),  # Next day 00:00:00
        "beyond_boundary": base_target_day.add(days=1, hours=1),  # Next day 01:00:00
        "before_target": base_target_day.subtract(hours=1)  # Previous day 23:00:00
    }
    
    return scenarios.get(scenario, None)

def test_continuation_scenarios():
    """Test different continuation timestamp scenarios."""
    print("=== Testing Continuation Scenarios ===\n")
    
    base_config = {
        'timezone': 'UTC',
        'x_time_back': '1d',
        'granularity': '3h'
    }
    
    scenarios = ["none", "early", "middle", "late"]
    
    for scenario in scenarios:
        print(f"--- Scenario: {scenario.upper()} ---")
        
        # Temporarily replace the function in the module
        import pipeline_framework.core.record_generation as rg
        original_func = rg.latest_timestamp_available
        rg.latest_timestamp_available = lambda: mock_latest_timestamp_available(scenario)
        
        try:
            result = generate_pipeline_time_window_record(base_config)
            
            if result is None:
                print(f"✅ Scenario '{scenario}' - Processing complete")
            else:
                print(f"✅ Scenario '{scenario}' successful:")
                print(f"   Target Day: {result['target_day_str']}")
                print(f"   Window: {result['window_start_time'].to_time_string()} - {result['window_end_time'].to_time_string()}")
                print(f"   Requested: 3h, Actual: {result['time_interval']}")
                
                # Check if we're at boundary
                next_day = result['target_day'].add(days=1)
                if result['window_end_time'] >= next_day:
                    print(f"   🚨 BOUNDARY HIT: Window capped at next day!")
                else:
                    print(f"   ✅ Within day boundary")
                
        except Exception as e:
            print(f"❌ Scenario '{scenario}' failed: {e}")
            
        finally:
            # Restore original function
            rg.latest_timestamp_available = original_func
            
        print()

def test_processing_complete_scenarios():
    """Test scenarios where processing should be complete."""
    print("=== Testing Processing Complete Scenarios ===\n")
    
    base_config = {
        'timezone': 'UTC',
        'x_time_back': '1d',
        'granularity': '1h'
    }
    
    import pipeline_framework.core.record_generation as rg
    original_func = rg.latest_timestamp_available
    
    completion_scenarios = ["beyond_boundary", "before_target", "exact_boundary"]
    
    for scenario in completion_scenarios:
        print(f"--- {scenario.replace('_', ' ').title()} Scenario ---")
        
        rg.latest_timestamp_available = lambda s=scenario: mock_latest_timestamp_available(s)
        
        try:
            result = generate_pipeline_time_window_record(base_config)
            
            if result is None:
                print(f"✅ Correctly identified processing as complete")
            else:
                print(f"❌ Should have returned None for complete processing")
                print(f"   Got window: {result['window_start_time'].to_time_string()} - {result['window_end_time'].to_time_string()}")
                
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
        
        finally:
            rg.latest_timestamp_available = original_func
        
        print()

def test_boundary_crossing_cases():
    """Test specific boundary crossing scenarios."""
    print("=== Testing Boundary Crossing Cases ===\n")
    
    import pipeline_framework.core.record_generation as rg
    original_func = rg.latest_timestamp_available
    
    # Test case 1: Late timestamp + large granularity
    print("--- Case 1: Late timestamp (22:00) + 5h granularity ---")
    rg.latest_timestamp_available = lambda: mock_latest_timestamp_available("late")
    
    config1 = {
        'timezone': 'UTC',
        'x_time_back': '1d',
        'granularity': '5h'  # 22:00 + 5h = 03:00 next day (should be capped)
    }
    
    try:
        result1 = generate_pipeline_time_window_record(config1)
        
        if result1 is None:
            print("✅ Processing complete - no boundary crossing test needed")
        else:
            print(f"✅ Window: {result1['window_start_time'].to_time_string()} - {result1['window_end_time'].to_time_string()}")
            print(f"   Requested: 5h, Actual: {result1['time_interval']}")
            
            # Calculate expected vs actual
            expected_end = result1['window_start_time'].add(hours=5)
            actual_end = result1['window_end_time']
            
            if actual_end < expected_end:
                print(f"   🚨 CAPPED: Would have been {expected_end.to_time_string()}, capped to {actual_end.to_time_string()}")
            else:
                print(f"   ✅ No capping needed")
            
    except Exception as e:
        print(f"❌ Case 1 failed: {e}")
    
    # Test case 2: Very late timestamp + small granularity  
    print("\n--- Case 2: Very late timestamp (23:30) + 1h granularity ---")
    
    # Create custom late timestamp
    target_day = pendulum.now('UTC').subtract(days=1).start_of('day')
    very_late_timestamp = target_day.add(hours=23, minutes=30)
    rg.latest_timestamp_available = lambda: very_late_timestamp
    
    config2 = {
        'timezone': 'UTC',
        'x_time_back': '1d',
        'granularity': '1h'  # 23:30 + 1h = 00:30 next day (should be capped)
    }
    
    try:
        result2 = generate_pipeline_time_window_record(config2)
        
        if result2 is None:
            print("✅ Processing complete - no boundary crossing test needed")
        else:
            print(f"✅ Window: {result2['window_start_time'].to_time_string()} - {result2['window_end_time'].to_time_string()}")
            print(f"   Requested: 1h, Actual: {result2['time_interval']}")
        
    except Exception as e:
        print(f"❌ Case 2 failed: {e}")
    
    # Restore original function
    rg.latest_timestamp_available = original_func

def test_complete_record_with_completion():
    """Test complete record building with processing completion scenarios."""
    print("\n=== Testing Complete Record Building with Completion ===")
    
    import pipeline_framework.core.record_generation as rg
    original_func = rg.latest_timestamp_available
    
    # Test with beyond boundary timestamp
    rg.latest_timestamp_available = lambda: mock_latest_timestamp_available("beyond_boundary")
    
    config = {
        'timezone': 'UTC',
        'x_time_back': '1d',
        'granularity': '1h',
        'default_record': {
            'source_name': 'test_source',
            'target_name': 'test_target',
            'pipeline_id': 'test_pipeline_123',
            'status': 'PENDING'
        }
    }
    
    try:
        result = build_complete_pipeline_record(config)
        
        if result is None:
            print("✅ Complete record building correctly skipped - processing complete")
        else:
            print("❌ Should have returned None for complete processing")
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    
    finally:
        rg.latest_timestamp_available = original_func

def run_advanced_tests():
    """Run all advanced tests."""
    print("🚀 Starting Advanced Record Generation Tests\n")
    
    test_continuation_scenarios()
    test_processing_complete_scenarios()
    test_boundary_crossing_cases() 
    test_complete_record_with_completion()
    
    print("\n🎯 Advanced testing completed!")

if __name__ == "__main__":
    run_advanced_tests()