# data_pipeline_project/tests/test_invalid_continuation.py

def test_invalid_continuation_timestamp():
    """Test what happens with invalid continuation timestamp."""
    
    import pipeline_framework.core.record_generation as rg
    
    # Mock function that returns NEXT DAY timestamp
    def mock_next_day_timestamp():
        target_day = pendulum.now('UTC').subtract(days=1).start_of('day')
        return target_day.add(days=1)  # Return NEXT day start
    
    original_func = rg.latest_timestamp_available
    rg.latest_timestamp_available = mock_next_day_timestamp
    
    config = {
        'timezone': 'UTC',
        'x_time_back': '1d',
        'granularity': '1h'
    }
    
    try:
        result = generate_pipeline_time_window_record(config)
        print(f"❌ BUG: Allowed invalid window!")
        print(f"   Target Day: {result['target_day_str']}")
        print(f"   Window: {result['window_start_time']} - {result['window_end_time']}")
    except Exception as e:
        print(f"✅ Error caught: {e}")
    finally:
        rg.latest_timestamp_available = original_func



