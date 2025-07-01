# data_pipeline_project/pipeline_framework/utils/time_utils.py

import re
import pendulum
from typing import Union
from datetime import datetime
import pandas as pd

TIME_PATTERN = re.compile(r'(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?')

def parse_time_to_seconds(time_str: str) -> int:
    """Convert time string like '1d2h30m20s' to total seconds."""
    if not time_str:
        raise ValueError("Time string cannot be empty")
        
    match = TIME_PATTERN.fullmatch(time_str.strip())
    if not match:
        raise ValueError(f"Invalid time format: {time_str}")
    
    days, hours, minutes, seconds = match.groups()
    total_seconds = (
        int(days or 0) * 86400 +
        int(hours or 0) * 3600 +
        int(minutes or 0) * 60 +
        int(seconds or 0)
    )
    
    if total_seconds == 0:
        raise ValueError(f"Time string results in zero duration: {time_str}")
        
    return total_seconds

def seconds_to_time_string(seconds: int) -> str:
    """Convert seconds back to time string format like '1d2h30m20s'."""
    if seconds <= 0:
        return "0s"
    
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0:
        parts.append(f"{secs}s")
    
    return "".join(parts) if parts else "0s"

def convert_to_pendulum(timestamp: Union[str, datetime, pd.Timestamp, pendulum.DateTime, int, float], 
                       timezone: str = None) -> pendulum.DateTime:
    """
    Convert any timestamp format to pendulum.DateTime via ISO string.
    
    Args:
        timestamp: Various timestamp formats
        timezone: Default timezone if timestamp has no timezone info
        
    Returns:
        pendulum.DateTime object
    """
    
    if isinstance(timestamp, pendulum.DateTime):
        return timestamp
    
    if isinstance(timestamp, str) and timestamp.lower() == 'now':
        if not timezone:
            raise ValueError("Timezone required for 'now'")
        return pendulum.now(timezone)
    
    try:
        if isinstance(timestamp, str):
            iso_string = timestamp
        
        elif isinstance(timestamp, datetime):
            iso_string = timestamp.isoformat()
        
        elif isinstance(timestamp, pd.Timestamp):
            iso_string = timestamp.isoformat()
        
        elif isinstance(timestamp, (int, float)):
            dt = datetime.fromtimestamp(timestamp)
            iso_string = dt.isoformat()
        
        else:
            raise ValueError(f"Unsupported timestamp type: {type(timestamp)}")
        
        return pendulum.parse(iso_string)
    
    except Exception as e:
        raise ValueError(f"Failed to convert timestamp {timestamp} to pendulum: {str(e)}")

def calculate_day_from_timestamp(timestamp: Union[str, datetime, pd.Timestamp, pendulum.DateTime, int, float], 
                                time_back: str, 
                                timezone: str = None) -> pendulum.DateTime:
    """
    Calculate target day by subtracting time_back from given timestamp.
    
    Args:
        timestamp: Any timestamp format
        time_back: Time string like '1d2h30m'
        timezone: Timezone for string timestamps without timezone info
    
    Returns:
        pendulum.DateTime: Start of target day
    """
    
    base_time = convert_to_pendulum(timestamp, timezone)
    time_back_seconds = parse_time_to_seconds(time_back)
    adjusted_time = base_time.subtract(seconds=time_back_seconds)
    return adjusted_time.start_of('day')



