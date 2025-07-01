# data_pipeline_project/config_handler.py

import json
from pathlib import Path
from typing import Dict, Any

def config_handler(root_project_path: str, default_record_path: str, group_specific_path: str) -> Dict[str, Any]:
    """
    Load and merge configuration from multiple JSON files
    
    Args:
        root_project_path: Base path to the project directory
        default_record_path: Relative path to default record config
        group_specific_path: Relative path to group-specific config
    
    Returns:
        Merged final_config dictionary
    """
    
    root_path = Path(root_project_path)
    
    # Load default record configuration
    default_config_path = root_path / default_record_path
    if not default_config_path.exists():
        raise FileNotFoundError(f"Default config file not found: {default_config_path}")
    
    with open(default_config_path, 'r') as f:
        default_config = json.load(f)
    
    # Load group-specific configuration
    group_config_path = root_path / group_specific_path
    if not group_config_path.exists():
        raise FileNotFoundError(f"Group config file not found: {group_config_path}")
    
    with open(group_config_path, 'r') as f:
        group_config = json.load(f)
    
    # Start with group config as base
    final_config = {}
    final_config.update(group_config)
    
    # Special handling for record section - merge instead of replace
    if 'record' in default_config and 'record' in group_config:
        merged_record = {}
        merged_record.update(default_config['record'])  # Start with defaults
        merged_record.update(group_config['record'])    # Override with group-specific
        final_config['record'] = merged_record
    elif 'record' in default_config:
        final_config['record'] = default_config['record']
    
    # Build Snowflake credential structures
    final_config['sf_creds'] = {
        'user': final_config['username'],
        'password': final_config['password'],
        'account': final_config['account']
    }
    
    # Drive database config (for pipeline records)
    final_config['sf_config'] = {
        'warehouse': final_config['warehouse'],
        'database': final_config['drive_database'],
        'schema': final_config['drive_schema'],
        'table': final_config['drive_table']
    }
    
    # Raw database config (for target data)
    final_config['sf_raw_config'] = {
        'warehouse': final_config['warehouse'],
        'database': final_config['raw_database'],
        'schema': final_config['raw_schema'],
        'table': final_config['raw_table'],
        'task_name': final_config['raw_task_name']
    }
    
    return final_config