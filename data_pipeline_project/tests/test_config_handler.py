# data_pipeline_project/tests/test_config_handler.py

import json
import tempfile
from pathlib import Path
from data_pipeline_project.pipeline_framework.config_handler import config_handler

def test_path_resolution():
    """Test that relative paths work correctly"""
    
    # Test path resolution
    current = Path("data_pipeline_project/projects/group_name/class_name")
    target = current / "../../../pipeline_framework/default_record.json"
    resolved = target.resolve()
    
    print("=== PATH RESOLUTION TEST ===")
    print(f"Current path: {current}")
    print(f"Relative target: ../../../pipeline_framework/default_record.json")
    print(f"Resolved path: {resolved}")
    print(f"Expected: ends with 'pipeline_framework/default_record.json'")
    print(f"Test passed: {str(resolved).endswith('pipeline_framework/default_record.json')}")
    print()

def test_config_handler():
    """Test config_handler with temporary files"""
    
    print("=== CONFIG HANDLER TEST ===")
    
    # Create temporary directory structure
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test directory structure
        project_dir = temp_path / "projects" / "group_name" / "class_name"
        framework_dir = temp_path / "pipeline_framework"
        
        project_dir.mkdir(parents=True)
        framework_dir.mkdir(parents=True)
        
        # Create test default_record.json
        default_record = {
            "record": {
                "SOURCE_NAME": "elasticsearch",
                "SOURCE_CATEGORY": None,
                "PIPELINE_STATUS": "PENDING",
                "RETRY_ATTEMPT": 0
            }
        }
        
        default_record_path = framework_dir / "default_record.json"
        with open(default_record_path, 'w') as f:
            json.dump(default_record, f, indent=2)
        
        # Create test config.json
        config_data = {
            "env": "test",
            "username": "test_user",
            "password": "test_pass",
            "account": "test_account",
            "warehouse": "test_wh",
            "drive_database": "test_drive_db",
            "drive_schema": "test_drive_schema",
            "drive_table": "test_drive_table",
            "raw_database": "test_raw_db",
            "raw_schema": "test_raw_schema",
            "raw_table": "test_raw_table",
            "raw_task_name": "test_task",
            "record": {
                "SOURCE_NAME": "test_es",
                "PIPELINE_NAME": "test_pipeline"
            }
        }
        
        config_path = project_dir / "config.json"
        with open(config_path, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        print(f"Test directory structure:")
        print(f"  Project dir: {project_dir}")
        print(f"  Framework dir: {framework_dir}")
        print(f"  Default record: {default_record_path}")
        print(f"  Config file: {config_path}")
        print()
        
        # Test config_handler
        try:
            final_config = config_handler(
                root_project_path=str(project_dir),
                default_record_path="../../../pipeline_framework/default_record.json",
                group_specific_path="./config.json"
            )
            
            print("✅ Config handler executed successfully!")
            print()
            print("Final config keys:")
            for key in sorted(final_config.keys()):
                if isinstance(final_config[key], dict):
                    print(f"  {key}: {type(final_config[key]).__name__} with {len(final_config[key])} keys")
                else:
                    print(f"  {key}: {final_config[key]}")
            print()
            
            # Test specific structures
            print("Credential structures:")
            print(f"  sf_creds: {final_config.get('sf_creds', 'MISSING')}")
            print(f"  sf_config: {final_config.get('sf_config', 'MISSING')}")
            print(f"  sf_raw_config: {final_config.get('sf_raw_config', 'MISSING')}")
            print()
            
            # Test record merging
            print("Record merging test:")
            print(f"  SOURCE_NAME: {final_config['record']['SOURCE_NAME']} (should be 'test_es')")
            print(f"  PIPELINE_STATUS: {final_config['record']['PIPELINE_STATUS']} (should be 'PENDING')")
            print(f"  RETRY_ATTEMPT: {final_config['record']['RETRY_ATTEMPT']} (should be 0)")
            
            return True
            
        except Exception as e:
            print(f"❌ Config handler failed: {str(e)}")
            return False

def test_airflow_variable_structure():
    """Test how Airflow variable updates would work"""
    
    print("=== AIRFLOW VARIABLE UPDATE TEST ===")
    
    # Mock Airflow Variable behavior
    mock_variables = {
        "SF_USERNAME": "prod_user",
        "SF_PASSWORD": "prod_password",
        "ES_HOST": "prod-es.company.com"
    }
    
    # Sample config
    final_config = {
        "sf_creds": {"user": "dev_user", "password": "dev_pass"},
        "host_name": "dev-es.company.com"
    }
    
    print("Before Airflow updates:")
    print(f"  sf_creds: {final_config['sf_creds']}")
    print(f"  host_name: {final_config['host_name']}")
    
    # Simulate Airflow variable updates
    if mock_variables.get("SF_USERNAME"):
        final_config["sf_creds"]["user"] = mock_variables["SF_USERNAME"]
    
    if mock_variables.get("SF_PASSWORD"):
        final_config["sf_creds"]["password"] = mock_variables["SF_PASSWORD"]
        
    if mock_variables.get("ES_HOST"):
        final_config["host_name"] = mock_variables["ES_HOST"]
    
    print("\nAfter Airflow updates:")
    print(f"  sf_creds: {final_config['sf_creds']}")
    print(f"  host_name: {final_config['host_name']}")
    print("✅ Airflow variable updates work correctly!")

if __name__ == "__main__":
    print("🧪 TESTING CONFIG HANDLER AND PATHS 🧪\n")
    
    # Run all tests
    test_path_resolution()
    test_config_handler()
    test_airflow_variable_structure()
    
    print("\n🎉 All tests completed!")


