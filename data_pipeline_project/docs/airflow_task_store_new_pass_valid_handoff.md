# Data Pipeline Framework - Store New and Pass Valid Record Task Documentation

## Overview

**Purpose:** This task combines record generation, database storage, and validation into a single Airflow-ready module that stores new records and passes valid pending records to downstream pipeline tasks.

**Key Function:** `store_new_and_pass_valid_record()` - generates new record → uploads to database → downloads oldest pending record → validates it → returns proceed/exit decision with valid record.

## What We Built

### New File Created
```
data_pipeline_project/pipeline_framework/airflow_tasks/
└── store_new_and_pass_valid_record.py
```

### Core Functionality
- **Record Generation**: Creates new time window records using existing framework
- **Database Upload**: Stores new records via placeholder function  
- **Database Download**: Retrieves oldest pending records via placeholder function
- **Record Validation**: Ensures records are complete and represent stable data
- **Pipeline Control**: Returns clear exit/proceed signals for Airflow orchestration

### Return Value Structure
```python
{
    'exit_dag': True | False,    # Clear signal for pipeline orchestration
    'record': Dict | None        # The validated complete record (if proceeding)
}
```

## Technical Implementation

### Main Function
```python
def store_new_and_pass_valid_record(final_config: Dict[str, Any]) -> Dict[str, Any]:
```

**Process Flow:**
1. Generate new record using `build_complete_pipeline_record()`
2. Upload new record using `upload_record_to_database()` 
3. Download oldest pending record using `download_oldest_pending_record()`
4. Validate oldest record using `validate_record_timing()`
5. Return validation result with proceed/exit decision

### Placeholder Functions (Need Implementation)
```python
def upload_record_to_database(record: Dict[str, Any]) -> bool:
    # TODO: Implement actual database upload logic
    
def download_oldest_pending_record() -> Optional[Dict[str, Any]]:
    # TODO: Implement actual database download logic
```

### Dependencies Used
- `record_generation.py` - For creating new records
- `record_validation.py` - For validating oldest pending records  
- `log_generator.py` - For structured logging
- `log_retry_decorators.py` - For execution timing

## Integration Guide

### Airflow DAG Integration
```python
# In your main DAG file
from data_pipeline_project.pipeline_framework.airflow_tasks.store_new_and_pass_valid_record import store_new_and_pass_valid_record

def airflow_task_wrapper(**context):
    config = {
        'timezone': 'America/Los_Angeles',
        'x_time_back': '1d', 
        'granularity': '2h',
        'default_record': {
            'source_name': 'elasticsearch',
            'target_name': 'snowflake',
            'pipeline_id': 'your_pipeline'
        }
    }
    
    result = store_new_and_pass_valid_record(config)
    
    if result['exit_dag']:
        # Skip downstream tasks
        raise AirflowSkipException("No valid record to process")
    
    # Pass valid record to downstream tasks
    return result['record']
```

### Configuration Requirements
```python
final_config = {
    'timezone': str,           # Processing timezone
    'x_time_back': str,        # Data stability threshold (e.g., '1d')
    'granularity': str,        # Time window size (e.g., '2h')
    'default_record': dict,    # Base record template
    'check_fields': list       # Optional: Fields to validate for completeness
}
```

## Testing Instructions

### Basic Test
```bash
cd E:\ClaudeAI-project_07
python data_pipeline_project/pipeline_framework/airflow_tasks/store_new_and_pass_valid_record.py
```

**Expected Output:**
- Structured logging showing all process steps
- Execution timing information
- Result dictionary with exit_dag and record fields
- No exceptions raised

### Integration Test with Existing Components
```bash
# Test record generation
python -m data_pipeline_project.tests.test_record_generation

# Test record validation  
python -m data_pipeline_project.tests.test_record_validation_rebuild

# Test combined functionality (manual verification)
python data_pipeline_project/pipeline_framework/airflow_tasks/store_new_and_pass_valid_record.py
```

## Next Steps for Teammate

### Immediate Priority: Database Implementation

**1. Choose Database Backend**
- Options: PostgreSQL, SQLite, MySQL, etc.
- Consider: Team preferences, existing infrastructure, scalability needs

**2. Implement Upload Function**
```python
def upload_record_to_database(record: Dict[str, Any]) -> bool:
    # TODO: Implement your database logic
    # - Connect to database
    # - Insert record into records table
    # - Set status = 'PENDING'
    # - Return True/False for success
```

**3. Implement Download Function**  
```python
def download_oldest_pending_record() -> Optional[Dict[str, Any]]:
    # TODO: Implement your database logic
    # - Connect to database  
    # - Query: SELECT * FROM records WHERE status='PENDING' ORDER BY created_at LIMIT 1
    # - Convert to dictionary with proper data types
    # - Return record or None
```

### Database Schema Suggestion
```sql
CREATE TABLE pipeline_records (
    id SERIAL PRIMARY KEY,
    target_day DATE,
    window_start_time TIMESTAMP,
    window_end_time TIMESTAMP, 
    time_interval VARCHAR(10),
    status VARCHAR(20) DEFAULT 'PENDING',
    key_x TEXT,
    key_y TEXT, 
    key_z TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

### Integration with Existing Placeholder Functions

**Update record_validation.py:**
```python
def get_oldest_pending_record() -> Optional[Dict[str, Any]]:
    # Replace placeholder with actual database call
    # Should match download_oldest_pending_record() logic
```

### Airflow DAG Development

**Create Main DAG File:**
- Import the new task function
- Create Airflow task wrapper
- Handle exit_dag signals (skip downstream tasks)
- Pass valid records via XCom to next tasks
- Add retry policies and error handling

### Next Pipeline Components to Develop

**1. Source Operations Task**
- Extract data from Elasticsearch using validated record
- Handle large dataset transfers
- Implement count validation

**2. Stage Operations Task**  
- Transfer data to S3 staging
- Manage staging lifecycle
- Handle transfer failures

**3. Target Operations Task**
- Load data to Snowflake
- Handle asynchronous loading
- Implement loading validation

## Handoff Checklist

### ✅ Complete and Ready
- [x] **store_new_and_pass_valid_record.py** - Full implementation with placeholders
- [x] **Integration with existing framework** - Uses record generation and validation  
- [x] **Comprehensive logging** - Structured logging throughout process
- [x] **Error handling** - Proper exception handling and cleanup
- [x] **Test infrastructure** - Basic testing capability included
- [x] **Clear documentation** - This complete handoff guide

### 🔄 Needs Implementation  
- [ ] **Database upload function** - Replace `upload_record_to_database()` placeholder
- [ ] **Database download function** - Replace `download_oldest_pending_record()` placeholder  
- [ ] **Database schema** - Create records table with proper structure
- [ ] **Airflow DAG wrapper** - Create task wrapper for Airflow integration
- [ ] **Integration testing** - Test with actual database backend

### 📋 Development Priorities
1. **Database Implementation** (High Priority)
2. **Airflow Integration** (High Priority)  
3. **Source Operations Task** (Medium Priority)
4. **Stage/Target Operations** (Medium Priority)
5. **Comprehensive Testing** (Ongoing)

### 🚀 Framework Status
- **Record Generation**: ✅ Complete with comprehensive testing
- **Record Validation**: ✅ Complete with rebuild capability
- **Combined Task**: ✅ Complete with placeholder database functions
- **Database Layer**: 🔄 Ready for implementation
- **Pipeline Framework**: 🔄 Foundation complete, ready for expansion

---

**Status:** Complete handoff package ready. Framework foundation established with clear implementation path for database integration and Airflow orchestration.