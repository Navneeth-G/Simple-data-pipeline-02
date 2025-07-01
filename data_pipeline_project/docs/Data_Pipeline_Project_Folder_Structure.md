# Data Pipeline Project - Folder Structure

```
data_pipeline_project/
│   .env                                          # Environment variables
│   README.md                                     # Project overview
│   __init__.py                                   # Python package marker
│   
├── .vscode/                                      # VSCode configuration
│   ├── launch.json                               # Debug configuration
│   └── settings.json                             # IDE settings
│       
├── docs/                                         # 📚 Documentation
│   ├── airflow_task_store_new_pass_valid_handoff.md
│   ├── data_pipeline_transfer_implementation_guide.md
│   ├── pre_validation_task_implementation_guide.md
│   ├── record_generation_complete_documentation.md
│   ├── record_validation_complete_documentation.md
│   └── universal_data_pipeline_framework_docs.md  # 🎯 Main documentation
│       
├── implementation_modules/                       # 🔌 System-specific implementations
│   ├── __init__.py
│   ├── elasticsearch_source/                     # Source system integration
│   │   └── __init__.py
│   ├── s3_stage/                                 # Staging system integration
│   │   └── __init__.py
│   └── snowflake_target/                         # Target system integration
│       └── __init__.py
│
├── pipeline_framework/                           # ⚙️ Universal framework (core)
│   ├── __init__.py
│   ├── airflow_tasks/                            # 🚀 Airflow task implementations
│   │   ├── audit_task.py                         # ✅ Data integrity validation
│   │   ├── cleanup_stale_locks.py                # 🧹 Operational hygiene
│   │   ├── pre_validation_count_check.py         # ✅ Resource optimization
│   │   ├── source_to_stage_transfer.py           # ✅ Data movement (S→Stage)
│   │   ├── stage_to_target_transfer.py           # ✅ Data loading (Stage→T)
│   │   └── store_new_and_pass_valid_record.py    # ✅ Record gen/validation/locking
│   │   
│   ├── core/                                     # 🧠 Core business logic
│   │   ├── __init__.py
│   │   ├── record_generation.py                  # ✅ Time window calculation
│   │   └── record_validation.py                  # ✅ Record validation + rebuild
│   │
│   └── utils/                                    # 🛠️ Shared utilities
│       ├── __init__.py
│       ├── log_generator.py                      # ✅ Structured logging
│       ├── log_retry_decorators.py               # ✅ Retry logic + timing
│       └── time_utils.py                         # ✅ Universal time handling
│
├── projects/                                     # 🏗️ Project-specific implementations
│   └── group_name/
│       └── class_name/
│           └── main_pipeline_dag.py              # 🎯 Main Airflow DAG
│
└── tests/                                        # 🧪 Test suite
    ├── __init__.py
    ├── quick_test.py                             # Quick validation tests
    ├── test_invalid_continuation.py              # Edge case testing
    ├── test_record_generation.py                 # ✅ Basic generation tests
    ├── test_record_generation_advanced.py        # ✅ Boundary condition tests
    ├── test_record_generation_updated.py         # Updated test scenarios
    ├── test_record_validation.py                 # ✅ Basic validation tests
    └── test_record_validation_rebuild.py         # ✅ Rebuild functionality tests
```

## Key Components Status

### ✅ **Complete & Ready**
- **Framework Core**: All utilities, business logic, and task implementations
- **Documentation**: Comprehensive guides for each component
- **Testing**: Extensive test coverage for core functionality

### 🔄 **Needs Implementation**
- **Database Integration**: Replace placeholder functions in all tasks
- **System Integrations**: Implement Elasticsearch, S3, Snowflake connectors
- **Main DAG**: Move from projects/ to production location
- **Configuration**: Implement `load_final_config()` function

### 📁 **Project Structure Benefits**
- **Clean Separation**: Framework vs implementation vs project-specific code
- **Scalability**: Easy to add new source/stage/target systems
- **Maintainability**: Clear organization with comprehensive documentation
- **Development Ready**: VSCode config, test suite, and environment setup