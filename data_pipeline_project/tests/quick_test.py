# data_pipeline_project/tests/quick_test.py

import sys
import os
# Add parent directory to path to access pipeline_framework
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_record_generation import run_all_tests

if __name__ == "__main__":
    run_all_tests()



