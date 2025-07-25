"""
Pytest configuration file that sets up import paths for all tests
"""

import sys
from pathlib import Path

# Add project root to Python path so all tests can import src modules
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root)) 