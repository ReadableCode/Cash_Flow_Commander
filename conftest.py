import os
import sys

# Make src/ modules and the test helpers importable from a repo-root pytest run.
_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, os.path.join(_ROOT, "tests", "test_utils"))
