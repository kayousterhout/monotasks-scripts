"""
This file allows a user to copy the most recent continuous monitor from
a remote executor.
"""

import sys

import utils

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print "Usage: python copy_continuous_monitor.py hostname identity_file output_prefix"
    sys.exit(1)
  utils.copy_latest_continuous_monitor(sys.argv[1], sys.argv[2], sys.argv[3], "root")
