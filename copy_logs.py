"""
This file contains helper functions to copy logs from a remote Spark cluster.
The main function expected to be used is copy_logs.
"""

from optparse import OptionParser
import subprocess
import sys
import utils

import plot_continuous_monitor

def copy_logs(argv):
  """ Copies logs back from a Spark cluster.

  This script copies the JSON event log and JSON continuous monitor back from a Spark
  driver and Spark executor, respectively, to the local machine.  Returns a two-item
  tuple with the name of the event log and the continuous monitor log.
  """
  parser = OptionParser()
  parser.add_option(
    "-e", "--executor-host", help="Executor from which to copy continuous monitor")
  parser.add_option(
    "-d", "--driver-host", help="Hostname of driver")
  parser.add_option(
    "-f", "--filename-prefix", help="Filename prefix to use for files copied back")
  parser.add_option(
    "-u", "--username", default="root", help="Username to user when logging in")
  parser.add_option(
    "-i", "--identity-file", help="Identity file to use when logging in")
  (opts, args) = parser.parse_args()

  if not opts.executor_host:
    parser.error("--executor-host must be specified")
  if not opts.driver_host:
    parser.error("--driver-host must be specified")
  if not opts.identity_file:
    parser.error("--identity-file must be specified")
  if not opts.filename_prefix:
    parser.error("--filename-prefix must be specified")

  # Copy the event log from the driver back to the local machine.
  ret = utils.ssh_get_stdout(
    opts.driver_host,
    opts.identity_file,
    opts.username,
    "ls -t /tmp/spark-events | head -n 1")
  print ret
  event_log_relative_filename = ret.strip("\n").strip("\r")
  event_log_filename = "/tmp/spark-events/{}".format(event_log_relative_filename)
  local_event_log_file = "{}_event_log".format(opts.filename_prefix)
  print "Copying event log from file {} on host {} back to {}".format(
    event_log_filename, opts.driver_host, local_event_log_file)
  utils.scp_from(
    opts.driver_host,
    opts.identity_file,
    opts.username,
    event_log_filename,
    local_event_log_file)

  # Copy the continuous monitor from the driver back to the local machine.
  local_continuous_monitor_file = utils.copy_latest_continuous_monitor(
    opts.executor_host,
    opts.identity_file,
    opts.filename_prefix,
    opts.username)

  return (local_event_log_file, local_continuous_monitor_file)

if __name__ == "__main__":
  copy_logs(sys.argv[1:])
