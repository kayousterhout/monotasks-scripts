from optparse import OptionParser
import subprocess
import sys

import parse_event_logs
import plot_continuous_monitor

# Copy a file from a given host through scp, throwing an exception if scp fails.
def scp_from(host, identity_file, username, remote_file, local_file):
  subprocess.check_call(
    "scp -q -o StrictHostKeyChecking=no -i %s '%s@%s:%s' '%s'" %
    (identity_file, username, host, remote_file, local_file), shell=True)

def ssh_get_stdout(host, identity_file, username, command):
  command = "source /root/.bash_profile; %s" % command
  ssh_command = ("ssh -t -o StrictHostKeyChecking=no -i %s %s@%s '%s'" %
    (identity_file, username, host, command))
  return subprocess.check_output(ssh_command, shell=True)

def main(argv):
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
  ret = ssh_get_stdout(
    opts.driver_host,
    opts.identity_file,
    opts.username,
    "ls -t /tmp/spark-events | head -n 1")
  print ret
  event_log_dir = ret.strip("\n").strip("\r")
  event_log_filename = "/tmp/spark-events/%s/EVENT_LOG_1" % event_log_dir
  local_event_log_file = "%s_event_log" % opts.filename_prefix
  print ("Copying event log from file %s on host %s back to %s" %
    (event_log_filename, opts.driver_host, local_event_log_file))
  scp_from(
    opts.driver_host,
    opts.identity_file,
    opts.username,
    event_log_filename,
    local_event_log_file)
  
  # Copy the continuous monitor from the driver back to the local machine.
  continuous_monitor_dir = ssh_get_stdout(
    opts.executor_host,
    opts.identity_file,
    opts.username,
    "ls -t /tmp/ | grep continuous_monitor | head -n 1").strip("\n").strip("\r")
  continuous_monitor_filename = "/tmp/%s/1" % continuous_monitor_dir
  local_continuous_monitor_file = "%s_executor_monitor" % opts.filename_prefix
  print ("Copying continuous monitor from file %s on host %s back to %s" %
    (continuous_monitor_filename, opts.executor_host, local_continuous_monitor_file))
  scp_from(
    opts.executor_host,
    opts.identity_file,
    opts.username,
    continuous_monitor_filename,
    local_continuous_monitor_file)

  print "Plotting continuous monitor"
  plot_continuous_monitor.plot_continuous_monitor(local_continuous_monitor_file)

  print "Parsing event logs"
  analyzer = parse_event_logs.Analyzer(local_event_log_file)
  analyzer.output_utilizations(local_event_log_file)


if __name__ == "__main__":
  main(sys.argv[1:])
