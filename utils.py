"""
This file contains helper functions used by many of the experiment scripts.
"""

from optparse import OptionParser
import subprocess
import sys

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
  return subprocess.Popen(ssh_command, stdout=subprocess.PIPE, shell=True).communicate()[0]

def copy_latest_continuous_monitor(hostname, identity_file, filename_prefix, username):
  """ Copies logs back from a Spark cluster.

  This script copies the JSON event log and JSON continuous monitor back from a Spark
  driver and Spark executor, respectively, to the local machine.  Returns a two-item
  tuple with the name of the event log and the continuous monitor log.
  """
  continuous_monitor_relative_filename = ssh_get_stdout(
    hostname,
    identity_file,
    username,
    "ls -t /tmp/ | grep continuous_monitor | head -n 1").strip("\n").strip("\r")
  continuous_monitor_filename = "/tmp/%s" % continuous_monitor_relative_filename
  local_continuous_monitor_file = "%s_executor_monitor" % filename_prefix
  print ("Copying continuous monitor from file %s on host %s back to %s" %
    (continuous_monitor_filename, hostname, local_continuous_monitor_file))
  scp_from(
    hostname,
    identity_file,
    username,
    continuous_monitor_filename,
    local_continuous_monitor_file)

  print "Plotting continuous monitor"
  plot_continuous_monitor.plot_continuous_monitor(local_continuous_monitor_file, open_graphs=True)
  return local_continuous_monitor_file

