"""
This file contains helper functions used by many of the experiment scripts.
"""

import numpy
from optparse import OptionParser
import os
from os import path
import subprocess
import sys

import plot_continuous_monitor

def create_gnuplot_file_from_base(base_filename, new_filename, keyword_to_value):
  """ Creates a new gnuplot file based on the given base file.

  For each entry in the keyword_to_value dictionary, replaces incidences of the key
  in the base gnuplot file with the value in keyword_to_value.
  """
  new_file = open(new_filename, "w")
  with open(base_filename, "r") as base_file:
    for line in base_file:
      for key, value in keyword_to_value.iteritems():
        line = line.replace(key, value)
      new_file.write(line)
  return new_file

# Copy a file from a given host through scp, throwing an exception if scp fails.
def scp_from(host, identity_file, username, remote_file, local_file):
  subprocess.check_call("scp -q -o StrictHostKeyChecking=no -i {} '{}@{}:{}' '{}'".format(
    identity_file, username, host, remote_file, local_file), shell=True)

def ssh_get_stdout(host, identity_file, username, command):
  if "ec2" in host:
    command = "source /root/.bash_profile; {}".format(command)
  ssh_command = "ssh -t -o StrictHostKeyChecking=no -i {} {}@{} '{}'".format(
    identity_file, username, host, command)
  return subprocess.Popen(ssh_command, stdout=subprocess.PIPE, shell=True).communicate()[0]

def copy_latest_zipped_logs(driver_hostname, identity_file, output_prefix, num_experiments,
                            username):
  list_filenames_command = "ls -t /mnt/experiment_log_*gz | head -n " + num_experiments
  event_log_filenames = ssh_get_stdout(
    driver_hostname,
    identity_file,
    username,
    list_filenames_command).strip("\n").strip("\r")
  print "Copying data from directories: " + event_log_filenames

  for event_log_filename in event_log_filenames.split("\n"):
    event_log_filename = event_log_filename.strip("\r")
    basename = os.path.basename(event_log_filename)
    local_zipped_logs_name = os.path.join(output_prefix, basename)
    print ("Copying event log from file %s back to %s" %
      (event_log_filename, local_zipped_logs_name))
    scp_from(
      driver_hostname,
      identity_file,
      username,
      event_log_filename,
      local_zipped_logs_name)

    # Unzip the file.
    subprocess.check_call(
      "tar -xvzf %s -C %s" % (local_zipped_logs_name, output_prefix),
      shell=True)

  # If necessary, move the file out of the /mnt directory.
  mnt_directory = path.join(output_prefix, "mnt")
  if path.exists(mnt_directory):
    command = "mv {}/* {}/".format(mnt_directory, output_prefix)
    print command
    subprocess.check_call(command, shell=True)
    subprocess.check_call("rmdir {}".format(mnt_directory), shell=True)

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
  continuous_monitor_filename = "/tmp/{}".format(continuous_monitor_relative_filename)
  local_continuous_monitor_file = "{}_executor_monitor".format(filename_prefix)
  print "Copying continuous monitor from file {} on host {} back to {}".format(
    continuous_monitor_filename, hostname, local_continuous_monitor_file)
  scp_from(
    hostname,
    identity_file,
    username,
    continuous_monitor_filename,
    local_continuous_monitor_file)

  print "Plotting continuous monitor"
  plot_continuous_monitor.plot_continuous_monitor(local_continuous_monitor_file, open_graphs=True)
  return local_continuous_monitor_file

def plot_continuous_monitors(log_dir):
  """ Plots all of the continuous monitors in the provided directory. """
  for log_filename in os.listdir(log_dir):
    if log_filename.endswith("executor_monitor"):
      plot_continuous_monitor.plot_continuous_monitor(
        path.join(log_dir, log_filename), use_gnuplot=True)

def find_index_of_shuffles(jobs):
  """ Returns the ids in jobs of jobs that do a shuffle.

  Arguments:
    jobs: a list of (job_id, job) pairs.
  """
  shuffle_job_ids = []
  for job_id, job in jobs:
    for stage_id, stage in job.stages.iteritems():
      if stage.has_shuffle_read():
        shuffle_job_ids.append(job_id)
        break
  return shuffle_job_ids

def get_min_med_max_string(runtimes_list):
  """ Returns a string with the minimum, median, and max of the given runtimes.

  The output divides all runtimes by 1000 (so if the input was milliseconds, the
  value in the returned string will be in seconds). This function is useful for
  writing data to plot.
  """
  return "{} {} {}".format(
      min(runtimes_list) / 1000.,
      numpy.percentile(runtimes_list, 50) / 1000.,
      max(runtimes_list) / 1000.)

def bytes_to_string(size):
  """Converts a quantity in bytes to a human-readable string such as "4.0 MB".

  Copied from org.apache.spark.util.Utils.bytesToString(size)
  """
  TB = 1L << 40
  GB = 1L << 30
  MB = 1L << 20
  KB = 1L << 10

  absolute_size = abs(size)
  size = float(size)
  if absolute_size >= 2 * TB:
    value = size / TB
    units = "TB"
  elif absolute_size >= 2 * GB:
    value = size / GB
    units = "GB"
  elif absolute_size >= 2 * MB:
    value = size / MB
    units = "MB"
  elif absolute_size >= 2 * KB:
    value = size / KB
    units = "KB"
  else:
    value = size
    units = "B"
  return "{:.2f} {}".format(value, units)

def bits_to_string(size):
  """Converts a quantity in bits to a human-readable string such as "4.0 Mb".

  Copied from org.apache.spark.util.Utils.bytesToString(size)
  """
  Tb = 1L << 40
  Gb = 1L << 30
  Mb = 1L << 20
  Kb = 1L << 10

  absolute_size = abs(size)
  size = float(size)
  if absolute_size >= 2 * Tb:
    value = size / Tb
    units = "Tb"
  elif absolute_size >= 2 * Gb:
    value = size / Gb
    units = "Gb"
  elif absolute_size >= 2 * Mb:
    value = size / Mb
    units = "Mb"
  elif absolute_size >= 2 * Kb:
    value = size / Kb
    units = "Kb"
  else:
    value = size
    units = "b"
  return "{:.2f} {}".format(value, units)
