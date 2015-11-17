"""
This file copies bacsk the most recent 5 job log bundles from the given
master and generates a graph plotting the job completion time as a function
of the number of tasks used.
"""

import numpy
import os
import subprocess
import sys

import parse_event_logs
import utils

def filter(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
   # Take all of the jobs except the first two (the first one is a warmup job,
   # and the second job was responsible for generating the input RDD and caching
   # it in-memory.
  filtered_jobs = sorted_jobs[2:]
  return {k:v for (k,v) in filtered_jobs}

def main(argv):
  if len(argv) < 4:
    print "Usage: parse_vary_num_tasks.py driver_hostname identity_file output_directory"
    sys.exit(1)

  driver_hostname = argv[1]
  identity_file = argv[2]
  output_prefix = argv[3]
  username = "root"

  if (not os.path.exists(output_prefix)):
    os.mkdir(output_prefix)

  event_log_filenames = utils.ssh_get_stdout(
    driver_hostname,
    identity_file,
    username,
    "ls -t /mnt/experiment_log_*gz | head -n 5").strip("\n").strip("\r")
  print event_log_filenames

  output_filename = os.path.join(output_prefix, "actual_runtimes")
  output_file = open(output_filename, "w")

  all_ideal_runtimes = []

  for event_log_filename in event_log_filenames.split("\n"):
    event_log_filename = event_log_filename.strip("\r")
    basename = os.path.basename(event_log_filename)
    local_zipped_logs_name = os.path.join(output_prefix, basename)
    print ("Copying event log from file %s back to %s" %
      (event_log_filename, local_zipped_logs_name))
    utils.scp_from(
      driver_hostname,
      identity_file,
      username,
      event_log_filename,
      local_zipped_logs_name)

    # Unzip the file.
    subprocess.check_call(
      "tar -xvzf %s -C %s" % (local_zipped_logs_name, output_prefix),
      shell=True)

    local_event_log_filename = os.path.join(local_zipped_logs_name[:-7], "event_log")
    print "Parsing event log in %s" % local_event_log_filename
    analyzer = parse_event_logs.Analyzer(local_event_log_filename, job_filterer = filter)

    all_jobs = analyzer.jobs.values()
    num_tasks_values = [len(stage.tasks) for job in all_jobs
      for (stage_id, stage) in job.stages.iteritems()]
    # Assumes all of the map and reduce staages use the same number of tasks.
    num_tasks = num_tasks_values[0]

    # Compute the ideal runtime.
    ideal_runtimes_millis = [sum([stage.ideal_time(15, 8, 1)
      for (stage_id, stage) in job.stages.iteritems()]) for job in all_jobs]
    print "Ideal runtimes:", ideal_runtimes_millis
    all_ideal_runtimes.extend(ideal_runtimes_millis)

    # Output the actual runtimes.
    actual_runtimes_millis = [job.runtime() for job in all_jobs]
    print "Actual runtimes:", actual_runtimes_millis
    output_file.write("%s\t%s\t%s\t%s\n" %
      (num_tasks,
       min(actual_runtimes_millis),
       numpy.percentile(actual_runtimes_millis, 50),
       max(actual_runtimes_millis)))
  output_file.close()

  ideal_runtime = numpy.mean(all_ideal_runtimes)
  plot_filename = os.path.join(output_prefix, "actual_runtimes.gp")
  plot_file = open(plot_filename, "w")
  for line in open("plot_vary_num_tasks_base.gp", "r"):
    new_line = line.replace("__NAME__", output_filename).replace(
      "__IDEAL_RUNTIME__", str(ideal_runtime))
    plot_file.write(new_line)
  plot_file.close()

  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  subprocess.check_call("open %s.pdf" % output_filename, shell=True)

if __name__ == "__main__":
  main(sys.argv)
