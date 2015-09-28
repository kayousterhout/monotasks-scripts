"""
This file contains utilities to parse the JSON event log output by Spark.
"""

import collections
import json
import logging
import numpy
from optparse import OptionParser
import shuffle_job_filterer
import sys

from job import Job

def get_json(line): 
  # Need to first strip the trailing newline, and then escape newlines (which can appear
  # in the middle of some of the JSON) so that JSON library doesn't barf.
  return json.loads(line.strip("\n").replace("\n", "\\n"))

class Analyzer:
  def __init__(self, filename, job_filterer = lambda x: x):
    """ The job_filterer function here accepts a dictionary mapping job ids to jobs, and returns
    a new dictionary mapping job_ids to jobs. It can be used to filter out particular jobs from
    the set of jobs that are analyzed. """
    self.filename = filename
    self.logger = logging.getLogger("Analyzer")
    self.jobs = {}
    # For each stage, jobs that rely on the stage.
    self.jobs_for_stage = {}

    f = open(filename, "r")
    for line in f:
      try:
        json_data = get_json(line)
      except:
        logger.error("BAD DATA: %s" % line)
        continue
      event_type = json_data["Event"]
      if event_type == "SparkListenerJobStart":
        stage_ids = json_data["Stage IDs"]
        job_id = json_data["Job ID"]
        for stage_id in stage_ids:
          if stage_id not in self.jobs_for_stage:
            self.jobs_for_stage[stage_id] = []
          self.jobs_for_stage[stage_id].append(job_id)
      elif event_type == "SparkListenerTaskEnd":
        stage_id = json_data["Stage ID"]
        # Add the event to all of the jobs that depend on the stage.
        for job_id in self.jobs_for_stage[stage_id]:
          if job_id not in self.jobs:
            self.jobs[job_id] = Job(job_id)
          self.jobs[job_id].add_event(json_data)

    self.logger.debug("Filtering jobs based on passed in filter function")
    self.jobs = job_filterer(self.jobs)
    self.logger.debug("Finished reading input data:")
    for job_id, job in self.jobs.iteritems():
      job.initialize_job()
      job_tasks= job.all_tasks()
      job_start_time = min([task.start_time for task in job_tasks])
      job_finish_time = max([task.finish_time for task in job_tasks])
      job_runtime = (job_finish_time - job_start_time) / 1000.0
      stage_str = ["%s (%sm)" % (stage_id, stage.runtime() / 60000.0)
        for (stage_id, stage) in job.stages.iteritems()]
      self.logger.debug("Job %s has stages: %s and runtime %sm (%ss)" %
        (job_id, stage_str, job_runtime / 60., job_runtime))

  def write_summary_file(self, values, filename):
    summary_file = open(filename, "w")
    for percentile in [5, 25, 50, 75, 95]:
      summary_file.write("%f\t" % numpy.percentile(values, percentile))
    summary_file.write("%f\t%f\n" % (min(values), max(values)))
    summary_file.close()

  def __write_utilization_summary_file(self, utilization_pairs, filename):
    utilization_pairs.sort() 
    current_total_runtime = 0
    percentiles = [0.05, 0.25, 0.5, 0.75, 0.95, 0.99]
    output = []
    percentile_index = 0
    # Add this up here rather than passing it in, because for some types of utilization
    # (e.g., network, disk), each task appears multiple times.
    total_runtime = sum([x[1] for x in utilization_pairs])
    for (utilization, runtime) in utilization_pairs:
      current_total_runtime += runtime
      current_total_runtime_fraction = float(current_total_runtime) / total_runtime
      if current_total_runtime_fraction > percentiles[percentile_index]:
        output.append(utilization)
        percentile_index += 1
        if percentile_index >= len(percentiles):
          break

    utilizations = [x[0] for x in utilization_pairs]
    weights = [x[1] for x in utilization_pairs]
    weighted_average = numpy.average(utilizations, weights=weights)
    output.append(weighted_average)
    f = open(filename, "w")
    f.write("\t".join([str(x) for x in output]))
    f.write("\n")
    f.close()

  def output_load_balancing_badness(self, prefix):
    self.logger.debug("Outputting information about load balancing")
    load_balancing = []
    for job_id, job in self.jobs.iteritems():
      for stage_id, stage in job.stages.iteritems():
        load_balancing.append(stage.load_balancing_badness())

    self.write_summary_file(load_balancing, "%s_load_balancing_badness" % prefix)

  def output_runtimes(self, prefix):
    runtimes = [job.runtime() for (job_id, job) in self.jobs.iteritems()]
    self.write_summary_file(runtimes, "%s_runtimes" % prefix)

  def output_utilizations(self, prefix):
    # TODO: This function outputs the distribution of utilizations while tasks were running by
    # calculating a weighted average of the utilizations while tasks were running, using the
    # macrotask duration as the weight. This is just an estimate of the average on the machine;
    # instead, we should just directly compute the average utilization using the continuous monitor.
    self.logger.debug("Outputting utilizations")
    disk_utilizations = []
    disk_throughputs = []
    process_user_cpu_utilizations = []
    cpu_utilizations = []
    network_utilizations = []
    network_utilizations_recv_only = []
    network_utilizations_fetch_only = []
    task_runtimes = []
    # Divide by 8 to convert to bytes!
    NETWORK_BANDWIDTH_BPS = 1.0e9 / 8

    for job_id, job in self.jobs.iteritems():
      for stage_id, stage in job.stages.iteritems():
        stage_runtime = (max([t.finish_time for t in stage.tasks]) -
          min([t.start_time for t in stage.tasks]))
        for task in stage.tasks:
          task_runtimes.append(task.runtime())
          cpu_utilizations.append((task.total_cpu_utilization / 8., task.runtime()))
          process_user_cpu_utilizations.append((task.process_user_cpu_utilization / 8., task.runtime()))
          for name, block_device_numbers in task.disk_utilization.iteritems():
            if name in ["xvdb", "xvdf"]:
              utilization = block_device_numbers[0]
              disk_utilizations.append((utilization, task.runtime()))
              effective_disk_throughput = 0
              if utilization > 0:
                effective_disk_throughput = ((block_device_numbers[1] + block_device_numbers[2]) /
                  utilization)
              disk_throughputs.append((effective_disk_throughput, task.runtime()))
          received_utilization = (task.network_bytes_received_ps /
            NETWORK_BANDWIDTH_BPS, task.runtime())
          network_utilizations.append(received_utilization)
          transmitted_utilization = (task.network_bytes_transmitted_ps /
            NETWORK_BANDWIDTH_BPS, task.runtime())
          network_utilizations.append(transmitted_utilization)
          network_utilizations_recv_only.append(received_utilization)
          if task.has_fetch:
            network_utilizations_fetch_only.append(received_utilization)
            network_utilizations_fetch_only.append(transmitted_utilization)
    
    self.write_summary_file(task_runtimes, "%s_%s" % (prefix, "task_runtimes"))
    self.__write_utilization_summary_file(
      disk_utilizations, "%s_%s" % (prefix, "disk_utilization"))
    self.__write_utilization_summary_file(
      disk_throughputs, "%s_%s" % (prefix, "disk_throughput"))
    self.__write_utilization_summary_file(
      network_utilizations, "%s_%s" % (prefix, "network_utilization"))
    self.__write_utilization_summary_file(
      network_utilizations_recv_only, "%s_%s" % (prefix, "network_utilization_recv"))
    if network_utilizations_fetch_only:
      self.__write_utilization_summary_file(
        network_utilizations_fetch_only,
        "%s_%s" % (prefix, "network_utilization_fetch_only"))
    self.__write_utilization_summary_file(
      cpu_utilizations, "%s_%s" % (prefix, "cpu_utilization"))
    self.__write_utilization_summary_file(
      process_user_cpu_utilizations, "%s_%s" % (prefix, "cpu_process_user_utilization"))

def main(argv):
  parser = OptionParser(usage="parse_logs.py [options] <log filename>")
  parser.add_option(
      "-d", "--debug", action="store_true", default=True,
      help="Enable additional debug logging")
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)
 
  if opts.debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)
  filename = args[0]
  if filename is None:
    parser.print_help()
    sys.exit(1)

  analyzer = Analyzer(filename)

  analyzer.output_utilizations(filename)
  analyzer.output_load_balancing_badness(filename)
  analyzer.output_runtimes(filename)

if __name__ == "__main__":
  main(sys.argv[1:])
