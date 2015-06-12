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

# Add the trace analysis scripts to the path.
# This assumes that this git hub repo: https://github.com/kayousterhout/trace-analysis.git
#   is located in the parent directory.
sys.path.append("../trace-analysis/")
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
    self.jobs = {}
    # For each stage, jobs that rely on the stage.
    self.jobs_for_stage = {}

    f = open(filename, "r")
    test_line = f.readline()
    try:
      get_json(test_line)
      self.is_json = True
      print "Parsing file %s as JSON" % filename
    except:
      self.is_json = False
      print "Parsing file %s as JobLogger output" % filename
    f.seek(0)

    for line in f:
      if self.is_json:
        try:
          json_data = get_json(line)
        except:
          print "BAD DATA: %s" % line
          continue
        event_type = json_data["Event"]
        if event_type == "SparkListenerJobStart":
          stage_ids = json_data["Stage IDs"]
          job_id = json_data["Job ID"]
          # Avoid using "Stage Infos" here, which was added in 1.2.0.
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
            self.jobs[job_id].add_event(json_data, True)
      else:
        # The file will only contain information for one job.
        self.jobs[0].add_event(line, False)

    print "Filtering jobs based on passed in filter function"
    self.jobs = job_filterer(self.jobs)
    print "Finished reading input data:"
    for job_id, job in self.jobs.iteritems():
      job.initialize_job()
      job_tasks= job.all_tasks()
      job_start_time = min([task.start_time for task in job_tasks])
      job_finish_time = max([task.finish_time for task in job_tasks])
      job_runtime = (job_finish_time - job_start_time) / 1000.0
      print "Job", job_id, " has stages: ", job.stages.keys(), " and runtime", job_runtime

  def output_all_waterfalls(self):
    for job_id, job in self.jobs.iteritems():
      filename = "%s_%s" % (self.filename, job_id)
      job.write_waterfall(filename)

  def write_summary_file(self, values, filename):
    summary_file = open(filename, "w")
    for percentile in [5, 25, 50, 75, 95]:
      summary_file.write("%f\t" % numpy.percentile(values, percentile))
    # Write max and min
    summary_file.write("%f\t%f\n" % (min(values), max(values)))
    summary_file.close()

  def output_all_job_info(self, agg_results_filename):
    network_speedups = []
    disk_speedups = []
    simulated_vs_actual = []
    straggler_speedups = []
    single_wave_straggler_speedups = []
    for job_id, job in sorted(self.jobs.iteritems()):
      filename = "%s_%s" % (self.filename, job_id)
      job.compute_speedups()
      self.__output_job_info(job, job_id, filename, agg_results_filename)
      network_speedups.append(job.no_network_speedup_tuple[0])
      disk_speedups.append(job.no_disk_speedup()[0])
      simulated_vs_actual.append(job.simulated_runtime_over_actual(filename))
      straggler_speedups.append(job.median_progress_rate_speedup(filename))
      single_wave_straggler_speedups.append(job.single_wave_straggler_speedup())

    if agg_results_filename != None:
      self.write_summary_file(network_speedups, "%s_network_summary" % agg_results_filename)
      self.write_summary_file(disk_speedups, "%s_disk_summary" % agg_results_filename)
      self.write_summary_file(simulated_vs_actual,
        "%s_simulation_vs_actual" % agg_results_filename)
      self.write_summary_file(straggler_speedups, "%s_straggler_summary" % agg_results_filename)
      self.write_summary_file(single_wave_straggler_speedups,
        "%s_straggler_single_wave_summary" % agg_results_filename)
      self.output_utilizations(agg_results_filename)

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
    f = open(filename, "w")
    f.write("\t".join([str(x) for x in output]))
    f.write("\n")
    f.close()

  def write_box_whiskers_info(self, filename, data):
    percentiles = [5, 25, 50, 75, 95]
    f = open(filename, "w")
    # Write the filename and 0 as a placeholder, to make it easier to generate a box/whiskers plot.
    f.write("%s\t0\t" % filename)
    for percentile in percentiles:
      f.write("%f\t" % numpy.percentile(data, percentile))
    f.write("\n")
    f.close()

  def output_load_balancing_badness(self, prefix):
    print "Outputting information about load balancing"
    load_balancing = []
    for job_id, job in self.jobs.iteritems():
      for stage_id, stage in job.stages.iteritems():
        load_balancing.append(stage.load_balancing_badness())
    print load_balancing

    self.write_box_whiskers_info("%s_load_balancing_badness" % prefix, load_balancing)

  def output_runtimes(self, prefix):
    runtimes = [job.original_runtime() for (job_id, job) in self.jobs.iteritems()]
    self.write_box_whiskers_info("%s_runtimes" % prefix, runtimes)

  def output_utilizations(self, prefix):
    print "Outputting utilizations"
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
      print "Adding data for job %s" % job_id
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

  def output_straggler_info(self, agg_results_filename):
    total_tasks = 0
    total_stragglers = 0
    total_explained_stragglers = 0
    for job_id, job in self.jobs.iteritems():
      total_tasks += sum([len(s.tasks) for s in job.stages.values()])
      all_stragglers = []
      for s in job.stages.values():
        all_stragglers.extend(s.get_progress_rate_stragglers())
      total_stragglers += len(all_stragglers)
      total_explained_stragglers += len([t for t in all_stragglers if t.straggler_behavior_explained])

    f = open("%s_stragglers" % agg_results_filename, "w")
    f.write("%s\t%s\t%s\n" % (total_tasks, total_stragglers, total_explained_stragglers))

  def __output_job_info(self, job, job_id, filename, agg_results_filename):
    job.print_stage_info()

    job.write_task_write_times_scatter(filename)

    #job.make_cdfs_for_performance_model(filename)

    job.write_waterfall(filename)

    fraction_time_scheduler_delay = job.fraction_time_scheduler_delay()
    print ("\nFraction time scheduler delay: %s" % fraction_time_scheduler_delay)
    fraction_time_waiting_on_shuffle_read = job.fraction_time_waiting_on_shuffle_read()
    print "\nFraction time waiting on shuffle read: %s" % fraction_time_waiting_on_shuffle_read
    no_input_disk_speedup = job.no_input_disk_speedup()[0]
    print "Speedup from eliminating disk for input: %s" % no_input_disk_speedup
    no_output_disk_speedup = job.no_output_disk_speedup()[0]
    print "Speedup from elimnating disk for output: %s" % no_output_disk_speedup
    no_shuffle_write_disk_speedup = job.no_shuffle_write_disk_speedup()[0]
    print "Speedup from eliminating disk for shuffle write: %s" % no_shuffle_write_disk_speedup
    no_shuffle_read_disk_speedup, throw_away, no_shuffle_read_runtime = \
      job.no_shuffle_read_disk_speedup()
    print "Speedup from eliminating shuffle read: %s" % no_shuffle_read_disk_speedup
    no_disk_speedup, simulated_original_runtime, no_disk_runtime = job.no_disk_speedup()
    print "No disk speedup: %s" % no_disk_speedup
    fraction_time_using_disk = job.fraction_time_using_disk()
    no_network_speedup, not_used, no_network_runtime = job.no_network_speedup_tuple
    print "No network speedup: %s" % no_network_speedup
    print("\nFraction of time spent writing/reading shuffle data to/from disk: %s" %
      fraction_time_using_disk)
    print("\nFraction of time spent garbage collecting: %s" %
      job.fraction_time_gc())
    no_compute_speedup = job.no_compute_speedup()[0]
    print "\nSpeedup from eliminating compute: %s" % no_compute_speedup
    fraction_time_waiting_on_compute = job.fraction_time_waiting_on_compute()
    print "\nFraction of time waiting on compute: %s" % fraction_time_waiting_on_compute
    fraction_time_computing = job.fraction_time_computing()
    print "\nFraction of time computing: %s" % fraction_time_computing
    
    replace_all_tasks_with_average_speedup = job.replace_all_tasks_with_average_speedup(filename)
    no_stragglers_replace_with_median_speedup = job.replace_all_tasks_with_median_speedup()
    no_stragglers_replace_95_with_median_speedup = \
      job.replace_stragglers_with_median_speedup(lambda runtimes: numpy.percentile(runtimes, 95))
    no_stragglers_replace_ganesh_with_median_speedup = \
      job.replace_stragglers_with_median_speedup(
        lambda runtimes: 1.5 * numpy.percentile(runtimes, 50))
    no_stragglers_perfect_parallelism = \
      job.no_stragglers_perfect_parallelism_speedup()
    median_progress_rate_speedup = job.median_progress_rate_speedup(filename)
    print (("\nSpeedup from eliminating stragglers: %s (perfect parallelism) %s (use average) "
      "%s (use median) %s (1.5=>median) %s (95%%ile=>med) %s (median progress rate)") %
      (no_stragglers_perfect_parallelism, replace_all_tasks_with_average_speedup,
       no_stragglers_replace_with_median_speedup, no_stragglers_replace_ganesh_with_median_speedup,
       no_stragglers_replace_95_with_median_speedup, median_progress_rate_speedup))

    simulated_versus_actual = job.simulated_runtime_over_actual(filename)
    print "\n Simulated versus actual runtime: ", simulated_versus_actual

    if agg_results_filename != None:
      print "Adding results to %s" % agg_results_filename
      f = open(agg_results_filename, "a")

      if self.is_json:
        name = job_id
      else:
        name = filename.split("/")[1].split("_")[0]
      data = [
        name,
        # 1
        fraction_time_waiting_on_shuffle_read,
        # 2 3
        no_disk_speedup, fraction_time_using_disk,
        no_compute_speedup, -1, fraction_time_computing,
        # 7 8
        replace_all_tasks_with_average_speedup, no_stragglers_replace_with_median_speedup,
        # 9 10
        no_stragglers_replace_95_with_median_speedup, no_stragglers_perfect_parallelism,
        # 11 12
        simulated_versus_actual, median_progress_rate_speedup,
        no_input_disk_speedup, no_output_disk_speedup,
        # 15 16
        no_shuffle_write_disk_speedup, no_shuffle_read_disk_speedup,
        # 17 18 19
        job.original_runtime(), simulated_original_runtime, no_disk_runtime, 
        no_shuffle_read_runtime,
        # 21 22 23
        job.no_gc_speedup()[0], no_network_speedup, no_network_runtime]
      job.write_data_to_file(data, f)
      f.close()
      job.write_straggler_info(filename, agg_results_filename)
      job.write_stage_info(filename, agg_results_filename)

      job.write_hdfs_stage_normalized_runtimes(agg_results_filename)

def main(argv):
  parser = OptionParser(usage="parse_logs.py [options] <log filename>")
  parser.add_option(
      "-d", "--debug", action="store_true", default=False,
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

  analyzer = Analyzer(filename, shuffle_job_filterer.filter)

  analyzer.output_utilizations(filename)
  analyzer.output_load_balancing_badness(filename)
  analyzer.output_runtimes(filename)
  analyzer.output_all_waterfalls()

if __name__ == "__main__":
  main(sys.argv[1:])
