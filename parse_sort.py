"""
This file extracts information about the runtimes of each stage of
sort jobs (and about the total runtime) to facilitate plotting
results.
"""

import os
import sys

import parse_event_logs
import utils


def filter(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
  # Find the second job that has a shuffle, so we skip the first sort job (which we
  # consider warmup).
  second_shuffle_index = utils.find_index_of_shuffles(sorted_jobs)[1]
  print "Second job with a shuffle is at index {} and has {} stages".format(
    second_shuffle_index, len(sorted_jobs[second_shuffle_index][1].stages))
  # Use every other job, since there's a garbage collection job in between each
  # sort job.
  filtered_jobs = sorted_jobs[second_shuffle_index::2]
  return {k:v for (k,v) in filtered_jobs}

def main(argv):
  if len(argv) < 2:
    print ("Usage: parse_sort.py output_directory [opt (to copy data): driver_hostname " +
        "identity_file num_experiments [opt username]]")
    sys.exit(1)

  output_prefix = argv[1]
  if (not os.path.exists(output_prefix)):
    os.mkdir(output_prefix)

  num_cores = 8

  if len(argv) >= 5:
    driver_hostname = argv[2]
    if "millennium" in driver_hostname:
      # The millennium machines have 16 cores.
      num_cores = 16
    identity_file = argv[3]
    num_experiments = argv[4]
    if len(argv) >= 6:
      username = argv[5]
    else:
      username = "root"
    utils.copy_latest_zipped_logs(driver_hostname, identity_file, output_prefix, num_experiments,
                                  username)

  # Find all of the directories with experiment data.
  all_dirnames = [d for d in os.listdir(output_prefix)
    if os.path.isdir(os.path.join(output_prefix, d))]
  all_dirnames.sort()

  for dirname in all_dirnames:
    utils.plot_continuous_monitors(os.path.join(output_prefix, dirname))

    local_event_log_filename = os.path.join(output_prefix, dirname, "event_log")
    print "Parsing event log in %s" % local_event_log_filename
    analyzer = parse_event_logs.Analyzer(local_event_log_filename, job_filterer = filter)
    analyzer.output_job_resource_metrics(local_event_log_filename)
    analyzer.output_stage_resource_metrics(local_event_log_filename)
    analyzer.output_ideal_time_metrics(local_event_log_filename)
    analyzer.output_compute_monotask_time_cdfs(local_event_log_filename)

    total_runtimes = []
    total_ideal_runtimes = []

    map_runtimes = []
    map_ideal_runtimes = []

    reduce_runtimes = []
    reduce_ideal_runtimes = []

    for (job_id, job) in analyzer.jobs.iteritems():
      job_millis = 0
      job_ideal_millis = 0
      for (stage_id, stage) in job.stages.iteritems():
        stage_ideal_millis = (
            1000 * stage.ideal_time_s(num_cores_per_executor = num_cores))
        stage_runtime = stage.runtime()
        job_millis += stage_runtime
        job_ideal_millis += stage_ideal_millis
        if stage.has_shuffle_read():
          reduce_runtimes.append(stage_runtime)
          reduce_ideal_runtimes.append(stage_ideal_millis)
        else:
          map_runtimes.append(stage_runtime)
          map_ideal_runtimes.append(stage_ideal_millis)
      total_runtimes.append(job_millis)
      total_ideal_runtimes.append(job_ideal_millis)

    output_filename = local_event_log_filename + "_job_runtimes"
    with open(output_filename, "w") as output_file:
      output_file.write("Name Actual (min, med, max) Ideal (min, med, max)\n")
      output_file.write("Map {} {}\n".format(
          utils.get_min_med_max_string(map_runtimes),
          utils.get_min_med_max_string(map_ideal_runtimes)))
      output_file.write("Reduce {} {}\n".format(
          utils.get_min_med_max_string(reduce_runtimes),
          utils.get_min_med_max_string(reduce_ideal_runtimes)))
      output_file.write("Total {} {}\n".format(
          utils.get_min_med_max_string(total_runtimes),
          utils.get_min_med_max_string(total_ideal_runtimes)))

if __name__ == "__main__":
  main(sys.argv)
