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
  network_throughput_gbps = 0.6

  if len(argv) >= 5:
    driver_hostname = argv[2]
    if "millennium" in driver_hostname:
      # The millennium machines have 16 cores.
      # TODO: Temporarily, only running 8, to understand CPU effects of opportunistic networking.
      num_cores = 8
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
  all_dirnames.sort(key = lambda d: -int(d.split("_")[4]))

  map_data_file = open(os.path.join(output_prefix, "map_times"), "w")
  reduce_data_file = open(os.path.join(output_prefix, "reduce_times"), "w")
  total_data_file = open(os.path.join(output_prefix, "total_times"), "w")
  for dirname in all_dirnames:
    utils.plot_continuous_monitors(os.path.join(output_prefix, dirname))

    local_event_log_filename = os.path.join(output_prefix, dirname, "event_log")
    print "Parsing event log in %s" % local_event_log_filename
    analyzer = parse_event_logs.Analyzer(local_event_log_filename, job_filterer = filter)
    analyzer.output_job_resource_metrics(local_event_log_filename)
    analyzer.output_stage_resource_metrics(local_event_log_filename)
    analyzer.output_ideal_time_metrics(local_event_log_filename)
    analyzer.output_ideal_time_metrics(local_event_log_filename, fix_executors = True)
    analyzer.output_compute_monotask_time_cdfs(local_event_log_filename)

    total_runtimes = []
    total_ideal_runtimes = []
    total_ideal_runtimes_fix_executors = []
    total_ideal_runtimes_fluid_resources = []

    map_runtimes = []
    map_ideal_runtimes_fix_executors = []
    map_ideal_runtimes_fluid_resources = []

    reduce_runtimes = []
    reduce_ideal_runtimes_fix_executors = []
    reduce_ideal_runtimes_fluid_resources = []
    reduce_cpu_ideal_times = []

    for (job_id, job) in analyzer.jobs.iteritems():
      print "******** Job %s ********" % job_id
      job_millis = 0
      job_ideal_millis = 0
      job_ideal_millis_fix_executors = 0

      # Counters for the job-wide CPU, network, and disk time.
      # These are used to compute an ideal that assumes that different
      # resources can be fluidly moved to different stages in the job (which
      # may not always be possible).
      job_cpu_millis = 0
      job_network_millis = 0
      job_disk_millis = 0

      for (stage_id, stage) in job.stages.iteritems():
        print "    *** Stage %s ***" % stage_id
        stage_ideal_times = stage.get_ideal_times_from_metrics(
          network_throughput_gbps,
          num_cores_per_executor = num_cores)
        print "    Ideal times (CPU, net, disk):", stage_ideal_times
        stage_ideal_millis = 1000 * max(stage_ideal_times)
        stage_runtime = stage.runtime()

        stage_ideal_times_fix_executors = stage.get_ideal_times_from_metrics_fix_executors(
          network_throughput_gigabits_per_executor = network_throughput_gbps,
          num_cores_per_executor = num_cores)
        print "    Ideal times w/ fixed execs (CPU, net, disk):", stage_ideal_times_fix_executors
        stage_ideal_millis_fix_executors = 1000 * max(stage_ideal_times_fix_executors)

        job_millis += stage_runtime
        job_ideal_millis += stage_ideal_millis
        job_ideal_millis_fix_executors += stage_ideal_millis_fix_executors

        job_cpu_millis += stage_ideal_times[0]
        job_network_millis += stage_ideal_times[1]
        job_disk_millis += stage_ideal_times[2]

        if stage.has_shuffle_read():
          reduce_runtimes.append(stage_runtime)
          reduce_ideal_runtimes_fix_executors.append(stage_ideal_millis_fix_executors)
          reduce_ideal_runtimes_fluid_resources.append(stage_ideal_millis)
          reduce_cpu_ideal_times.append(1000 * stage_ideal_times[0])
        else:
          map_runtimes.append(stage_runtime)
          map_ideal_runtimes_fix_executors.append(stage_ideal_millis_fix_executors)
          map_ideal_runtimes_fluid_resources.append(stage_ideal_millis)
      total_runtimes.append(job_millis)
      total_ideal_runtimes.append(job_ideal_millis)
      total_ideal_runtimes_fix_executors.append(job_ideal_millis_fix_executors)
      print "  SUMMARY: Job CPU: {}, network: {}, disk: {}".format(
        job_cpu_millis, job_network_millis, job_disk_millis)
      total_ideal_runtimes_fluid_resources.append(
        1000 * max(job_cpu_millis, job_network_millis, job_disk_millis))

    output_filename = local_event_log_filename + "_job_runtimes"
    with open(output_filename, "w") as output_file:
      output_file.write("Name Actual (min, med, max) Ideal (min, med, max)\n")
      map_data = "{} {}".format(
          utils.get_min_med_max_string(map_runtimes),
          utils.get_min_med_max_string(map_ideal_runtimes_fluid_resources))
      reduce_data = "{} {}".format(
          utils.get_min_med_max_string(reduce_runtimes),
          utils.get_min_med_max_string(reduce_ideal_runtimes_fluid_resources))
      total_data = "{} {}".format(
          utils.get_min_med_max_string(total_runtimes),
          utils.get_min_med_max_string(total_ideal_runtimes))

      output_file.write("Map {}\n".format(map_data))
      output_file.write("Reduce {}\n".format(reduce_data))
      output_file.write("Total {}\n".format(total_data))

      job_params = dirname.split("_")
      num_shuffle_values = int(job_params[5])
      num_tasks = int(job_params[2])
      map_data_file.write("{} {} {} {}\n".format(
        num_shuffle_values,
        num_tasks,
        map_data,
        utils.get_min_med_max_string(map_ideal_runtimes_fix_executors)))
      reduce_data_file.write("{} {} {} {} {}\n".format(
        num_shuffle_values,
        num_tasks,
        reduce_data,
        utils.get_min_med_max_string(reduce_ideal_runtimes_fix_executors),
        utils.get_min_med_max_string(reduce_cpu_ideal_times)))
      total_data_file.write("{} {} {} {} {}\n".format(
        num_shuffle_values,
        num_tasks,
        total_data,
        utils.get_min_med_max_string(total_ideal_runtimes_fix_executors),
        utils.get_min_med_max_string(total_ideal_runtimes_fluid_resources)))

    # Generate a gnuplot file to plot the total times and ideal times.
    plot_basename = os.path.join(output_prefix, "total_times")
    plot_file = utils.create_gnuplot_file_from_base(
      "gnuplot_files/plot_totals_and_ideals_base.gp",
      plot_basename + ".gp",
      {"__OUTPUT_FILEPATH__": plot_basename + ".pdf", "__TOTAL_TIMES_FILEPATH__": plot_basename}) 
    plot_file.close()

if __name__ == "__main__":
  main(sys.argv)
