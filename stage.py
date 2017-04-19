import logging

import metrics
from task import Task


class Stage:
  def __init__(self):
    self.start_time = -1
    self.tasks = []

  def average_task_runtime(self):
    return sum([t.runtime() for t in self.tasks]) * 1.0 / len(self.tasks)

  def __str__(self):
    max_task_runtime = max([t.runtime() for t in self.tasks])
    if self.tasks[0].has_fetch:
      input_method = "shuffle"
    else:
      input_method = self.tasks[0].input_read_method
    return (("%s tasks (avg runtime: %s, max runtime: %s) Start: %s, runtime: %s, "
      "Max concurrency: %s, "
      "Input MB: %s (from %s), Output MB: %s, Straggers: %s, Progress rate straggers: %s, "
      "Progress rate stragglers explained by scheduler delay (%s), HDFS read (%s), "
      "HDFS and read (%s), GC (%s), Network (%s), JIT (%s), output rate stragglers: %s") %
      (len(self.tasks), self.average_task_runtime(), max_task_runtime, self.start_time,
       self.finish_time() - self.start_time, concurrency.get_max_concurrency(self.tasks),
       self.input_mb(), input_method, self.output_mb(),
       self.traditional_stragglers(), self.progress_rate_stragglers()[0],
       self.scheduler_delay_stragglers()[0], self.hdfs_read_stragglers()[0],
       self.hdfs_read_and_scheduler_delay_stragglers()[0], self.gc_stragglers()[0],
       # Do not compute the JIT stragglers here! Screws up the calculation.
       self.network_stragglers()[0], -1,
       self.output_progress_rate_stragglers()[0]))

  def verbose_str(self):
    # Get info about the longest task.
    max_index = -1
    max_runtime = -1
    for i, task in enumerate(self.tasks):
      if task.runtime() > max_runtime:
        max_runtime = task.runtime()
        max_index = i
    return "%s\n    Longest Task: %s" % (self, self.tasks[i])

  def get_executor_id_to_resource_metrics(self):
    """Compiles a description of this stage's resource usage on each executor.

    Returns a mapping from executor id to an ExecutorResourceMetrics object containing the
    executor's CPU, network, and GC resource usage while this stage was running.
    """
    return {executor: metrics.ExecutorResourceMetrics.get_resource_metrics_for_executor_tasks(tasks)
      for executor, tasks in self.get_executor_id_to_tasks().iteritems()}

  def get_executor_id_to_tasks(self):
    """
    Returns a mapping from executor id to a list of all the tasks from this stage that ran on that
    executor.
    """
    executor_id_to_tasks = {}
    for task in self.tasks:
      if task.executor_id not in executor_id_to_tasks:
        executor_id_to_tasks[task.executor_id] = []
      executor_id_to_tasks[task.executor_id].append(task)
    return executor_id_to_tasks

  def load_balancing_badness(self):
    executor_id_to_tasks = self.get_executor_id_to_tasks()
    total_time = 0
    for executor_id, tasks in executor_id_to_tasks.iteritems():
      min_start_time = min([t.start_time for t in tasks])
      max_finish_time = max([t.finish_time for t in tasks])
      total_time += max_finish_time - min_start_time

    ideal_time = total_time / len(executor_id_to_tasks)
    return float(self.runtime()) / ideal_time

  def runtime(self):
    return self.finish_time() - self.start_time

  def has_shuffle_read(self):
    total_shuffle_read_bytes = sum(
      [t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
    return total_shuffle_read_bytes > 0

  def finish_time(self):
    return max([t.finish_time for t in self.tasks])

  def total_runtime(self):
    return sum([t.finish_time - t.start_time for t in self.tasks])

  def input_mb(self):
    """ Returns the total input size for this stage.

    This is only valid if the stage read data from a shuffle.
    """
    total_input_bytes = sum([t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
    total_input_bytes += sum([t.input_mb for t in self.tasks])
    return total_input_bytes

  def output_mb(self):
    """ Returns the total output size for this stage.

    This is only valid if the output data was written for a shuffle.
    TODO: Add HDFS / in-memory RDD output size.
    """
    total_output_size = sum([t.shuffle_mb_written for t in self.tasks])
    return total_output_size

  def get_network_mb(self):
    return sum([t.remote_mb_read for t in self.tasks if t.has_fetch])

  def add_event(self, data):
    task = Task(data)

    if self.start_time == -1:
      self.start_time = task.start_time
    else:
      self.start_time = min(self.start_time, task.start_time)

    self.tasks.append(task)

  def ideal_time_s(self, network_throughput_gigabits_per_executor, num_cores_per_executor):
    ideal_times = self.get_ideal_times_from_metrics(
      network_throughput_gigabits_per_executor,
      num_cores_per_executor)
    return max(ideal_times)

  def get_ideal_ser_deser_time_s(self, num_cores_per_executor = 8):
    num_executors = len(self.get_executor_id_to_tasks())
    total_ser_time_millis = sum([t.hdfs_ser_comp_millis for t in self.tasks])
    total_deser_time_millis = sum([t.hdfs_deser_decomp_millis for t in self.tasks])
    return (float(total_ser_time_millis + total_deser_time_millis) /
      (num_executors * num_cores_per_executor * 1000))

  def get_ideal_times_from_metrics(
      self,
      network_throughput_gigabits_per_executor,
      num_cores_per_executor = 8,
      use_disk_monotask_times = False):
    """Returns a 3-tuple containing the ideal CPU, network, and disk times (s) for this stage.

    The ideal times are calculated by assuming that the CPU, network, and disk tasks can be
    perfectly scheduled to take advantage of the cluster's available resources.
    """
    # First, calculate the total resource usage based on the OS-level counters.
    # These will be used to sanity check the job's metrics.
    total_cpu_millis = 0
    total_network_bytes_transmitted = 0
    total_disk_bytes_read_written = 0
    total_disk_throughput_Bps = 0

    executor_id_to_metrics = self.get_executor_id_to_resource_metrics()
    disks = set()
    for executor_metrics in executor_id_to_metrics.itervalues():
      total_cpu_millis += executor_metrics.cpu_metrics.cpu_millis

      total_network_bytes_transmitted += executor_metrics.network_metrics.bytes_transmitted

      for disk_name, disk_metrics in executor_metrics.disk_name_to_metrics.iteritems():
        # We only consider disks that are used as Spark or HDFS data directories.
        if disk_name in ["xvdb", "xvdc", "xvdf"]:
          total_disk_bytes_read_written += (disk_metrics.bytes_read + disk_metrics.bytes_written)
          total_disk_throughput_Bps += disk_metrics.effective_throughput_Bps()
          disks.add(disk_name)

    num_executors = len(executor_id_to_metrics)

    ideal_cpu_s = self.__get_ideal_cpu_s(
      total_cpu_millis_os_counters = total_cpu_millis,
      num_executors = num_executors,
      num_cores_per_executor = num_cores_per_executor)

    total_network_throughput_Bps = ((1024 * 1024 * 1024 / 8) *
      len(executor_id_to_metrics) * network_throughput_gigabits_per_executor)
    ideal_network_s = self.__get_ideal_network_s(
      total_network_bytes_os_counters = total_network_bytes_transmitted,
      total_network_throughput_Bps = total_network_throughput_Bps)

    if use_disk_monotask_times:
      ideal_disk_s = self.__get_ideal_disk_s(num_executors, len(disks))
    else:
      # TODO: Compute how many bytes the job thinks it read from / wrote to disk, and use the OS
      # metrics as a sanity-check. This may require adding some info to the continuous monitor
      # about whether the shuffle data was in-memory or on-disk.
      if total_disk_throughput_Bps > 0:
        ideal_disk_s = float(total_disk_bytes_read_written) / total_disk_throughput_Bps
      else:
        ideal_disk_s = 0
        if total_disk_bytes_read_written > 0:
          logging.warning(
            "Outputting 0 disk seconds because throughput while writing {} bytes was 0.".format(
              total_disk_bytes_read_written))
    return (ideal_cpu_s, ideal_network_s, ideal_disk_s)

  def get_ideal_times_from_metrics_fix_executors(
      self,
      network_throughput_gigabits_per_executor,
      num_cores_per_executor = 8,
      use_disk_monotask_times = False):
    """Returns a 3-tuple containing the ideal CPU, network, and disk time(s) for this stage.

    Unlike the above method, this method assumes that the assignment of tasks to worker machines
    is fixed (so, for example, the ideal CPU time is the maximum CPU time on any one executor,
    rather than the total CPU time across all executors divided by the number of executors).

    This method uses the monotask times to determine the ideal compute time, but uses the
    executor metrics to compute the ideal network and disk times.  This is because we don't
    currently have enough disk information to determine the ideal disk time, because each
    monotask just has the total disk time, but doesn't break that into local versus remote
    time, or into how much time was spent on each local disk.
    """
    max_executor_cpu_millis = 0
    max_network_seconds = 0
    max_disk_seconds = 0

    executor_id_to_metrics = self.get_executor_id_to_resource_metrics()
    network_throughput_Bps = network_throughput_gigabits_per_executor * 1024 * 1024 * 1024 / 8.0
    for executor_id, executor_metrics in executor_id_to_metrics.iteritems():
      tasks_for_executor = [t for t in self.tasks if t.executor_id == executor_id]

      # For monotasks, always use the monotask time (not the underlying OS counters) to
      # compute the ideal time, for consistency with the model described in the paper.
      executor_cpu_millis = (sum([t.compute_monotask_millis for t in tasks_for_executor]) /
        num_cores_per_executor)
      if executor_cpu_millis == 0:
        # This is a Spark job, so use the CPU counters.
        executor_cpu_millis = (float(executor_metrics.cpu_metrics.cpu_millis) /
          num_cores_per_executor)
      max_executor_cpu_millis = max(max_executor_cpu_millis, executor_cpu_millis)

      # Use the bytes transmitted to calculate the network time. Could alternately use the
      # bytes received (this won't include any packets that were dropped).
      executor_network_seconds = (
        float(executor_metrics.network_metrics.bytes_transmitted) / network_throughput_Bps)
      max_network_seconds = max(max_network_seconds, executor_network_seconds)

      # For the disks, also assume there's no flexibility in which disk data gets written to.
      # TODO: Should be calculating the disk time based on the disk monotasks, not based on the OS
      # counters.
      disks = set()
      for disk_name, disk_metrics in executor_metrics.disk_name_to_metrics.iteritems():
        if disk_name in ["xvdb", "xvdc", "xvdf"]:
          disks.add(disk_name)
          disk_bytes_read_written = disk_metrics.bytes_read + disk_metrics.bytes_written
          if not use_disk_monotask_times:
            # TODO: Don't use the effective throughput! This can be wrong.
            disk_throughput = disk_metrics.effective_throughput_Bps()
            if disk_bytes_read_written > 0 and disk_throughput > 0:
              disk_seconds = disk_bytes_read_written / disk_throughput
              max_disk_seconds = max(max_disk_seconds, disk_seconds)
      if use_disk_monotask_times:
        # Calcuate the ideal disk time based on the monotask times.
        total_disk_monotask_millis = sum([t.disk_monotask_millis for t in tasks_for_executor])
        disk_seconds = float(total_disk_monotask_millis) / (len(disks) * 1000)
        max_disk_seconds = max(max_disk_seconds, disk_seconds)

    return (max_executor_cpu_millis / 1000., max_network_seconds, max_disk_seconds)

  def __get_ideal_cpu_s(self, total_cpu_millis_os_counters, num_executors, num_cores_per_executor):
    # Attempt to use the CPU monotask time to compute the ideal time. If the CPU monotask time
    # is 0, that means this was a Spark job, in which case we have no choice but to use the OS
    # counters.
    total_cpu_monotask_millis = sum([t.compute_monotask_millis for t in self.tasks])
    if total_cpu_monotask_millis > 0:
      # The compute monotask time should be very close to the time from the OS counters.
      self.__check_times_within_error_bound(
        base_time = total_cpu_monotask_millis,
        second_time = total_cpu_millis_os_counters,
        max_relative_difference = 0.1,
        error_message = ("Executor counters say {} CPU millis elapsed, but total CPU " +
          "monotask time was {}").format(total_cpu_millis_os_counters, total_cpu_monotask_millis))
      # Use the monotask time to compute the ideal time.
      total_cpu_millis = total_cpu_monotask_millis
    else:
      total_cpu_millis = total_cpu_millis_os_counters
    return float(total_cpu_millis) / (num_executors * num_cores_per_executor * 1000)

  def __get_ideal_disk_s(self, num_executors, disks_per_executor):
    """ Returns the ideal disk time, based on the disk monotasks times.

    This should only be used when the disk concurrency was 1 (otherwise this will overestimate
    the disk time significantly).

    TODO: Ideally we'd have the disk each monotask ran on, so we could calculate the degree to which
    issues were because of load balancing issues across the disks.
    """
    total_disk_monotask_millis = sum([t.disk_monotask_millis for t in self.tasks])
    return float(total_disk_monotask_millis) / (num_executors * disks_per_executor * 1000)

  def __get_ideal_network_s(self, total_network_bytes_os_counters, total_network_throughput_Bps):
    job_network_mb = self.get_network_mb()

    total_network_mb_transmitted = total_network_bytes_os_counters / (1024 * 1024)
    # Use the executor data about the total network data transmitted as a sanity check: this
    # should be close to how much data the job thinks it transferred over the network.
    # When the shuffle opportunistically starts early, this will be incorrect, because the job
    # won't think it sent any bytes over the network.
    if (job_network_mb > 0):
      self.__check_times_within_error_bound(
        base_time = job_network_mb,
        second_time = total_network_mb_transmitted,
        max_relative_difference = 0.1,
        error_message = (("Executor counters say {} bytes transmitted, but job thinks {} " +
          "was transmitted").format(total_network_mb_transmitted, job_network_mb)))
     # Ultimately return what the network thinks it transmitted. This is required for the
     # calculation to work properly with the pipelined shuffle, where during the reduce
     # stage, there's a bunch of data transmitted that's not associated with a particular
     # task. It also works better in general, because typically there's some overhead, where
     # the actual data transmitted is somewhat higher than what the job thought.
    return total_network_bytes_os_counters / total_network_throughput_Bps

  def __check_times_within_error_bound(self, base_time, second_time, max_relative_difference,
                                       error_message):

    if float(abs(second_time - base_time)) / base_time > max_relative_difference:
      if base_time > 0 and second_time > 0:
        logging.warning(error_message)

