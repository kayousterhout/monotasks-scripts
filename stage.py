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

  def ideal_time_s(self, num_cores_per_executor):
    ideal_times = self.get_ideal_times_from_metrics(num_cores_per_executor)
    return max(ideal_times)

  def get_ideal_times_from_metrics(self, num_cores_per_executor = 8):
    """Returns a 3-tuple containing the ideal CPU, network, and disk times (s) for this stage.

    The ideal times are calculated by assuming that the CPU, network, and disk tasks can be
    perfectly scheduled to take advantage of the cluster's available resources.
    """
    # First, calculate the total resource usage based on the OS-level counters.
    # These will be used to sanity check the job's metrics.
    total_cpu_millis = 0
    total_network_bytes_transmitted = 0
    total_network_throughput_Bps = 0
    total_disk_bytes_read_written = 0
    total_disk_throughput_Bps = 0

    executor_id_to_metrics = self.get_executor_id_to_resource_metrics()
    for executor_metrics in executor_id_to_metrics.itervalues():
      total_cpu_millis += executor_metrics.cpu_metrics.cpu_millis

      network_metrics = executor_metrics.network_metrics
      total_network_bytes_transmitted += network_metrics.bytes_transmitted
      total_network_throughput_Bps += network_metrics.effective_transmit_throughput_Bps

      for disk_name, disk_metrics in executor_metrics.disk_name_to_metrics.iteritems():
        # We only consider disks that are used as Spark or HDFS data directories.
        if disk_name in ["xvdb", "xvdc", "xvdf"]:
          total_disk_bytes_read_written += (disk_metrics.bytes_read + disk_metrics.bytes_written)
          total_disk_throughput_Bps += disk_metrics.effective_throughput_Bps()

    num_executors = len(executor_id_to_metrics)

    ideal_cpu_s = self.__get_ideal_cpu_s(
      total_cpu_millis_os_counters = total_cpu_millis,
      num_executors = num_executors,
      num_cores_per_executor = num_cores_per_executor)

    ideal_network_s = self.__get_ideal_network_s(
      total_network_bytes_os_counters = total_network_bytes_transmitted,
      total_network_throughput_Bps = total_network_throughput_Bps)

    # TODO: Compute how many bytes the job thinks it read from / wrote to disk, and use the OS
    # metrics as a sanity-check. This may require adding some info to the continuous monitor
    # about whether the shuffle data was in-memory or on-disk.
    if total_disk_throughput_Bps > 0:
      ideal_disk_s = float(total_disk_bytes_read_written) / total_disk_throughput_Bps
    else:
      ideal_disk_s = 0
      logging.getLogger("Stage").warning(
        "Outputting 0 disk seconds because throughput while writing {} bytes was 0.".format(
          total_disk_bytes_read_written))
    return (ideal_cpu_s, ideal_network_s, ideal_disk_s)

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

  def __get_ideal_network_s(self, total_network_bytes_os_counters, total_network_throughput_Bps):
    # Compute how many bytes the job thinks it sent over the network. If that's
    # 0, all of the network traffic was control messages (and not related to the job's
    # completion time) so the ideal network time is 0.
    job_network_mb = self.get_network_mb()
    if job_network_mb == 0:
      return 0

    total_network_mb_transmitted = total_network_bytes_os_counters / (1024 * 1024)
    # Use the executor data about the total network data transmitted as a sanity check: this
    # should be close to how much data the job thinks it transferred over the network.
    self.__check_times_within_error_bound(
      base_time = job_network_mb,
      second_time = total_network_mb_transmitted,
      max_relative_difference = 0.1,
      error_message = (("Executor counters say {} bytes transmitted, but job thinks {} " +
        "was transmitted").format(total_network_mb_transmitted, job_network_mb)))
    return float(job_network_mb) / total_network_throughput_Bps

  def __check_times_within_error_bound(self, base_time, second_time, max_relative_difference,
                                       error_message):
    if float(abs(second_time - base_time)) / base_time > max_relative_difference:
      raise Exception(error_message)

