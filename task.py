import logging

import metrics


class Task:
  def __init__(self, data):
    self.initialize_from_json(data)

    self.scheduler_delay = (self.finish_time - self.executor_run_time -
      self.executor_deserialize_time - self.result_serialization_time - self.start_time)
    # Should be set to true if this task is a straggler and we know the cause of the
    # straggler behavior.
    self.straggler_behavior_explained = False

  def initialize_from_json(self, json_data):
    self.logger = logging.getLogger("Task")

    task_info = json_data["Task Info"]
    task_metrics = json_data["Task Metrics"]
    self.task_id = task_info["Task ID"]

    # Times for monotasks.
    self.disk_monotask_millis = 0
    if "Disk Nanos" in task_metrics:
      self.disk_monotask_millis = task_metrics["Disk Nanos"] / 1000000.

    self.compute_monotask_millis = 0
    if "Computation Nanos" in task_metrics:
      self.compute_monotask_millis = task_metrics["Computation Nanos"] / 1000000.

    DISK_BYTES_SPILLED_KEY = "Disk Bytes Spilled"
    if DISK_BYTES_SPILLED_KEY in task_metrics and task_metrics[DISK_BYTES_SPILLED_KEY] > 0:
      print "Task has spilled disk bytes! these aren't accounted for in metrics"
      print self.task_id

    self.start_time = task_info["Launch Time"]
    self.finish_time = task_info["Finish Time"]
    self.executor = task_info["Host"]
    self.executor_run_time = task_metrics["Executor Run Time"]
    self.executor_deserialize_time = task_metrics["Executor Deserialize Time"]
    self.result_serialization_time = task_metrics["Result Serialization Time"]
    self.gc_time = task_metrics["JVM GC Time"]
    self.end_gc_millis = task_metrics.get("JVM GC Time Total", 0.)
    self.start_gc_millis = self.end_gc_millis - self.gc_time
    self.executor_id = task_info["Executor ID"]

    self.disk_utilization = {}
    DISK_UTILIZATION_KEY = "Disk Utilization"
    if DISK_UTILIZATION_KEY in task_metrics:
      for dic in task_metrics[DISK_UTILIZATION_KEY]["Device Name To Utilization"]:
        for device_name, block_utilization in dic.iteritems():
          self.disk_utilization[device_name] = metrics.DiskUtilization(
            block_utilization.get("Start Counters", {}),
            block_utilization.get("End Counters", {}),
            block_utilization["Disk Utilization"],
            block_utilization["Read Throughput"],
            block_utilization["Write Throughput"])

    self.start_network_transmit_idle_millis = task_metrics.get(
      "Start Network Transmit Total Idle Millis", 0.)
    self.end_network_transmit_idle_millis = task_metrics.get(
      "End Network Transmit Total Idle Millis", 0.)

    self.network_bytes_transmitted_ps = 0
    self.network_bytes_received_ps = 0
    NETWORK_UTILIZATION_KEY = "Network Utilization"
    if NETWORK_UTILIZATION_KEY in task_metrics:
      network_utilization = task_metrics[NETWORK_UTILIZATION_KEY]
      self.network_bytes_transmitted_ps = network_utilization["Bytes Transmitted Per Second"]
      self.network_bytes_received_ps = network_utilization["Bytes Received Per Second"]

    self.process_cpu_utilization = 0
    self.process_user_cpu_utilization = 0
    self.process_system_cpu_utilization = 0
    self.total_cpu_utilization = 0
    self.start_total_cpu_jiffies = 0
    self.start_cpu_utilization_millis = 0
    self.end_total_cpu_jiffies = 0
    self.end_cpu_utilization_millis = 0
    CPU_UTILIZATION_KEY = "Cpu Utilization"
    if CPU_UTILIZATION_KEY in task_metrics:
      cpu_utilization = task_metrics[CPU_UTILIZATION_KEY]
      self.process_user_cpu_utilization = cpu_utilization["Process User Utilization"]
      self.process_system_cpu_utilization = cpu_utilization["Process System Utilization"]
      self.process_cpu_utilization = (self.process_user_cpu_utilization +
        self.process_system_cpu_utilization)
      self.total_cpu_utilization = (cpu_utilization["Total User Utilization"] +
        cpu_utilization["Total System Utilization"])

      START_COUNTER_KEY = "Start Counters"
      if START_COUNTER_KEY in cpu_utilization:
        start_counters = cpu_utilization[START_COUNTER_KEY]
        self.start_total_cpu_jiffies = (start_counters["Total User Jiffies"] +
          start_counters["Total System Jiffies"])
        self.start_cpu_utilization_millis = start_counters["Time Milliseconds"]
        end_counters = cpu_utilization["End Counters"]
        self.end_total_cpu_jiffies = (end_counters["Total User Jiffies"] +
          end_counters["Total System Jiffies"])
        self.end_cpu_utilization_millis = end_counters["Time Milliseconds"]

    self.shuffle_write_time = 0
    self.shuffle_mb_written = 0
    SHUFFLE_WRITE_METRICS_KEY = "Shuffle Write Metrics"
    if SHUFFLE_WRITE_METRICS_KEY in task_metrics:
      shuffle_write_metrics = task_metrics[SHUFFLE_WRITE_METRICS_KEY]
      # Convert to milliseconds (from nanoseconds).
      self.shuffle_write_time = shuffle_write_metrics["Shuffle Write Time"] / 1.0e6
      OPEN_TIME_KEY = "Shuffle Open Time"
      if OPEN_TIME_KEY in shuffle_write_metrics:
        shuffle_open_time = shuffle_write_metrics[OPEN_TIME_KEY] / 1.0e6
        self.shuffle_write_time += shuffle_open_time
      CLOSE_TIME_KEY = "Shuffle Close Time"
      if CLOSE_TIME_KEY in shuffle_write_metrics:
        shuffle_close_time = shuffle_write_metrics[CLOSE_TIME_KEY] / 1.0e6
        self.shuffle_write_time += shuffle_close_time
      self.shuffle_mb_written = shuffle_write_metrics["Shuffle Bytes Written"] / 1048576.

    INPUT_METRICS_KEY = "Input Metrics"
    self.input_read_time = 0
    self.input_read_method = "unknown"
    self.input_mb = 0
    if INPUT_METRICS_KEY in task_metrics:
      input_metrics = task_metrics[INPUT_METRICS_KEY]
      if "Read Time Nanos" in input_metrics:
        self.input_read_time = input_metrics["Read Time Nanos"] / 1.0e6
      self.input_read_method = input_metrics["Data Read Method"]
      if self.input_read_method == "Hadoop" and "Hadoop Bytes Read" in input_metrics:
        # Use a special counter; Spark's estimate is wrong.
        self.input_mb = input_metrics["Hadoop Bytes Read"] / 1048576.
      else:
        self.input_mb = input_metrics["Bytes Read"] / 1048576.

    self.output_write_time = 0
    self.output_mb = 0
    self.output_on_disk = True
    OUTPUT_WRITE_KEY = "Output Write Blocked Nanos"
    if OUTPUT_WRITE_KEY in task_metrics:
      self.output_write_time = task_metrics[OUTPUT_WRITE_KEY] / 1.0e6

    OUTPUT_BYTES_KEY = "Output Bytes"
    if OUTPUT_BYTES_KEY in task_metrics:
      self.output_mb = task_metrics[OUTPUT_BYTES_KEY] / 1048576.

    # Account for in-memory output.
    UPDATED_BLOCKS_KEY = "Updated Blocks"
    if UPDATED_BLOCKS_KEY in task_metrics:
      for block in task_metrics[UPDATED_BLOCKS_KEY]:
        status = block["Status"]
        memory_size = block["Status"]["Memory Size"]
        if status["Memory Size"] > 0 and self.output_on_disk:
          assert(self.output_mb == 0)
          self.output_on_disk = False
        self.output_mb += memory_size / 1048576.

    self.has_fetch = True
    # False if the task was a map task that did not run locally with its input data.
    self.data_local = True
    SHUFFLE_READ_METRICS_KEY = "Shuffle Read Metrics"
    if SHUFFLE_READ_METRICS_KEY not in task_metrics:
      if task_info["Locality"] != "NODE_LOCAL":
        self.data_local = False
      self.has_fetch = False
      return

    shuffle_read_metrics = task_metrics[SHUFFLE_READ_METRICS_KEY]

    self.fetch_wait = shuffle_read_metrics["Fetch Wait Time"]
    self.local_blocks_read = shuffle_read_metrics["Local Blocks Fetched"]
    self.remote_blocks_read = shuffle_read_metrics["Remote Blocks Fetched"]
    self.remote_mb_read = shuffle_read_metrics["Remote Bytes Read"] / 1048576.
    self.local_mb_read = 0
    LOCAL_BYTES_READ_KEY = "Local Bytes Read"
    if LOCAL_BYTES_READ_KEY in shuffle_read_metrics:
      self.local_mb_read = shuffle_read_metrics[LOCAL_BYTES_READ_KEY] / 1048576.
    # The local read time is not included in the fetch wait time: the task blocks
    # on reading data locally in the BlockFetcherIterator.initialize() method.
    self.local_read_time = 0
    LOCAL_READ_TIME_KEY = "Local Read Time"
    if LOCAL_READ_TIME_KEY in shuffle_read_metrics:
      # Local read time is in nanoseconds in Kay's special branch.
      self.local_read_time = shuffle_read_metrics[LOCAL_READ_TIME_KEY] / 1.0e6
    self.total_time_fetching = shuffle_read_metrics["Fetch Wait Time"]

  def input_size_mb(self):
    if self.has_fetch:
      return self.remote_mb_read + self.local_mb_read
    else:
      return self.input_mb

  def __str__(self):
    if self.has_fetch:
      base = self.start_time
      # Print times relative to the start time so that they're easier to read.
      desc = (("Start time: %s, local read time: %s, " +
            "fetch wait: %s, compute time: %s, gc time: %s, shuffle write time: %s, " +
            "result ser: %s, finish: %s, shuffle bytes: %s, input bytes: %s") %
             (self.start_time, self.local_read_time,
              self.fetch_wait, self.compute_time(), self.gc_time,
              self.shuffle_write_time, self.result_serialization_time, self.finish_time - base,
              self.local_mb_read + self.remote_mb_read, self.input_mb))
    else:
      desc = (("Start time: %s, finish: %s, scheduler delay: %s, input read time: %s, " +
        "gc time: %s, shuffle write time: %s") %
        (self.start_time, self.finish_time, self.scheduler_delay, self.input_read_time,
         self.gc_time, self.shuffle_write_time))
    return desc

  def log_verbose(self):
    self.logger.debug(str(self))

  def runtime(self):
    return self.finish_time - self.start_time
