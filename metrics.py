#
# Copyright 2016 The Regents of The University California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

MILLIS_PER_JIFFY = 10
BYTES_PER_SECTOR = 512

SECTORS_READ_KEY = "Sectors Read"
MILLIS_READING_KEY = "Millis Reading"
SECTORS_WRITTEN_KEY = "Sectors Written"
MILLIS_WRITING_KEY = "Millis Writing"


class NetworkMetrics(object):

  def __init__(self, elapsed_millis, transmit_idle_millis):
    self.elapsed_millis = elapsed_millis
    self.transmit_idle_millis = transmit_idle_millis
    self.transmit_active_millis = self.elapsed_millis - self.transmit_idle_millis

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.transmit_idle_millis += other_metrics.transmit_idle_millis
    self.transmit_active_millis += other_metrics.transmit_active_millis

  def __repr__(self):
    return (
      "network metrics:\n" +
      "\ttransmit idle millis: {}\n".format(self.transmit_idle_millis) +
      "\ttransmit active millis: {}\n".format(self.transmit_active_millis)
    )


class DiskMetrics(object):

  def __init__(self, elapsed_millis, start_counters, end_counters):
    self.elapsed_millis = elapsed_millis
    self.bytes_read = BYTES_PER_SECTOR * (end_counters.get(SECTORS_READ_KEY, 0.) -
      start_counters.get(SECTORS_READ_KEY, 0.))
    self.millis_reading = (end_counters.get(MILLIS_READING_KEY, 0.) -
      start_counters.get(MILLIS_READING_KEY, 0.))
    self.bytes_written = BYTES_PER_SECTOR * (end_counters.get(SECTORS_WRITTEN_KEY, 0.) -
      start_counters.get(SECTORS_WRITTEN_KEY, 0.))
    self.millis_writing = (end_counters.get(MILLIS_WRITING_KEY, 0.) -
      start_counters.get(MILLIS_WRITING_KEY, 0.))

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.bytes_read += other_metrics.bytes_read
    self.millis_reading += other_metrics.millis_reading
    self.bytes_written += other_metrics.bytes_written
    self.millis_writing += other_metrics.millis_writing

  def __repr__(self):
    return (
      "disk metrics:\n" +
      "\tbytes read: {}\n".format(self.bytes_read) +
      "\tmillis reading: {}\n".format(self.millis_reading) +
      "\tbytes written: {}\n".format(self.bytes_written) +
      "\tmillis writing: {}".format(self.millis_writing)
    )


class ExecutorResourceMetrics(object):
  """
  Describes the end-to-end CPU, network, disk, and GC usage of a single executor during some time
  period.
  """

  def __init__(self, elapsed_millis, num_tasks, cpu_millis, network_metrics,
      disk_name_to_metrics, gc_millis):
    self.elapsed_millis = elapsed_millis
    self.num_tasks = num_tasks
    self.cpu_millis = cpu_millis
    self.network_metrics = network_metrics
    self.disk_name_to_metrics = disk_name_to_metrics
    self.gc_millis = gc_millis

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.num_tasks += other_metrics.num_tasks
    self.cpu_millis += other_metrics.cpu_millis
    self.network_metrics.add_metrics(other_metrics.network_metrics)
    for disk_name, disk_metrics in other_metrics.disk_name_to_metrics.iteritems():
      self.disk_name_to_metrics[disk_name].add_metrics(disk_metrics)
    self.gc_millis += other_metrics.gc_millis

  @staticmethod
  def get_resource_metrics_for_executor_tasks(tasks):
    """Creates an ExecutorResourceMetrics object for the provided tasks.

    All of the tasks in `tasks` should have run on the same executor. Returns an
    ExecutorResourceMetrics object describing the CPU, network, disk, and GC usage
    during the time period from when the first task started running on the
    executor until the last task finished on the executor.
    """
    first_task_to_start = sorted(tasks, key=lambda task: task.start_time)[0]
    last_task_to_finish = sorted(tasks, key=lambda task: task.finish_time)[-1]
    elapsed_millis = last_task_to_finish.finish_time - first_task_to_start.start_time

    cpu_millis = (last_task_to_finish.end_total_cpu_jiffies -
      first_task_to_start.start_total_cpu_jiffies) * MILLIS_PER_JIFFY
    transmit_idle_millis = (last_task_to_finish.end_network_transmit_idle_millis -
      first_task_to_start.start_network_transmit_idle_millis)
    network_metrics = NetworkMetrics(
      elapsed_millis=elapsed_millis,
      transmit_idle_millis=transmit_idle_millis)

    disk_name_to_metrics = {}
    for disk_name, disk_utilization in first_task_to_start.disk_utilization.iteritems():
      disk_name_to_metrics[disk_name] = DiskMetrics(
        elapsed_millis=elapsed_millis,
        start_counters=first_task_to_start.disk_utilization[disk_name].start_counters,
        end_counters=last_task_to_finish.disk_utilization[disk_name].end_counters)

    gc_millis = last_task_to_finish.end_gc_millis - first_task_to_start.start_gc_millis
    return ExecutorResourceMetrics(
      elapsed_millis=elapsed_millis,
      num_tasks=len(tasks),
      cpu_millis=cpu_millis,
      network_metrics=network_metrics,
      disk_name_to_metrics=disk_name_to_metrics,
      gc_millis=gc_millis)

  def __repr__(self):
    return (
      "elapsed millis: {}\n".format(self.elapsed_millis) +
      "num tasks: {}\n".format(self.num_tasks) +
      "CPU millis: {}\n".format(self.cpu_millis) +
      str(self.network_metrics) +
      "\n".join(["{} {}".format(disk_name, disk_metrics)
        for disk_name, disk_metrics in self.disk_name_to_metrics.iteritems()]) +
      "\nGC millis: {}\n".format(self.gc_millis)
    )


class DiskUtilization(object):
  """ Holds disk utilization metadata from while a particular task was running.

  This information is parsed from an event log.
  """

  def __init__(self, start_counters, end_counters, utilization, read_throughput_Bps,
      write_throughput_Bps):
    self.start_counters = start_counters
    self.end_counters = end_counters
    self.utilization = utilization
    self.read_throughput_Bps = read_throughput_Bps
    self.write_throughput_Bps = write_throughput_Bps
