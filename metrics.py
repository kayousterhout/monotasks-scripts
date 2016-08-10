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

import utils

MILLIS_PER_JIFFY = 10
BYTES_PER_SECTOR = 512

SECTORS_READ_KEY = "Sectors Read"
MILLIS_READING_KEY = "Millis Reading"
SECTORS_WRITTEN_KEY = "Sectors Written"
MILLIS_WRITING_KEY = "Millis Writing"
TOTAL_IO_MILLIS_KEY = "Millis Total"

# This is based on looking at one of the continuous monitors from
# a network-bound experiment running on the millennium cluster.
MILLENNIUM_MAX_NETWORK_GIGABITS_PER_S = 0.917

AWS_M24XLARGE_MAX_NETWORK_GIGABITS_PER_S = 0.7


class CpuMetrics(object):

  def __init__(
      self,
      elapsed_millis,
      cpu_millis,
      num_cores,
      hdfs_deser_decomp_millis,
      hdfs_ser_comp_millis):
    self.elapsed_millis = elapsed_millis
    self.cpu_millis = cpu_millis
    self.num_cores = num_cores
    self.hdfs_deser_decomp_millis = hdfs_deser_decomp_millis
    self.hdfs_ser_comp_millis = hdfs_ser_comp_millis
    self.__calculate_util()

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.cpu_millis += other_metrics.cpu_millis
    self.hdfs_deser_decomp_millis += other_metrics.hdfs_deser_decomp_millis
    self.hdfs_ser_comp_millis += other_metrics.hdfs_ser_comp_millis
    self.__calculate_util()

  def __calculate_util(self):
    self.utilization = float(self.cpu_millis) / (self.elapsed_millis * self.num_cores)

  def __repr__(self):
    return (
      "cpu metrics:\n" +
      "\tutilization: {:.2f}%\n".format(self.utilization * 100) +
      "\tHDFS deser/decomp , ser/comp: {:.2f} s , {:.2f} s\n".format(
        float(self.hdfs_deser_decomp_millis) / 1000,
        float(self.hdfs_ser_comp_millis) / 1000)
    )


class NetworkMetrics(object):
  """ Describes the network utilization on a particular executor during a period of time.

  Currently, self.transmit_idle_millis is not accurate and often overestimates the time the
  network spent idle (because the Java-level network scheduler was idle, but during that period,
  it had passed data to the OS networking stack's buffers, so the network was still in use).
  As a result, transmit_idle_millis should not be used.
  """

  def __init__(self, elapsed_millis, transmit_idle_millis, bytes_transmitted):
    self.elapsed_millis = elapsed_millis
    self.transmit_idle_millis = transmit_idle_millis
    self.transmit_active_millis = self.elapsed_millis - self.transmit_idle_millis
    self.bytes_transmitted = bytes_transmitted
    self.__calculate_util_and_throughput()

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.transmit_idle_millis += other_metrics.transmit_idle_millis
    self.transmit_active_millis += other_metrics.transmit_active_millis
    self.bytes_transmitted += other_metrics.bytes_transmitted
    self.__calculate_util_and_throughput()

  def __calculate_util_and_throughput(self):
    self.transmit_utilization = float(self.transmit_active_millis) / self.elapsed_millis
    self.effective_transmit_throughput_Bps = (
      float(self.bytes_transmitted) / self.transmit_active_millis) * 1000

  def __repr__(self):
    return (
      "network metrics:\n" +
      "\tdata transmitted: {}\n".format(utils.bytes_to_string(self.bytes_transmitted)) +
      "\ttransmit utilization: {:.2f}%\n".format(self.transmit_utilization * 100) +
      "\teffective transmit throughput: {}/s ({}/s)\n".format(
        utils.bits_to_string(self.effective_transmit_throughput_Bps * 8),
        utils.bytes_to_string(self.effective_transmit_throughput_Bps))
    )


class DiskMetrics(object):

  def __init__(self, elapsed_millis, start_counters, end_counters):
    self.elapsed_millis = elapsed_millis
    self.bytes_read = BYTES_PER_SECTOR * (end_counters.get(SECTORS_READ_KEY, 0.) -
      start_counters.get(SECTORS_READ_KEY, 0.))
    # self.millis_reading and self.millis_writing are only updated when a disk access finishes
    # (instead of incrementally as the access progresses), so they can be grossly inaccurate if
    # disk accesses are not completely contained within the sampling window.
    self.millis_reading = (end_counters.get(MILLIS_READING_KEY, 0.) -
      start_counters.get(MILLIS_READING_KEY, 0.))
    self.bytes_written = BYTES_PER_SECTOR * (end_counters.get(SECTORS_WRITTEN_KEY, 0.) -
      start_counters.get(SECTORS_WRITTEN_KEY, 0.))
    self.millis_writing = (end_counters.get(MILLIS_WRITING_KEY, 0.) -
      start_counters.get(MILLIS_WRITING_KEY, 0.))
    self.total_io_millis = (end_counters.get(TOTAL_IO_MILLIS_KEY, 0.) -
      start_counters.get(TOTAL_IO_MILLIS_KEY, 0.))

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.bytes_read += other_metrics.bytes_read
    self.millis_reading += other_metrics.millis_reading
    self.bytes_written += other_metrics.bytes_written
    self.millis_writing += other_metrics.millis_writing
    self.total_io_millis += other_metrics.total_io_millis

  def utilization(self):
    return float(self.total_io_millis) / self.elapsed_millis

  def effective_throughput_Bps(self):
    if self.total_io_millis == 0:
      return 0
    else:
      return (float(self.bytes_read + self.bytes_written) / self.total_io_millis) * 1000

  def __repr__(self):
    return (
      "disk metrics:\n" +
      "\tdata transferred (read,written): {} , {}\n".format(
        utils.bytes_to_string(self.bytes_read), utils.bytes_to_string(self.bytes_written)) +
      "\tutilization: {:.2f}%\n".format(self.utilization() * 100) +
      "\teffective throughput: {}/s\n".format(utils.bytes_to_string(self.effective_throughput_Bps()))
    )


class ExecutorResourceMetrics(object):
  """
  Describes the end-to-end CPU, network, disk, and GC usage of a single executor during some time
  period.
  """

  def __init__(self, start_millis, end_millis, num_tasks, cpu_metrics, network_metrics,
      disk_name_to_metrics, gc_millis):
    self.start_millis = start_millis
    self.end_millis = end_millis
    self.elapsed_millis = self.end_millis - self.start_millis
    self.num_tasks = num_tasks
    self.cpu_metrics = cpu_metrics
    self.network_metrics = network_metrics
    self.disk_name_to_metrics = disk_name_to_metrics
    self.gc_millis = gc_millis

  def add_metrics(self, other_metrics):
    self.elapsed_millis += other_metrics.elapsed_millis
    self.num_tasks += other_metrics.num_tasks
    self.cpu_metrics.add_metrics(other_metrics.cpu_metrics)
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
    start_millis = first_task_to_start.start_time
    end_millis = last_task_to_finish.finish_time
    elapsed_millis = end_millis - start_millis

    cpu_millis = (last_task_to_finish.end_total_cpu_jiffies -
      first_task_to_start.start_total_cpu_jiffies) * MILLIS_PER_JIFFY
    cpu_metrics = CpuMetrics(
      elapsed_millis=elapsed_millis,
      cpu_millis=cpu_millis,
      num_cores=8,
      hdfs_deser_decomp_millis=sum([t.hdfs_deser_decomp_millis for t in tasks]),
      hdfs_ser_comp_millis=sum([t.hdfs_ser_comp_millis for t in tasks])
    )

    transmit_idle_millis = (last_task_to_finish.end_network_transmit_idle_millis -
      first_task_to_start.start_network_transmit_idle_millis)
    bytes_transmitted = (
      last_task_to_finish.network_utilization.end_counters["Transmitted Bytes"] -
      first_task_to_start.network_utilization.start_counters["Transmitted Bytes"])
    network_metrics = NetworkMetrics(
      elapsed_millis=elapsed_millis,
      transmit_idle_millis=transmit_idle_millis,
      bytes_transmitted=bytes_transmitted)

    disk_name_to_metrics = {}
    for disk_name, disk_utilization in first_task_to_start.disk_utilization.iteritems():
      disk_name_to_metrics[disk_name] = DiskMetrics(
        elapsed_millis=elapsed_millis,
        start_counters=first_task_to_start.disk_utilization[disk_name].start_counters,
        end_counters=last_task_to_finish.disk_utilization[disk_name].end_counters,
      )

    gc_millis = last_task_to_finish.end_gc_millis - first_task_to_start.start_gc_millis
    return ExecutorResourceMetrics(
      start_millis=start_millis,
      end_millis=end_millis,
      num_tasks=len(tasks),
      cpu_metrics=cpu_metrics,
      network_metrics=network_metrics,
      disk_name_to_metrics=disk_name_to_metrics,
      gc_millis=gc_millis)

  def __repr__(self):
    return (
      "time (start,elapsed,end): {} ms, {} ms, {} ms\n".format(
        self.start_millis, self.elapsed_millis, self.end_millis) +
      "num tasks: {}\n".format(self.num_tasks) +
      str(self.cpu_metrics) +
      str(self.network_metrics) +
      "".join(["{} {}".format(disk_name, disk_metrics)
        for disk_name, disk_metrics in self.disk_name_to_metrics.iteritems()]) +
      "GC millis: {}\n".format(self.gc_millis)
    )


class NetworkUtilization(object):
  """ Holds network utilization metadata from while a particular task was running.

  This information is parsed from an event log.
  """

  def __init__(self, start_counters, end_counters, bytes_transmitted_ps, bytes_received_ps):
    self.start_counters = start_counters
    self.end_counters = end_counters
    self.bytes_transmitted_ps = bytes_transmitted_ps
    self.bytes_received_ps = bytes_received_ps


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
