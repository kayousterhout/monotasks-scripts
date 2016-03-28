import argparse
import json
import plot_gnuplot
import plot_matplotlib

BYTES_PER_GIGABYTE = float(1024 * 1024 * 1024)
BYTES_PER_KILOBYTE = 1024 * 1024
BYTES_PER_GIGABIT = BYTES_PER_GIGABYTE / 8
CORES = 8.0

class DiskUtilization:
  """ Represents the utilization of one disk at one point in time. """
  def __init__(self, json_entry):
    assert(len(json_entry) == 1)
    self.disk_name, utilization_info = json_entry.popitem()
    self.total_utilization = utilization_info["Disk Utilization"]
    self.read_throughput = utilization_info["Read Throughput"]
    self.write_throughput = utilization_info["Write Throughput"]
    self.running_disk_monotasks = 0


def is_valid_disk_name(disk_name):
  """ Returns true if the given disk name is the name of a physical disk.

  This method is based on knowledge about how the EC2 and r*.millennium
  machines are configured.
  """
  if "ram" in disk_name or "xvda1" in disk_name:
    # Ignore the ram disks on the millinnium machines, and the S3 mount on the EC2 machines.
    return False
  if "sd" in disk_name:
    # This is a disk on one of the millennium machines! Only the "sbd" and "sda1" disks are valid.
    if disk_name not in ["sdb", "sda1"]:
      return False
  return True


def plot_continuous_monitor(filename, open_graphs=False, use_gnuplot=False):
  continuous_monitor_data = []

  start = -1
  at_beginning = True
  # Mapping of disk IDs to the 1-indexed index in utilization file where the information
  # about that disk begins.
  disks_to_index = {}
  for (i, line) in enumerate(open(filename, "r")):
    try:
      json_data = json.loads(line)
    except ValueError:
      # This typically happens at the end of the file, which can get cutoff when the job stops.
      print "Stopping parsing due to incomplete line"
      if not at_beginning:
        break
      else:
        # There are some non-JSON lines at the beginning of the file.
        print "Skipping non-JSON line at beginning of file: %s" % line
        continue
    at_beginning = False
    time = json_data["Current Time"]
    if start == -1:
      start = time
    raw_disk_utilizations = json_data["Disk Utilization"]["Device Name To Utilization"]
    disk_to_utilization = {}
    for utilization_json in raw_disk_utilizations:
      parsed_utilization = DiskUtilization(utilization_json)
      disk_name = parsed_utilization.disk_name
      if (is_valid_disk_name(disk_name)):
        disk_to_utilization[disk_name] = parsed_utilization
    cpu_utilization = json_data["Cpu Utilization"]
    cpu_system = cpu_utilization["Total System Utilization"]
    cpu_total = (cpu_utilization["Total User Utilization"] +
      cpu_utilization["Total System Utilization"])
    network_utilization = json_data["Network Utilization"]
    bytes_received = network_utilization["Bytes Received Per Second"]
    running_compute_monotasks = 0
    if "Running Compute Monotasks" in json_data:
      running_compute_monotasks = json_data["Running Compute Monotasks"]

    if "Running Disk Monotasks" in json_data:
      # Parse the number of currently running disk monotasks for each disk.
      for running_disk_monotasks_info in json_data["Running Disk Monotasks"]:
        running_disk_monotasks = running_disk_monotasks_info["Running And Queued Monotasks"]
        disk_name = running_disk_monotasks_info["Disk Name"]
        if disk_name in disk_to_utilization:
          disk_utilization = disk_to_utilization[disk_name]
          disk_utilization.running_disk_monotasks = running_disk_monotasks

    running_macrotasks = 0
    if "Running Macrotasks" in json_data:
      running_macrotasks = json_data["Running Macrotasks"]
    local_running_macrotasks = 0
    if "Local Running Macrotasks" in json_data:
      local_running_macrotasks = json_data["Local Running Macrotasks"]
    gc_fraction = 0
    if "Fraction GC Time" in json_data:
      gc_fraction = json_data["Fraction GC Time"]
    outstanding_network_bytes = 0
    if "Outstanding Network Bytes" in json_data:
      outstanding_network_bytes = json_data["Outstanding Network Bytes"]
    if bytes_received == "NaN" or bytes_received == "Infinity":
      continue
    bytes_transmitted = network_utilization["Bytes Transmitted Per Second"]
    if bytes_transmitted == "NaN" or bytes_transmitted == "Infinity":
      continue
    if str(cpu_total).find("NaN") > -1 or str(cpu_total).find("Infinity") > -1:
      continue
    macrotasks_in_network = 0
    if "Macrotasks In Network" in json_data:
      macrotasks_in_network = json_data["Macrotasks In Network"]
    macrotasks_in_compute = 0
    if "Macrotasks In Compute" in json_data:
      macrotasks_in_compute = json_data["Macrotasks In Compute"]
    macrotasks_in_disk = 0
    if "Macrotasks In Disk" in json_data:
      macrotasks_in_disk = json_data["Macrotasks In Disk"]
    free_heap_memory = 0
    if "Free Heap Memory Bytes" in json_data:
      free_heap_memory = json_data["Free Heap Memory Bytes"]
    free_off_heap_memory = 0
    if "Free Off-Heap Memory Bytes" in json_data:
      free_off_heap_memory = json_data["Free Off-Heap Memory Bytes"]

    data = [
      ('time', time - start),
      ('cpu utilization', cpu_total / CORES),
      ('bytes received', bytes_received / BYTES_PER_GIGABIT),
      ('bytes transmitted', bytes_transmitted / BYTES_PER_GIGABIT),
      ('running compute monotasks', running_compute_monotasks), # 5
      ('running macrotasks', running_macrotasks),
      ('gc fraction', gc_fraction),
      ('outstanding network bytes', outstanding_network_bytes / BYTES_PER_KILOBYTE),
      ('macrotasks in network', macrotasks_in_network),
      ('macrotasks in compute', macrotasks_in_compute), # 10
      ('cpu system', cpu_system / CORES),
      ('macrotasks in disk', macrotasks_in_disk),
      ('free heap memory', free_heap_memory / BYTES_PER_GIGABYTE),
      ('free off heap memory', free_off_heap_memory / BYTES_PER_GIGABYTE),
      ('local running macrotasks', local_running_macrotasks) # 15
    ]

    # Append info about each disk (in sorted order, so that the standard EC2 disks appear in
    # predictable order).
    for disk_id, disk_util in sorted(disk_to_utilization.iteritems()):
      # This is done redundantly for each line in the continuous monitor, but they should all be
      # the same, so this should be an issue. Saving the index needs to happen before the
      # disk's utilization information gets appended to data below.
      disks_to_index[disk_id] = len(data) + 1

      data.extend([
        ("%s utilization" % disk_id, disk_util.total_utilization),
        ("%s read throughput" % disk_id, disk_util.read_throughput),
        ("%s write throughput" % disk_id, disk_util.write_throughput),
        ("%s running disk monotasks" % disk_id, disk_util.running_disk_monotasks)
      ])

    continuous_monitor_data.append(data)

  if use_gnuplot:
    plot_gnuplot.plot(continuous_monitor_data, filename, open_graphs, disks_to_index)
  else:
    plot_matplotlib.plot([dict(line) for line in continuous_monitor_data],
                         filename, open_graphs)


def get_util_for_disk(disk_utils, disk):
  """
  Returns the disk utilization metrics for the specified disk, given the
  utilization information for all disks, or None if the desired disk cannot be
  found.
  """
  for disk_util in disk_utils:
    if disk in disk_util:
      return disk_util[disk]
  return None


def parse_args():
  parser = argparse.ArgumentParser(description="Plots Spark continuous monitor logs.")
  parser.add_argument("-f", "--filename",
                      help="The path to a continuous monitor log file.",
                      required=True)
  parser.add_argument("-o", "--open-graphs",
                      help="open generated graphs",
                      action="store_true", default=False)
  parser.add_argument("-g", "--gnuplot",
                      help="generate graphs with gnuplot",
                      action="store_true", default=False)

  return parser.parse_args()


def main():
  args = parse_args()
  plot_continuous_monitor(args.filename, args.open_graphs, args.gnuplot)

if __name__ == "__main__":
  main()
