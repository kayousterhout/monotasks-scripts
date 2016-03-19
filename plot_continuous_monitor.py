import argparse
import json
import plot_gnuplot
import plot_matplotlib

BYTES_PER_GIGABYTE = float(1024 * 1024 * 1024)
BYTES_PER_KILOBYTE = 1024 * 1024
BYTES_PER_GIGABIT = BYTES_PER_GIGABYTE / 8
CORES = 8.0

def plot_continuous_monitor(filename, open_graphs=False, use_gnuplot=False):
  continuous_monitor_data = []

  start = -1
  at_beginning = True
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
    disk_utilizations = json_data["Disk Utilization"]["Device Name To Utilization"]
    xvdf_utilization = get_util_for_disk(disk_utilizations, "xvdf")
    xvdb_utilization = get_util_for_disk(disk_utilizations, "xvdb")
    xvdf_total_utilization = xvdf_utilization["Disk Utilization"]
    xvdb_total_utilization = xvdb_utilization["Disk Utilization"]
    xvdf_read_throughput = xvdf_utilization["Read Throughput"]
    xvdf_write_throughput = xvdf_utilization["Write Throughput"]
    xvdb_read_throughput = xvdb_utilization["Read Throughput"]
    xvdb_write_throughput = xvdb_utilization["Write Throughput"]
    cpu_utilization = json_data["Cpu Utilization"]
    cpu_system = cpu_utilization["Total System Utilization"]
    cpu_total = (cpu_utilization["Total User Utilization"] +
      cpu_utilization["Total System Utilization"])
    network_utilization = json_data["Network Utilization"]
    bytes_received = network_utilization["Bytes Received Per Second"]
    running_compute_monotasks = 0
    if "Running Compute Monotasks" in json_data:
      running_compute_monotasks = json_data["Running Compute Monotasks"]

    xvdf_running_disk_monotasks = 0
    xvdb_running_disk_monotasks = 0
    if "Running Disk Monotasks" in json_data:
      # Parse the number of currently running disk monotasks for each disk.
      for running_disk_monotasks_info in json_data["Running Disk Monotasks"]:
        running_disk_monotasks = running_disk_monotasks_info["Running And Queued Monotasks"]
        disk_name = running_disk_monotasks_info["Disk Name"]
        if "xvdf" in disk_name:
          xvdf_running_disk_monotasks = running_disk_monotasks
        elif "xvdb" in disk_name:
          xvdb_running_disk_monotasks = running_disk_monotasks

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
      ('xvdf utilization', xvdf_total_utilization),
      ('xvdb utilization', xvdb_total_utilization),
      ('cpu utilization', cpu_total / CORES),
      ('bytes received', bytes_received / BYTES_PER_GIGABIT),
      ('bytes transmitted', bytes_transmitted / BYTES_PER_GIGABIT),
      ('running compute monotasks', running_compute_monotasks),
      ('running monotasks', running_macrotasks),
      ('gc fraction', gc_fraction),
      ('outstanding network bytes', outstanding_network_bytes / BYTES_PER_KILOBYTE),
      ('macrotasks in network', macrotasks_in_network),
      ('macrotasks in compute', macrotasks_in_compute),
      ('cpu system', cpu_system / CORES),
      ('macrotasks in disk', macrotasks_in_disk),
      ('xvdf read throughput', xvdf_read_throughput),
      ('xvdf write throughput', xvdf_write_throughput),
      ('xvdb read throughput', xvdb_read_throughput),
      ('xvdb write throughput', xvdb_write_throughput),
      ('xvdf running disk monotasks', xvdf_running_disk_monotasks),
      ('xvdb running disk monotasks', xvdb_running_disk_monotasks),
      ('free heap memory', free_heap_memory / BYTES_PER_GIGABYTE),
      ('free off heap memory', free_off_heap_memory / BYTES_PER_GIGABYTE),
      ('local running macrotasks', local_running_macrotasks)
    ]
    continuous_monitor_data.append(data)

  if use_gnuplot:
    plot_gnuplot.plot(continuous_monitor_data, filename, open_graphs)
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
