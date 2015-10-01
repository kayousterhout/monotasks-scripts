import inspect
import json
import os
import subprocess
import sys

def write_data(out_file, data):
  stringified = [str(x) for x in data]
  out_file.write("\t".join(stringified))
  out_file.write("\n")

def plot_continuous_monitor(filename, open_graphs=False):
  out_filename = "%s_utilization" % filename
  out_file = open(out_filename, "w")

  # Get the location of the monotasks-scripts repository by getting the directory containing the
  # file that is currently being executed.
  scripts_dir = os.path.dirname(inspect.stack()[0][1])

  # Write plot files.
  utilization_plot_filename = "%s_utilization.gp" % filename
  utilization_plot_file = open(utilization_plot_filename, "w")
  for line in open(os.path.join(scripts_dir, "plot_utilization_base.gp"), "r"):
    new_line = line.replace("__NAME__", out_filename)
    utilization_plot_file.write(new_line)
  utilization_plot_file.close()

  disk_plot_filename = "%s_disk_utilization.gp" % filename
  disk_plot_file = open(disk_plot_filename, "w")
  for line in open(os.path.join(scripts_dir, "plot_disk_utilization_base.gp"), "r"):
    new_line = line.replace("__OUT_FILENAME__", "%s_disk_utilization.pdf" % filename).replace(
      "__NAME__", out_filename)
    disk_plot_file.write(new_line)
  disk_plot_file.close()

  monotasks_plot_filename = "%s_monotasks.gp" % filename
  monotasks_plot_file = open(monotasks_plot_filename, "w")
  for line in open(os.path.join(scripts_dir, "plot_monotasks_base.gp"), "r"):
    new_line = line.replace("__OUT_FILENAME__", "%s_monotasks.pdf" % filename).replace(
      "__NAME__", out_filename)
    monotasks_plot_file.write(new_line)
  monotasks_plot_file.close()

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
    disk_utilization = json_data["Disk Utilization"]["Device Name To Utilization"]
    xvdf_utilization = disk_utilization[0]["xvdf"]
    xvdb_utilization = disk_utilization[1]["xvdb"]
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
    running_macrotasks = 0

    # Parse the number of currently running disk monotasks for each disk.
    # xvdf is mounted on /mnt2 and xvdb on /mnt (the indexing below will need to be updated
    # if we use different instance types).
    running_disk_monotasks_info = json_data["Running Disk Monotasks"]
    RUNNING_MONOTASKS_KEY = "Running And Queued Monotasks"
    xvdf_running_disk_monotasks = running_disk_monotasks_info[0][RUNNING_MONOTASKS_KEY]
    xvdb_running_disk_monotasks = running_disk_monotasks_info[1][RUNNING_MONOTASKS_KEY]

    if "Running Macrotasks" in json_data:
      running_macrotasks = json_data["Running Macrotasks"]
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

    data = [
      time - start,
      xvdf_total_utilization,
      xvdb_total_utilization,
      cpu_total / 8.0,
      bytes_received / 125000000.,
      bytes_transmitted / 125000000.,
      running_compute_monotasks,
      running_macrotasks,
      gc_fraction,
      outstanding_network_bytes / (1024 * 1024),
      macrotasks_in_network,
      macrotasks_in_compute,
      cpu_system / 8.0,
      macrotasks_in_disk,
      xvdf_read_throughput,
      xvdf_write_throughput,
      xvdb_read_throughput,
      xvdb_write_throughput,
      xvdf_running_disk_monotasks,
      xvdb_running_disk_monotasks]
    write_data(out_file, data)
  out_file.close()

  subprocess.check_call("gnuplot %s" % utilization_plot_filename, shell=True)
  subprocess.check_call("gnuplot %s" % monotasks_plot_filename, shell=True)
  subprocess.check_call("gnuplot %s" % disk_plot_filename, shell=True)

  if (open_graphs):
    subprocess.check_call("open %s_utilization.pdf" % filename, shell=True)
    subprocess.check_call("open %s_monotasks.pdf" % filename, shell=True)
    subprocess.check_call("open %s_disk_utilization.pdf" % filename, shell=True)

def parse_args():
  parser = argparse.ArgumentParser(description="Plots Spark continuous monitor logs.")
  parser.add_argument(
    "-f", "--filename", help="The path to a continuous monitor log file.", required=True)
  parser.add_argument(
    "-o",
    "--open-graphs",
    action="store_true",
    default=False,
    dest="open_graphs",
    help="Whether to open the resulting graph PDFs.")
  return parser.parse_args()

def main():
  args = parse_args()
  plot_continuous_monitor(args.filename, args.open_graphs)

if __name__ == "__main__":
  main()
