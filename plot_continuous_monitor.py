import json
import optparse
import subprocess
import sys

def write_data(out_file, data):
  stringified = [str(x) for x in data]
  out_file.write("\t".join(stringified))
  out_file.write("\n")

def plot_continuous_monitor(filename):
  out_filename = "%s_utilization" % filename
  out_file = open(out_filename, "w")

  # Write a plot file.
  plot_filename = "%s_utilization.gp" % filename
  plot_file = open(plot_filename, "w")
  for line in open("plot_cdf_base.gp", "r"):
    new_line = line.replace("__NAME__", out_filename)
    plot_file.write(new_line)
  plot_file.close()
 
  start = -1
  for (i, line) in enumerate(open(filename, "r")):
    try:
      json_data = json.loads(line)
    except ValueError:
      # This typically happens at the end of the file, which can get cutoff when the job stops.
      print "Stopping parsing due to incomplete line"
      break
    time = json_data["Current Time"]
    if start == -1:
      start = time
    disk_utilization = json_data["Disk Utilization"]
    xvdf_total_utilization = disk_utilization[0]["xvdf"]["Disk Utilization"]
    xvdb_total_utilization = disk_utilization[1]["xvdb"]["Disk Utilization"]
    cpu_utilization = json_data["Cpu Utilization"]
    cpu_total = (cpu_utilization["Total User Utilization"] +
      cpu_utilization["Total System Utilization"])
    network_utilization = json_data["Network Utilization"]
    bytes_received = network_utilization["Bytes Received Per Second"]
    running_compute_monotasks = 0
    if "Running Compute Monotasks" in json_data:
      running_compute_monotasks = json_data["Running Compute Monotasks"] / 8.0
    running_macrotasks = 0
    if "Running Macrotasks" in json_data:
      running_macrotasks = json_data["Running Macrotasks"] / 8.0
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
      outstanding_network_bytes / (1024 * 1024)]
    write_data(out_file, data)
  out_file.close()

  subprocess.check_call("gnuplot %s" % plot_filename, shell=True)
  subprocess.check_call("open %s_utilization.pdf" % filename, shell=True)

def main():
  parser = optparse.OptionParser(usage="foo.py <log filename")
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)

  filename = args[0]
  plot_continuous_monitor(filename)

if __name__ == "__main__":
  main()
