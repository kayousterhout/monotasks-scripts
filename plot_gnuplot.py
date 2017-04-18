import inspect
from os import path
import subprocess

LINE_TEMPLATE = "\"{}\" using 1:{} with l ls {} title \"{}\""

def plot(cm_data, file_prefix, open_graphs, disk_to_index):
  """
  Creates gnuplot files that can be used to generate plots with data for
  various continuous monitor attributes, and uses those files to generate PDFs
  of each plot
  """
  # Write continuous monitor data to tab deliminated data file.
  out_filename = "{}_utilization".format(file_prefix)
  with open(out_filename, 'w') as out_file:
    for data in cm_data:
      write_data(out_file, [val[1] for val in data])

  # Get the location of the monotasks-scripts repository by getting the
  # directory containing the file that is currently being executed.
  scripts_dir = path.dirname(inspect.stack()[0][1])
  attributes = ['utilization', 'monotasks', 'memory']

  for attribute in attributes:
    plot_gnuplot_attribute(attribute, file_prefix, open_graphs, scripts_dir, disk_to_index)

  for disk_name, index in disk_to_index.iteritems():
    plot_single_disk(disk_name, index, file_prefix, open_graphs, scripts_dir, out_filename)


def plot_gnuplot_attribute(attribute, file_prefix, open_graphs, scripts_dir, disk_to_index):
  """ Create a gnuplot file and associated pdf for a some attribute (like utilization) """
  data_filename = '{}_utilization'.format(file_prefix)
  plot_filename = '{}_{}.gp'.format(file_prefix, attribute)
  base_plot_filename = path.join(scripts_dir, 'gnuplot_files/plot_{}_base.gp'.format(attribute))
  pdf_filename = '{}_{}.pdf'.format(file_prefix, attribute)
  with open(plot_filename, 'w') as plot_file:
    for line in open(base_plot_filename, 'r'):
      new_line = line.replace('__OUT_FILENAME__', pdf_filename).replace('__NAME__', data_filename)
      plot_file.write(new_line)

    color = 6
    if attribute == "utilization":
      for disk, index in sorted(disk_to_index.iteritems()):
        plot_file.write(",\\\n{}".format(LINE_TEMPLATE.format(
          data_filename, index, color, "{} Utilization".format(disk))))
        color += 1

  subprocess.check_call('gnuplot {}'.format(plot_filename), shell=True)
  if open_graphs and attribute != 'memory':
    subprocess.check_call('open {}'.format(pdf_filename), shell=True)


def plot_single_disk(disk_to_plot, start_index, file_prefix, open_graphs, scripts_dir,
                     util_filename):
  """ Plots the utilization for a single disk. """
  disk_plot_filename_prefix = '{}_{}_disk_utilization'.format(file_prefix, disk_to_plot)
  disk_plot_filename = '{}.gp'.format(disk_plot_filename_prefix)
  disk_plot_output = '{}.pdf'.format(disk_plot_filename_prefix)
  with open(disk_plot_filename, 'w') as disk_plot_file:
    for line in open(path.join(scripts_dir, 'gnuplot_files/plot_disk_utilization_base.gp'), 'r'):
      new_line = line.replace('__OUT_FILENAME__', disk_plot_output)
      disk_plot_file.write(new_line)
    # Write lines to plot the information about one disk.
    line_template_y2 = "{} axes x1y2".format(LINE_TEMPLATE)
    disk_plot_file.write("plot ")
    disk_plot_file.write(LINE_TEMPLATE.format(util_filename, start_index, 2, "Utilization"))
    disk_plot_file.write(",\\\n")
    disk_plot_file.write(line_template_y2.format(
      util_filename, start_index + 1, 3, "Read Throughput"))
    disk_plot_file.write(",\\\n")
    disk_plot_file.write(line_template_y2.format(
      util_filename, start_index + 2, 4, "Write Throughput"))
    disk_plot_file.write(",\\\n")
    disk_plot_file.write(LINE_TEMPLATE.format(util_filename, start_index + 3, 5, "Monotasks"))
    disk_plot_file.write(",\\\n")
    disk_plot_file.write(LINE_TEMPLATE.format(
      util_filename, start_index + 4, 7, "Queued Read Monotasks"))
    disk_plot_file.write(",\\\n")
    disk_plot_file.write(
      LINE_TEMPLATE.format(util_filename, start_index + 6, 8, "Queued Write Monotasks"))

  subprocess.check_call('gnuplot {}'.format(disk_plot_filename), shell=True)
  if open_graphs:
    subprocess.check_call('open {}'.format(disk_plot_output), shell=True)
  return disk_plot_output


def write_data(out_file, data):
  stringified = [str(x) for x in data]
  out_file.write('\t'.join(stringified))
  out_file.write('\n')
