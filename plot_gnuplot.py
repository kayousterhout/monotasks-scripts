import inspect
import os
import subprocess


def plot(cm_data, file_prefix, open_graphs, disk_to_index):
  """
  Creates gnuplot files that can be used to generate plots with data for
  various continuous monitor attributes, and uses those files to generate PDFs
  of each plot
  """
  # Write continuous monitor data to tab deliminated data file.
  out_filename = "%s_utilization" % file_prefix
  with open(out_filename, 'w') as out_file:
    for data in cm_data:
      write_data(out_file, [val[1] for val in data])

  # Get the location of the monotasks-scripts repository by getting the
  # directory containing the file that is currently being executed.
  scripts_dir = os.path.dirname(inspect.stack()[0][1])
  attributes = ['utilization', 'monotasks', 'memory']

  for attribute in attributes:
    plot_gnuplot_attribute(attribute, file_prefix, open_graphs, scripts_dir)

  for disk_name, index in disk_to_index.iteritems():
    plot_single_disk(disk_name, index, file_prefix, open_graphs, scripts_dir, out_filename)


def plot_gnuplot_attribute(attribute, file_prefix, open_graphs, scripts_dir):
  """ Create a gnuplot file and associated pdf for a some attribute (like disk_utilization) """
  data_filename = '{0}_utilization'.format(file_prefix)
  plot_filename = '{0}_{1}.gp'.format(file_prefix, attribute)
  pdf_filename = '{0}_{1}.pdf'.format(file_prefix, attribute)
  plot_file = open(plot_filename, 'w')

  for line in open(os.path.join(scripts_dir, 'gnuplot_files/plot_{0}_base.gp'.format(attribute)), 'r'):
    new_line = line.replace('__OUT_FILENAME__', pdf_filename).replace('__NAME__', data_filename)
    plot_file.write(new_line)
  plot_file.close()

  subprocess.check_call('gnuplot {0}'.format(plot_filename), shell=True)
  if open_graphs:
    subprocess.check_call('open {0}'.format(pdf_filename), shell=True)


def plot_single_disk(disk_to_plot, start_index, file_prefix, open_graphs, scripts_dir,
                     util_filename):
  """ Plots the utilization for a single disk. """
  disk_plot_filename_prefix = '{0}_{1}_disk_utilization'.format(file_prefix, disk_to_plot)
  disk_plot_filename = '{0}.gp'.format(disk_plot_filename_prefix)
  disk_plot_output = '{0}.pdf'.format(disk_plot_filename_prefix)
  disk_plot_file = open(disk_plot_filename, 'w')
  for line in open(os.path.join(scripts_dir, 'gnuplot_files/plot_disk_utilization_base.gp'), 'r'):
      new_line = line.replace('__OUT_FILENAME__', disk_plot_output)
      disk_plot_file.write(new_line)
  # Write lines to plot the information about one disk.
  line_template_y1 = "\"%s\" using 1:%d with l ls %d title \"%s\""
  line_template_y2 = "%s axes x1y2" % line_template_y1
  disk_plot_file.write("plot ")
  disk_plot_file.write(line_template_y1 % (util_filename, start_index, 2, "Utilization"))
  disk_plot_file.write(",\\\n")
  disk_plot_file.write(line_template_y2 % (util_filename, start_index + 1, 3, "Read Throughput"))
  disk_plot_file.write(",\\\n")
  disk_plot_file.write(line_template_y2 % (util_filename, start_index + 2, 4, "Write Throughput"))
  disk_plot_file.write(",\\\n")
  disk_plot_file.write(line_template_y1 % (util_filename, start_index + 3, 5, "Monotasks"))

  disk_plot_file.close()
  subprocess.check_call('gnuplot {0}'.format(disk_plot_filename), shell=True)
  if open_graphs:
    subprocess.check_call('open {0}'.format(disk_plot_output), shell=True)
  return disk_plot_output


def write_data(out_file, data):
  stringified = [str(x) for x in data]
  out_file.write('\t'.join(stringified))
  out_file.write('\n')
