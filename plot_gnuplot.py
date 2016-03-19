import inspect
import os
import subprocess


def plot(cm_data, file_prefix, open_graphs):
  """
  Creates gnuplot files that can be used to generate plots with data for
  various continuous monitor attributes, and uses those files to generate PDFs
  of each plot
  """
  # Write continuous monitor data to tab deliminated data file.
  out_filename = '%s_utilization'.format(file_prefix)
  with open(out_filename, 'w') as out_file:
    for data in cm_data:
      write_data(out_file, [val[1] for val in data])

  # Get the location of the monotasks-scripts repository by getting the
  # directory containing the file that is currently being executed.
  scripts_dir = os.path.dirname(inspect.stack()[0][1])
  attributes = ['utilization', 'disk_utilization', 'monotasks', 'memory']

  for attribute in attributes:
    plot_gnuplot_attribute(attribute, file_prefix, open_graphs, scripts_dir)

  plot_single_disk('xvdb', ['xvdf'], file_prefix, open_graphs, scripts_dir,
                   out_filename)
  plot_single_disk('xvdf', ['xvdb'], file_prefix, open_graphs, scripts_dir,
                   out_filename)


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


def plot_single_disk(disk_to_plot, disks_to_skip, file_prefix, open_graphs, scripts_dir, util_filename):
  """
  Plots the utilization for a single disk, ignoring the utilization for any disks in
  disks_to_skip.
  """
  disk_plot_filename_prefix = '{0}_{1}_disk_utilization'.format(file_prefix, disk_to_plot)
  disk_plot_filename = '{0}.gp'.format(disk_plot_filename_prefix)
  disk_plot_output = '{0}.pdf'.format(disk_plot_filename_prefix)
  disk_plot_file = open(disk_plot_filename, 'w')
  for line in open(os.path.join(scripts_dir, 'gnuplot_files/plot_disk_utilization_base.gp'), 'r'):
    skip = False
    for disk_to_skip in disks_to_skip:
      if line.find(disk_to_skip) != -1:
        skip = True
    if not skip:
      new_line = line.replace('__OUT_FILENAME__', disk_plot_output).replace(
        '__NAME__', util_filename)
      disk_plot_file.write(new_line)

  disk_plot_file.close()
  subprocess.check_call('gnuplot {0}'.format(disk_plot_filename), shell=True)
  if open_graphs:
    subprocess.check_call('open {0}'.format(disk_plot_output), shell=True)
  return disk_plot_output


def write_data(out_file, data):
  stringified = [str(x) for x in data]
  out_file.write('\t'.join(stringified))
  out_file.write('\n')
