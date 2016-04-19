import matplotlib.pyplot as pyplot

from matplotlib.backends import backend_pdf


def continuous_monitor_col(continuous_monitor, key):
  """
  For a given key, returns a list of data from continuous monitor entries
  corresponding to that key.
  """
  return [data[key] for data in continuous_monitor]


def plot(cm_data, file_prefix, open_graphs, disks):
  disk_utilization_params = ['{0} utilization'.format(disk) for disk in disks]
  disk_params = (
    disk_utilization_params +
    ['{0} write throughput'.format(disk) for disk in disks] +
    ['{0} read throughput'.format(disk) for disk in disks]
  )
  memory_params = [
    'free heap memory',
    'free off heap memory'
  ]
  monotasks_params = [
    'local running macrotasks',
    'macrotasks in network',
    'macrotasks in compute',
    'macrotasks in disk',
    'running macrotasks',
    'gc fraction',
    'outstanding network bytes'
  ]
  utilization_params = [
    'cpu utilization',
    'bytes received',
    'bytes transmitted',
    'cpu system',
    'gc fraction'
  ] + disk_utilization_params

  def plot_params(params_to_plot, title):
    """
    Creates a matplotlib graph using continuous monitor data.
    Time is the x axis and data corresponding to each parameter is used to
    generate a new line on the line graph.
    """
    pyplot.figure(title)
    pyplot.title(title)
    pyplot.grid(b=True, which='both')
    times = continuous_monitor_col(cm_data, key='time')
    for key in params_to_plot:
      pyplot.plot(times, continuous_monitor_col(cm_data, key), label=key)
    legend = pyplot.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    pdf_filepath = '{0}_{1}_graphs.pdf'.format(file_prefix, title.lower().replace(' ', '_'))
    with backend_pdf.PdfPages(pdf_filepath) as pdf:
      pdf.savefig(additional_artists=[legend], bbox_inches='tight')

  plot_params(disk_params, title='Disk Utilization')
  plot_params(memory_params, title='Memory')
  plot_params(monotasks_params, title='Monotasks')
  plot_params(utilization_params, title='Utilization')

  for disk in disks:
    disk_params = ['{0} running disk monotasks'.format(disk),
                   '{0} write throughput'.format(disk),
                   '{0} read throughput'.format(disk),
                   '{0} utilization'.format(disk)]
    plot_params(disk_params, title='{0} Utilization'.format(disk))

  if open_graphs:
    pyplot.show()
