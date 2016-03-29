import matplotlib.pyplot as pyplot

from matplotlib.backends.backend_pdf import PdfPages


def continuous_monitor_col(key, continuous_monitor):
  """
  For a given key, returns a list of data from continuous monitor entries
  corresponding to that key.
  """
  return [data[key] for data in continuous_monitor]


def plot(cm_data, file_prefix, open_graphs):
  disk_utilization_params = ['xvdf utilization',
                             'xvdb utilization',
                             'xvdf write throughput',
                             'xvdf read throughput',
                             'xvdb write throughput',
                             'xvdb read throughput']
  memory_params = ['free heap memory',
                   'free off heap memory']
  monotasks_params = ['local running macrotasks',
                      'macrotasks in network',
                      'macrotasks in compute',
                      'macrotasks in disk',
                      'running macrotasks',
                      'gc fraction',
                      'outstanding network bytes']
  utilization_params = ['cpu utilization',
                        'xvdf utilization',
                        'xvdb utilization',
                        'cpu system',
                        'gc fraction']
  xvdf_params = ['xvdf running disk monotasks',
                 'xvdf write throughput',
                 'xvdf read throughput',
                 'xvdf utilization']
  xvdb_params = ['xvdb running disk monotasks',
                 'xvdb write throughput',
                 'xvdb read throughput',
                 'xvdb utilization']

  def plot_params(params_to_plot, title):
    """
    Creates a matplotlib graph using continuous monitor data.
    Time is the x axis and data corresponding to each parameter is used to
    generate a new line on the line graph.
    """
    handles = []
    time = continuous_monitor_col('time', cm_data)
    for key in params_to_plot:
      handle = pyplot.plot(time, continuous_monitor_col(key, cm_data),
                           label=key)[0]
      handles.append(handle)
    pyplot.legend(handles)
    pyplot.title(title)
    pdf.savefig()
    if open_graphs:
      pyplot.show()
    else:
      pyplot.close()

  with PdfPages('{0}_graphs.pdf'.format(file_prefix)) as pdf:
    plot_params(disk_utilization_params,
                'Disk Utilization')
    plot_params(memory_params,
                'Memory')
    plot_params(monotasks_params,
                'Monotasks')
    plot_params(utilization_params,
                'Utilization')
    plot_params(xvdb_params,
                'xvdb Utilization')
    plot_params(xvdf_params,
                'xvdf Utilization')
