import sys

# Add this if the milliseconds are included in the log output.
offset = 0

input_filename = sys.argv[1]
LINE_TEMPLATE = ("set arrow from {start_x},{y} to {end_x},{y} ls {style} nohead\n" +
  "set label \"{runtime} ({macrotask_id})\" at {end_x},{y} left font 'Times,5'\n")

def write_stage_data(stage_data, plot_file):
  # Sort the monotasks by start time so that they plot more nicely.
  stage_data.sort(key = lambda entry: entry[1])
  for (current_y, (line_style, start_millis, runtime_millis, macrotask_id)) in enumerate(stage_data):
    plot_file.write(LINE_TEMPLATE.format(
      start_x = start_millis,
      end_x = start_millis + runtime_millis,
      runtime = runtime_millis,
      macrotask_id = macrotask_id,
      y = current_y,
      style = line_style))

def get_first_start(data_filename):
  with open(data_filename, "r") as data_file:
    line = data_file.readline()
    items = line.split(" ")
    return int(items[offset + 19][:-3])

def write_monotask_times(data_filename, plot_file, first_start):
  max_y = 0
  current_stage = 0
  last_end = 0
  first_task_id = 0
  last_network_monotask_end = 0
  # TODO: Keep track of when the last network monotask ended, so we can just plot the delta.
  with open(data_filename, "r") as data_file:
    # Line style, start time, runtime, and macrotask id for each monotask in the stage.
    stage_data = []
    for line in data_file:
      items = line.split(" ")
      start_millis = int(items[offset + 19][:-3])

      runtime_millis = int(items[offset + 15])
      macrotask_id = int(items[offset + 9])

      if line.find("network") != -1 and line.find("NetworkResponse") == -1:
        line_style = 1
        finish_time = start_millis + runtime_millis
        # This is the old code -- but with the zero runtime monotasks (the ones that got
        # short-circuited) it doesn't work correctly.
        #start_millis = max(start_millis, last_network_monotask_end)
        runtime_millis = finish_time - start_millis
        last_network_monotask_end = finish_time
        # Use the reduce ID as the macrotask ID, so it's easier to correlate them.
        macrotask_id = int(items[offset + 12].strip("}").strip(")"))
      elif line.find("compute") != -1:
        line_style = 2

        # Only roll over the stage for compute monotasks, because of complexities with the
        # pipelined network ones.
        stage = int(items[offset + 12].strip(")").strip("}"))
        if current_stage != stage:
          write_stage_data(stage_data, plot_file)
          max_y = max(max_y, len(stage_data))
          stage_data = []
          first_task_id = macrotask_id
          current_stage = stage

      else:
        continue
      stage_data.append([
        line_style,
        start_millis - first_start,
        runtime_millis,
        macrotask_id])
      last_end = max(last_end, start_millis + runtime_millis - first_start)

    # Don't forget to plot the last stage!
    write_stage_data(stage_data, plot_file)

  return (last_end, max_y)

first_start = get_first_start(input_filename)

# Write the basic information to generate the waterfall plot.
plot_prefix = "{}_monotask_waterfall".format(input_filename)
# Write the template.
plot_file = open("{}.gp".format(plot_prefix), "w")
with open("gnuplot_files/waterfall_base.gp", "r") as base_file:
  for line in base_file:
    plot_file.write(line)

plot_file.write("set output \"{}.pdf\"\n".format(plot_prefix))

# Write one line for each monotask.
last_end, max_y = write_monotask_times(input_filename, plot_file, first_start)

plot_file.write("set xrange [0:{}]\n".format(last_end))
plot_file.write("set yrange [0:{}]\n".format(max_y))
plot_file.write("plot -1 ls 2 title \"Compute\", -1 ls 1 title \"Network\"\n")

plot_file.close()

