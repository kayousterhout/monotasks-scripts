from task import Task

class Stage:
  def __init__(self):
    self.start_time = -1
    self.tasks = []

  def average_task_runtime(self):
    return sum([t.runtime() for t in self.tasks]) * 1.0 / len(self.tasks)

  def __str__(self):
    max_task_runtime = max([t.runtime() for t in self.tasks])
    if self.tasks[0].has_fetch:
      input_method = "shuffle"
    else:
      input_method = self.tasks[0].input_read_method
    return (("%s tasks (avg runtime: %s, max runtime: %s) Start: %s, runtime: %s, "
      "Max concurrency: %s, "
      "Input MB: %s (from %s), Output MB: %s, Straggers: %s, Progress rate straggers: %s, "
      "Progress rate stragglers explained by scheduler delay (%s), HDFS read (%s), "
      "HDFS and read (%s), GC (%s), Network (%s), JIT (%s), output rate stragglers: %s") %
      (len(self.tasks), self.average_task_runtime(), max_task_runtime, self.start_time,
       self.finish_time() - self.start_time, concurrency.get_max_concurrency(self.tasks),
       self.input_mb(), input_method, self.output_mb(),
       self.traditional_stragglers(), self.progress_rate_stragglers()[0],
       self.scheduler_delay_stragglers()[0], self.hdfs_read_stragglers()[0],
       self.hdfs_read_and_scheduler_delay_stragglers()[0], self.gc_stragglers()[0],
       # Do not compute the JIT stragglers here! Screws up the calculation.
       self.network_stragglers()[0], -1,
       self.output_progress_rate_stragglers()[0]))

  def verbose_str(self):
    # Get info about the longest task.
    max_index = -1
    max_runtime = -1
    for i, task in enumerate(self.tasks):
      if task.runtime() > max_runtime:
        max_runtime = task.runtime()
        max_index = i
    return "%s\n    Longest Task: %s" % (self, self.tasks[i])    

  def load_balancing_badness(self):
    executor_id_to_tasks = {}
    for task in self.tasks:
      if task.executor_id not in executor_id_to_tasks:
        executor_id_to_tasks[task.executor_id] = []
      executor_id_to_tasks[task.executor_id].append(task)

    total_time = 0
    for executor_id, tasks in executor_id_to_tasks.iteritems():
      min_start_time = min([t.start_time for t in tasks])
      max_finish_time = max([t.finish_time for t in tasks])
      total_time += max_finish_time - min_start_time

    ideal_time = total_time / len(executor_id_to_tasks)
    return float(self.runtime()) / ideal_time

  def runtime(self):
    return self.finish_time() - self.start_time

  def has_shuffle_read(self):
    total_shuffle_read_bytes = sum(
      [t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
    return total_shuffle_read_bytes > 0

  def finish_time(self):
    return max([t.finish_time for t in self.tasks])

  def total_runtime(self):
    return sum([t.finish_time - t.start_time for t in self.tasks])

  def input_mb(self):
    """ Returns the total input size for this stage.
    
    This is only valid if the stage read data from a shuffle.
    """
    total_input_bytes = sum([t.remote_mb_read + t.local_mb_read for t in self.tasks if t.has_fetch])
    total_input_bytes += sum([t.input_mb for t in self.tasks])
    return total_input_bytes

  def output_mb(self):
    """ Returns the total output size for this stage.

    This is only valid if the output data was written for a shuffle.
    TODO: Add HDFS / in-memory RDD output size.
    """
    total_output_size = sum([t.shuffle_mb_written for t in self.tasks])
    return total_output_size

  def add_event(self, data):
    task = Task(data)

    if self.start_time == -1:
      self.start_time = task.start_time
    else:
      self.start_time = min(self.start_time, task.start_time)

    self.tasks.append(task)




