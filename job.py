import collections
import logging

import stage

class Job:
  def __init__(self, id):
    self.id = id
    self.logger = logging.getLogger("Job")
    # Map of stage IDs to Stages.
    self.stages = collections.defaultdict(stage.Stage)

  def add_event(self, data):
    event_type = data["Event"]
    if event_type == "SparkListenerTaskEnd":
      stage_id = data["Stage ID"]
      self.stages[stage_id].add_event(data)

  def initialize_job(self):
    """ Should be called after adding all events to the job. """
    # Drop empty stages.
    stages_to_drop = []
    for id, s in self.stages.iteritems():
      if len(s.tasks) == 0:
        stages_to_drop.append(id)
    for id in stages_to_drop:
      print "Dropping stage %s because it is empty" % id
      del self.stages[id]

  def all_tasks(self):
    """ Returns a list of all tasks. """
    return [task for stage in self.stages.values() for task in stage.tasks]

  def print_stage_info(self):
    for id, stage in self.stages.iteritems():
      print "STAGE %s: %s" % (id, stage.verbose_str())

  def print_heading(self, text):
    print "\n******** %s: %s ********" % (self.id, text)

  def runtime(self):
    actual_start_time = min([s.start_time for s in self.stages.values()])
    actual_finish_time = max([s.finish_time() for s in self.stages.values()])
    return actual_finish_time - actual_start_time

  def write_data_to_file(self, data, file_handle, newline=True):
    stringified_data = [str(x) for x in data]
    if newline:
      stringified_data += "\n"
    file_handle.write("\t".join(stringified_data))

  def write_stage_info(self, query_id, prefix):
    f = open("%s_stage_info" % prefix, "a")
    last_stage_runtime = -1
    last_stage_finish_time = 0
    for stage in self.stages.values():
      # This is a hack! Count the most recent stage with runtime > 1s as the "last".
      # Shark produces 1-2 very short stages at the end that do not seem to do anything (and
      # certainly aren't doing the output write we're trying to account for).
      if (stage.finish_time() - stage.start_time) > 1000 and stage.finish_time() > last_stage_finish_time:
        last_stage_finish_time = stage.finish_time()
        last_stage_runtime = stage.finish_time() - stage.start_time

    f.write("%s\t%s\t%s\n" % (query_id, last_stage_runtime, self.original_runtime()))
    f.close()
