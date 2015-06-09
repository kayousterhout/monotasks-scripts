"""
This file copies logs from a remote cluster and parses the logs, assuming that they are from a job
that performs shuffles.
"""

import copy_logs
import parse_event_logs
import shuffle_job_filterer

def main(argv):
  (local_event_log_file, continuous_monitor_file) = copy_logs.copy_logs(argv)
  analyzer = parse_event_logs.Analyzer(local_event_log_file, shuffle_job_filterer.filter)
  analyzer.output_utilizations(local_event_log_file)
  analyzer.output_load_balancing_badness(local_event_log_file)
  analyzer.output_runtimes(local_event_log_file)

if __name__ == "__main__":
  main(sys.argv[1:])
