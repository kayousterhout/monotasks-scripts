# Monotasks Experiment Scripts

This repository contains scripts used for parsing the results of
monotasks experiments.

The following files provide functionality for parsing log files:
- `parse_event_logs.py`: This is the main gateway for log parsing.
It includes an Analyzer class that reads all of the information
from a particular log file, and that includes functions to output
information about the jobs, including the utilization while it was
running, information about load balancing across executors, information
about runtimes and ideal runtimes, and more.
- `job.py`: Handles parsing information about a particular job
- `stage.py`: Handles stage-specific information
- `task.py`: Handles parsing information about a particular task
- `metrics.py`: Functionality for parsing information about the
utilization on executors while jobs were running.

The following scripts are commonly used for experiment parsing:
- `parse_sort.py`: Uses the Analyzer class in `parse_event_logs.py`
to output information about the map and reduce stages of a sort
workload.
- `plot_bdb.py` and `plot_bdb_estimates.py` plot box and whiskers
plots with the results of the big data benchmark experiments.
- `plot_continuous_monitor.py`: Plots the utilization and number
of tasks over time, using the continuous monitor file.
- `plot_num_threads_per_disk.py`: Makes a graph comparing job 
completion time to the number of threads per disk. Assumes the
data was generated with the `run_basic_disk_job.py` script
in the monotasks experiments directory.

In addition, this repository contains the following scripts:
- `copy_continuous_monitor.py`: Provides command-line access to
`utils.copy_latest_continuous_monitor`, which copies a continuous
monitor back from a worker machine.
- `copy_logs.py`: Copies an event log from a Spark master and
a continuous monitor from a single worker from a cluster back to
the local machine.
- `filter_single_stage_jobs.py`: A filter to be used with an
Analyzer that filters out all single-stage jobs
- `make_utilization_box_whiskers.py`: Makes a box-and-whiskers plot
of the utilization during an experiment, based on data output
by parse_experiment_logs.py.
- `parse_shuffle_jobs.py`: Uses `parse_event_logs.py` to parse
shuffle-only jobs.
- `parse_vary_network_concurrency.py`: Uses the Analyzer class
to generate graphs based on 5 experiments that vary the network
concurrency.
- `parse_vary_num_tasks.py`: Similar to the above, except
parses experiments that vary the number of total tasks a
job is broken into.
- `plot_gnuplot.py` and `plot_matplotlib.py`: Plotting functionality
for generating gnuplot and matplotlib plots, respectively.
- `plot_monotask_times.py`: Makes a waterfall graph of when
monotasks started and ended.
- `plot_vary_num_tasks_simple.py`: More sophisticated functionality
for creating a graph of the completion time of a job relative
to the number of tasks a job was broken into.
- `shuffle_job_filterer.py`: Filters out only jobs with more than
5 tasks and exactly 1 stage.
- `sort_experiment_utilization.py`: Creates CDF data of the utilization
while a job was running (out of date).
