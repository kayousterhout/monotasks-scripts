This repository contains scripts for parsing monotasks experiment
results. In general, the `copy_logs.py` script should be sufficient
for basic experiments; that script copies logs back from a Spark
driver (where the event log is copied from, which summarizes all tasks)
and one of the Spark executors (where the continuous monitor is copied
from, which periodically logs utilization information on the machine).
The `copy_logs.py` script automatically generates a graph of utilization
as a function of time, and outputs files describing the distribution
of utilization for the last job that ran in the experiment. To see
how to use the script, run:

    python copy_logs.py --help
