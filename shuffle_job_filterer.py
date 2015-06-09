def filter(all_jobs_dict):
  sorted_jobs = sorted(all_jobs_dict.iteritems())
  filtered_jobs = []
  for job_id, job in sorted_jobs:
    if len(job.stages) > 1:
      continue
    for stage_id, stage in job.stages.iteritems():
      if len(stage.tasks) > 5:
        filtered_jobs.append((job_id, job))
        continue
  return {k:v for (k,v) in filtered_jobs[-9:]}
