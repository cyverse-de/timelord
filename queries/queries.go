package queries

// RunningJobs returns a list of the jobs that are in a running state.
const RunningJobs = `
SELECT
  j.id job_id,
  j.app_id app_id,
  e.external_id invocation_id,
  o.id tool_id,
  o.time_limit_seconds time_limit_seconds,
  (SELECT
    sent_on
  FROM
    job_status_updates
  WHERE
    external_id = e.external_id
    AND status = 'Running'
  ORDER BY
    sent_on asc
  LIMIT 1) sent_on
FROM
  jobs j,
  job_steps e,
  apps a,
  app_steps s,
  tasks t,
  tools o
WHERE
  j.status = 'Running'
  AND j.app_id::uuid = a.id
  AND j.id = e.job_id
  AND s.app_id = a.id
  AND s.task_id = t.id
  AND t.tool_id = o.id`
