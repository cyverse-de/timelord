package queries

import "database/sql"

// RunningJobsQuery is a query that returns information about a running job that
// should be useful for killing jobs that have gone over their time limit.
const RunningJobsQuery = `
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
  AND j.app_id = a.id::text
  AND j.id = e.job_id
  AND s.app_id = a.id
  AND s.task_id = t.id
  AND t.tool_id = o.id`

// RunningJob represents a job that is in the 'Running' state in the DE
// database.
type RunningJob struct {
	JobID        string
	AppID        string
	InvocationID string
	ToolID       string
	TimeLimit    int
	StartOn      int64
}

// LookupRunningJobs returns a []RunningJob populated with the jobs recorded
// in the DE db that are in the 'Running' state.
func LookupRunningJobs(db *sql.DB) ([]RunningJob, error) {
	var jobs []RunningJob

	rows, err := db.Query(RunningJobsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		j := RunningJob{}
		err = rows.Scan(
			&j.JobID,
			&j.AppID,
			&j.InvocationID,
			&j.ToolID,
			&j.TimeLimit,
			&j.StartOn,
		)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}

	err = rows.Err()
	return jobs, err
}
