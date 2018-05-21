package queries

import "database/sql"

// RunningJobsQuery is a query that returns information about a running job that
// should be useful for killing jobs that have gone over their time limit.
const RunningJobsQuery = `
SELECT
  e.external_id             invocation_id,
  r.username                username,
  j.job_name                analysis_name,
  j.job_description         analysis_description,
  j.status                  analysis_status,
  j.start_date              analysis_start_date,
  j.result_folder_path      analysis_result_folder_path,
  SUM(o.time_limit_seconds) time_limit_seconds,
  MIN(u.sent_on)            sent_on
FROM
  jobs j
  JOIN users r              ON j.user_id = r.id
  JOIN job_steps e          ON j.id = e.job_id
  JOIN apps a               ON j.app_id = a.id::text
  JOIN app_steps s          ON a.id = s.app_id
  JOIN tasks t              ON s.task_id = t.id
  JOIN tools o              ON t.tool_id = o.id
  JOIN job_status_updates u ON u.external_id = e.external_id
WHERE
  j.status ILIKE 'Running'
  AND u.status ILIKE 'Running'
GROUP BY
  j.id,
  j.app_id,
  r.username,
  e.external_id
HAVING
  BOOL_OR(o.interactive) = TRUE`

// RunningJob represents a job that is in the 'Running' state in the DE
// database.
type RunningJob struct {
	InvocationID             string
	Username                 string
	AnalysisName             string
	AnalysisDescription      string
	AnalysisStatus           string
	AnalysisStartDate        string
	AnalysisResultFolderPath string
	TimeLimit                int
	StartOn                  int64
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
			&j.InvocationID,
			&j.Username,
			&j.AnalysisName,
			&j.AnalysisDescription,
			&j.AnalysisStatus,
			&j.AnalysisStartDate,
			&j.AnalysisResultFolderPath,
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
