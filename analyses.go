package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/cyverse-de/messaging/v9"
	pq "github.com/lib/pq"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// TimestampFromDBFormat is the format of the timestamps retrieved from the
// database through the GraphQL server. Shouldn't have timezone info.
const TimestampFromDBFormat = "2006-01-02T15:04:05"

// Job contains the information about an analysis that we're interested in.
type Job struct {
	ID             string `json:"id"`
	AppID          string `json:"app_id"`
	UserID         string `json:"user_id"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Name           string `json:"name"`
	ResultFolder   string `json:"result_folder"`
	StartDate      string `json:"start_date"`
	PlannedEndDate string `json:"planned_end_date"`
	Subdomain      string `json:"subdomain"`
	Type           string `json:"type"`
	User           string `json:"user"`
	ExternalID     string `json:"external_id"`
}

// getJobDuration takes a job and returns a duration string and the start time of the job
func getJobDuration(j *Job) (string, time.Time, error) {
	starttime, err := time.ParseInLocation(TimestampFromDBFormat, j.StartDate, time.Local)
	if err != nil {
		return "", starttime, errors.Wrapf(err, "failed to parse start date %s", j.StartDate)
	}

	// just print H(HH):MM format
	dur := time.Since(starttime).Round(time.Minute)
	h := dur / time.Hour
	dur -= h * time.Hour
	m := dur / time.Minute
	return fmt.Sprintf("%d:%02d", h, m), starttime, nil
}

const jobsToKillQuery = `
select jobs.id,
       jobs.app_id,
       jobs.user_id,
       jobs.status,
       jobs.job_description,
       jobs.job_name,
       jobs.result_folder_path,
       jobs.planned_end_date,
       jobs.start_date,
       job_types.system_id,
       users.username
  from jobs
  join job_types on jobs.job_type_id = job_types.id
  join users on jobs.user_id = users.id
 where jobs.status = $1
   and jobs.planned_end_date <= $2`

const externalIDsQuery = `
select job_steps.external_id
  from job_steps
 where job_steps.job_id = $1
 limit 1`

func getExternalID(ctx context.Context, dedb *sql.DB, jobID string) (string, error) {
	var (
		err        error
		row        *sql.Row
		externalID string
	)

	row = dedb.QueryRowContext(
		ctx,
		externalIDsQuery,
		jobID,
	)
	if err = row.Scan(&externalID); err != nil {
		return "", err
	}

	return externalID, err
}

// JobsToKill returns a list of running jobs that are past their expiration date
// and can be killed off. 'api' should be the base URL for the analyses service.
func JobsToKill(ctx context.Context, dedb *sql.DB) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	if rows, err = dedb.QueryContext(
		ctx,
		jobsToKillQuery,
		"Running",
		time.Now().Format("2006-01-02 15:04:05.000000-07"),
	); err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}

	for rows.Next() {
		var (
			job            Job
			startDate      pq.NullTime
			plannedEndDate pq.NullTime
		)

		job = Job{}

		if err = rows.Scan(
			&job.ID,
			&job.AppID,
			&job.UserID,
			&job.Status,
			&job.Description,
			&job.Name,
			&job.ResultFolder,
			&plannedEndDate,
			&startDate,
			&job.Type,
			&job.User,
		); err != nil {
			return nil, err
		}

		if plannedEndDate.Valid {
			job.PlannedEndDate = plannedEndDate.Time.Format(TimestampFromDBFormat)
		}

		if startDate.Valid {
			job.StartDate = startDate.Time.Format(TimestampFromDBFormat)
		}

		job.ExternalID, err = getExternalID(ctx, dedb, job.ID)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

const periodicWarningsQuery = `
SELECT jobs.id,
       jobs.app_id,
       jobs.user_id,
       jobs.status,
       jobs.job_description,
       jobs.job_name,
       jobs.result_folder_path,
       jobs.planned_end_date,
       jobs.start_date,
       job_types.system_id,
       users.username
  FROM jobs
  JOIN job_types on jobs.job_type_id = job_types.id
  JOIN users on jobs.user_id = users.id
  LEFT join notif_statuses ON jobs.id = notif_statuses.analysis_id
 WHERE jobs.status = $1
   AND (notif_statuses.last_periodic_warning is null
    OR notif_statuses.last_periodic_warning < now() - coalesce(notif_statuses.periodic_warning_period, '4 hours'::interval))
`

// JobPeriodicWarnings returns a list of running jobs that may need periodic notifications to be sent
func JobPeriodicWarnings(ctx context.Context, dedb *sql.DB) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	if rows, err = dedb.QueryContext(
		ctx,
		periodicWarningsQuery,
		"Running",
	); err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}

	for rows.Next() {
		var (
			job            Job
			startDate      pq.NullTime
			plannedEndDate pq.NullTime
		)

		job = Job{}

		if err = rows.Scan(
			&job.ID,
			&job.AppID,
			&job.UserID,
			&job.Status,
			&job.Description,
			&job.Name,
			&job.ResultFolder,
			&plannedEndDate,
			&startDate,
			&job.Type,
			&job.User,
		); err != nil {
			return nil, err
		}

		if plannedEndDate.Valid {
			job.PlannedEndDate = plannedEndDate.Time.Format(TimestampFromDBFormat)
		}

		if startDate.Valid {
			job.StartDate = startDate.Time.Format(TimestampFromDBFormat)
		}

		job.ExternalID, err = getExternalID(ctx, dedb, job.ID)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

const jobWarningsQuery = `
select jobs.id,
       jobs.app_id,
       jobs.user_id,
       jobs.status,
       jobs.job_description,
       jobs.job_name,
       jobs.result_folder_path,
       jobs.planned_end_date,
       jobs.start_date,
       job_types.system_id,
       users.username
  from jobs
  join job_types on jobs.job_type_id = job_types.id
  join users on jobs.user_id = users.id
 where jobs.status = $1
   and jobs.planned_end_date > $2
   and jobs.planned_end_date <= $3
`

// JobKillWarnings returns a list of running jobs that are set to be killed
// within the number of minutes specified. 'api' should be the base URL for the
// analyses service.
func JobKillWarnings(ctx context.Context, dedb *sql.DB, minutes int64) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	now := time.Now()
	// fmtstring := "2006-01-02 15:04:05.000000-07"
	// nowtimestamp := now.Format(fmtstring)
	// futuretimestamp := now.Add(time.Duration(minutes) * time.Minute).Format(fmtstring)

	if rows, err = dedb.QueryContext(
		ctx,
		jobWarningsQuery,
		"Running",
		now,
		now.Add(time.Duration(minutes)*time.Minute),
	); err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}

	for rows.Next() {
		var (
			job            Job
			startDate      pq.NullTime
			plannedEndDate pq.NullTime
		)

		job = Job{}

		if err = rows.Scan(
			&job.ID,
			&job.AppID,
			&job.UserID,
			&job.Status,
			&job.Description,
			&job.Name,
			&job.ResultFolder,
			&plannedEndDate,
			&startDate,
			&job.Type,
			&job.User,
		); err != nil {
			return nil, err
		}

		if plannedEndDate.Valid {
			job.PlannedEndDate = plannedEndDate.Time.Format(TimestampFromDBFormat)
		}

		if startDate.Valid {
			job.StartDate = startDate.Time.Format(TimestampFromDBFormat)
		}

		job.ExternalID, err = getExternalID(ctx, dedb, job.ID)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return jobs, nil
}

// JobKiller is responsible for killing jobs either in HTCondor or in K8s.
type JobKiller struct {
	K8sEnabled     bool   // whether or not the VICE apps are running k8s
	AppsBase       string // base URL for the apps service
	AppExposerBase string // base URL for the app-exposer serivce
}

// KillJob uses either the apps or app-exposer APIs to kill a VICE job.
func (j *JobKiller) KillJob(ctx context.Context, dedb *sql.DB, job *Job) error {
	if j.K8sEnabled {
		return j.killK8sJob(ctx, dedb, job)
	}
	return j.killCondorJob(ctx, job.ID, job.User)

}

// killCondorJob uses the provided API at the base URL to kill a running job. This
// will probably be to the apps service. jobID should be the UUID for the Job,
// typically returned in the ID field by the analyses service. The username
// should be the short username for the user that launched the job.
func (j *JobKiller) killCondorJob(ctx context.Context, jobID, username string) error {
	apiURL, err := url.Parse(j.AppsBase)
	if err != nil {
		return err
	}

	apiURL.Path = filepath.Join(apiURL.Path, "analyses", jobID, "stop")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL.String(), nil)
	if err != nil {
		return err
	}

	var shortusername string
	userparts := strings.Split(username, "@")
	if len(userparts) > 1 {
		shortusername = userparts[0]
	} else {
		shortusername = username
	}
	q := req.URL.Query()
	q.Add("user", shortusername)
	req.URL.RawQuery = q.Encode()

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("response status code for POST %s was %d as %s", apiURL.String(), resp.StatusCode, username)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.Infof("response from %s was: %s", req.URL, string(body))
	return nil
}

// killK8sJob uses the app-exposer API to make a job save its outputs and exit.
// JobID should be the external_id (AKA invocationID) for the job.
func (j *JobKiller) killK8sJob(ctx context.Context, dedb *sql.DB, job *Job) error {
	var err error

	origAPIURL, err := url.Parse(j.AppExposerBase)
	if err != nil {
		return err
	}

	externalID := job.ExternalID

	var apiURL *url.URL
	apiURL, err = url.Parse(origAPIURL.String()) // lol
	if err != nil {
		return errors.Wrapf(err, "error parsing URL %s while processing external-id %s", origAPIURL.String(), externalID)
	}

	apiURL.Path = filepath.Join(apiURL.Path, "vice", externalID, "save-and-exit")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL.String(), nil)
	if err != nil {
		return errors.Wrapf(err, "error creating save-and-exit request for external-id %s", externalID)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "error calling save-and-exit for external-id %s", externalID)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("response status code for POST %s was %d", apiURL.String(), resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "error reading response body of save-and-exit call for external-id %s", externalID)
	}

	log.Infof("response from %s was: %s", req.URL, string(body))

	resp.Body.Close()

	return nil
}

const jobByExternalIDQuery = `
select jobs.id,
       jobs.app_id,
       jobs.user_id,
       jobs.status,
       jobs.job_description,
       jobs.job_name,
       jobs.result_folder_path,
       jobs.planned_end_date,
       jobs.subdomain,
       jobs.start_date,
       job_types.system_id,
       users.username,
       job_steps.external_id
  from jobs
  join job_types on jobs.job_type_id = job_types.id
  join users on jobs.user_id = users.id
  join job_steps on jobs.id = job_steps.job_id
 where job_steps.external_id = $1`

func lookupByExternalID(ctx context.Context, dedb *sql.DB, externalID string) (*Job, error) {
	var (
		err            error
		job            *Job
		subdomain      sql.NullString
		startDate      pq.NullTime
		plannedEndDate pq.NullTime
	)

	job = &Job{}

	if err = dedb.QueryRowContext(ctx, jobByExternalIDQuery, externalID).Scan(
		&job.ID,
		&job.AppID,
		&job.UserID,
		&job.Status,
		&job.Description,
		&job.Name,
		&job.ResultFolder,
		&plannedEndDate,
		&subdomain,
		&startDate,
		&job.Type,
		&job.User,
		&job.ExternalID,
	); err != nil {
		return nil, err
	}
	if plannedEndDate.Valid {
		job.PlannedEndDate = plannedEndDate.Time.Format(TimestampFromDBFormat)
	}
	if startDate.Valid {
		job.StartDate = startDate.Time.Format(TimestampFromDBFormat)
	}
	if subdomain.Valid {
		job.Subdomain = subdomain.String
	}

	return job, nil
}

func generateSubdomain(userID, externalID string) string {
	return fmt.Sprintf("a%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s", userID, externalID))))[0:9]
}

const setSubdomainMutation = `update only jobs set subdomain = $1 where id = $2`

func setSubdomain(ctx context.Context, dedb *sql.DB, analysisID, subdomain string) error {
	var err error

	if _, err = dedb.ExecContext(ctx, setSubdomainMutation, subdomain, analysisID); err != nil {
		return errors.Wrapf(err, "error setting subdomain for job %s to %s", analysisID, subdomain)
	}

	return err
}

const setPlannedEndDateMutation = `update only jobs set planned_end_date = $1 where id = $2`

// setPlannedEndDate takes in context, db, a job ID, and a number of milliseconds since the epoch and sets that value as the planned end date for that job
// previously, we were passing around semi-mangled timestamps, and needed to correct for having read in a local timestamp as a UTC one. This should no longer be necessary, but is noted here in case bugs crop up
func setPlannedEndDate(ctx context.Context, dedb *sql.DB, id string, millisSinceEpoch int64) error {
	var err error

	plannedEndDate := time.UnixMilli(millisSinceEpoch).
		Format("2006-01-02 15:04:05.000000-07")

	if _, err = dedb.ExecContext(ctx, setPlannedEndDateMutation, plannedEndDate, id); err != nil {
		return errors.Wrapf(err, "error setting planned_end_date to %s for job %s", plannedEndDate, id)
	}

	return err
}

const stepTypeQuery = `
SELECT t.name
  FROM jobs j
  JOIN job_steps s
    ON j.id = s.job_id
  JOIN job_types t
    ON s.job_type_id = t.id
 WHERE j.id = $1`

func isInteractive(ctx context.Context, dedb *sql.DB, id string) (bool, error) {
	var (
		err      error
		rows     *sql.Rows
		jobTypes []string
	)

	if rows, err = dedb.QueryContext(ctx, stepTypeQuery, id); err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var t string
		err = rows.Scan(&t)
		if err != nil {
			return false, err
		}
		jobTypes = append(jobTypes, t)
	}

	found := false
	for _, j := range jobTypes {
		if j == "Interactive" {
			found = true
		}
	}

	return found, nil
}

const getUserIDQuery = `
SELECT user_id
  FROM jobs
 WHERE id = $1
`

func getUserIDForJob(ctx context.Context, dedb *sql.DB, analysisID string) (string, error) {
	var (
		err    error
		userID string
	)
	if err = dedb.QueryRowContext(ctx, getUserIDQuery, analysisID).Scan(&userID); err != nil {
		return "", err
	}
	return userID, nil
}

// getTimeLimitQuery is the query for calculating a number-of-seconds time limit for a job
// if a time_limit_seconds is not set for a tool, use 72 hours (72 * 60 * 60 = 259200)
const getTimeLimitQuery = `
SELECT sum(CASE WHEN tools.time_limit_seconds > 0 THEN tools.time_limit_seconds ELSE 259200 END)
  FROM tools
  JOIN tasks ON tools.id = tasks.tool_id
  JOIN app_steps ON tasks.id = app_steps.task_id
  JOIN jobs ON jobs.app_version_id = app_steps.app_version_id
 WHERE jobs.id = $1
`

func getTimeLimit(ctx context.Context, dedb *sql.DB, analysisID string) (int64, error) {
	var (
		err              error
		timeLimitSeconds int64
	)
	if err = dedb.QueryRowContext(ctx, getTimeLimitQuery, analysisID).Scan(&timeLimitSeconds); err != nil {
		return 0, err
	}
	return timeLimitSeconds, nil
}

// EnsureSubdomain makes sure the provided job has a subdomain set in the DB, returning it
func EnsureSubdomain(ctx context.Context, dedb *sql.DB, analysis *Job) (string, error) {
	if analysis.Subdomain == "" {
		// make sure to use analysis.ID, not external ID here.
		userID, err := getUserIDForJob(ctx, dedb, analysis.ID)
		if err != nil {
			return "", errors.Wrapf(err, "error getting userID for job %s", analysis.ID)
		} else {
			log.Infof("user id is %s and invocation id is %s", userID, analysis.ExternalID)

			// make sure to use externalID, not analysis.ID here
			subdomain := generateSubdomain(userID, analysis.ExternalID)

			log.Infof("generated subdomain for analysis %s is %s, based on user ID %s and invocation ID %s", analysis.ID, subdomain, userID, analysis.ExternalID)

			if err = setSubdomain(ctx, dedb, analysis.ID, subdomain); err != nil {
				return "", errors.Wrapf(err, "error setting subdomain for analysis '%s' to '%s'", analysis.ID, subdomain)
			}
			return subdomain, nil
		}
	} else {
		return analysis.Subdomain, nil
	}
}

func EnsurePlannedEndDate(ctx context.Context, dedb *sql.DB, analysis *Job) error {
	// Check to see if the planned_end_date is set for the analysis
	if analysis.PlannedEndDate != "" {
		log.Infof("planned end date for %s is set to %s, nothing to do", analysis.ID, analysis.PlannedEndDate)
		return nil // it's already set, so move along.
	}

	startDate, err := time.ParseInLocation(TimestampFromDBFormat, analysis.StartDate, time.Local)
	if err != nil {
		return errors.Wrapf(err, "error parsing start date field %s", analysis.StartDate)
	}
	sdnano := startDate.UnixNano()

	timeLimitSeconds, err := getTimeLimit(ctx, dedb, analysis.ID)
	if err != nil {
		return errors.Wrapf(err, "error fetching time limit for analysis %s", analysis.ID)
	}

	// StartDate is in milliseconds, so convert it to nanoseconds, add correct number of seconds,
	// then convert back to milliseconds.
	endDate := time.Unix(0, sdnano).Add(time.Duration(timeLimitSeconds)*time.Second).UnixNano() / 1000000
	if err = setPlannedEndDate(ctx, dedb, analysis.ID, endDate); err != nil {
		return errors.Wrapf(err, "error setting planned end date for analysis '%s' to '%d'", analysis.ID, endDate)
	}
	return nil
}

// CreateMessageHandler returns a function that can be used by the messaging
// package to handle job status messages. The handler will set the planned
// end date for an analysis if it's not already set.
func CreateMessageHandler(dedb *sql.DB) func(context.Context, amqp.Delivery) {
	return func(ctx context.Context, delivery amqp.Delivery) {
		var err error
		msgLog := log.WithFields(log.Fields{"context": "message handler"})

		if err = delivery.Ack(false); err != nil {
			msgLog.Error(err)
		}

		update := &messaging.UpdateMessage{}

		if err = json.Unmarshal(delivery.Body, update); err != nil {
			msgLog.Error(errors.Wrap(err, "error unmarshaling body of update message"))
			return
		}

		var externalID string
		if update.Job.InvocationID == "" {
			msgLog.Error("external ID was not provided as the invocation ID in the status update, ignoring update")
			return
		}
		externalID = update.Job.InvocationID
		msgLog = msgLog.WithFields(log.Fields{"externalID": externalID})

		analysis, err := lookupByExternalID(ctx, dedb, externalID)
		if err != nil {
			msgLog.Error(errors.Wrapf(err, "error looking up analysis by external ID '%s'", externalID))
			return
		}
		msgLog = msgLog.WithFields(log.Fields{"ID": analysis.ID})

		analysisIsInteractive, err := isInteractive(ctx, dedb, analysis.ID)
		if err != nil {
			msgLog.Error(errors.Wrapf(err, "error looking up interactive status for analysis %s", analysis.ID))
			return
		}

		if !analysisIsInteractive {
			msgLog.Infof("analysis %s is not interactive, so move along", analysis.ID)
			return
		}

		if update.State != "Running" {
			msgLog.Infof("job status update for %s was %s, moving along", analysis.ID, update.State)
			return
		}

		msgLog.Infof("job status update for %s was %s", analysis.ID, update.State)

		// Set the subdomain
		subdomain, err := EnsureSubdomain(ctx, dedb, analysis)
		if err != nil {
			msgLog.Error(errors.Wrap(err, "error ensuring subdomain for analysis"))
		}
		msgLog = msgLog.WithFields(log.Fields{"subdomain": subdomain})

		err = EnsurePlannedEndDate(ctx, dedb, analysis)
		if err != nil {
			msgLog.Error(errors.Wrap(err, "error ensuring planned end date for analysis"))
		}
	}
}
