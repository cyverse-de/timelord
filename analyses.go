package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
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

// VICEURI is the base URI for VICE access
var VICEURI string

// AnalysesInit initializes the base URI for VICE access
func AnalysesInit(u string) {
	VICEURI = u
}

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
	NotifyPeriodic bool   `json:"notify_periodic"`
	PeriodicPeriod int    `json:"periodic_period"`
}

type Analysis = Job

func (j *Job) accessURL() (string, error) {
	if VICEURI == "" {
		return "", nil
	}
	vice_uri, err := url.Parse(VICEURI)
	if err != nil {
		return "", errors.Wrapf(err, "Error parsing VICE URI from %s", VICEURI)
	}
	vice_uri.Host = j.Subdomain + "." + vice_uri.Host
	return vice_uri.String(), nil
}

// getJobDuration takes a job and returns a duration string since the start of the job
func getJobDuration(j *Job) (string, error) {
	starttime, err := time.ParseInLocation(TimestampFromDBFormat, j.StartDate, time.Local)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse start date %s", j.StartDate)
	}

	// just print H(HH):MM format
	dur := time.Since(starttime).Round(time.Minute)
	h := dur / time.Hour
	dur -= h * time.Hour
	m := dur / time.Minute
	return fmt.Sprintf("%d:%02d", h, m), nil
}

// getRemainingDuration takes a job and returns a duration string until the planned end date
func getRemainingDuration(j *Job) (string, error) {
	endtime, err := time.ParseInLocation(TimestampFromDBFormat, j.PlannedEndDate, time.Local)
	if err != nil {
		return "", errors.Wrapf(err, "failed to parse planned end date %s", j.PlannedEndDate)
	}

	// just print H(HH):MM format
	dur := time.Until(endtime).Round(time.Minute)
	h := dur / time.Hour
	dur -= h * time.Hour
	m := dur / time.Minute
	return fmt.Sprintf("%d:%02d", h, m), nil
}

type TimelordAnalyses struct {
	dedb *sql.DB
}

func NewTimelordAnalyses(dedb *sql.DB) *TimelordAnalyses {
	return &TimelordAnalyses{
		dedb,
	}
}

func (a *TimelordAnalyses) jobFromRow(ctx context.Context, rows *sql.Rows) (Job, error) {
	var (
		err            error
		job            Job
		startDate      pq.NullTime
		plannedEndDate pq.NullTime
		subdomain      sql.NullString
	)

	job = Job{}

	if err := rows.Scan(
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
		&job.NotifyPeriodic,
		&job.PeriodicPeriod,
	); err != nil {
		return job, err
	}

	if plannedEndDate.Valid {
		job.PlannedEndDate = plannedEndDate.Time.Format(TimestampFromDBFormat)
	}

	if subdomain.Valid {
		job.Subdomain = subdomain.String
	}

	if startDate.Valid {
		job.StartDate = startDate.Time.Format(TimestampFromDBFormat)
	}

	job.ExternalID, err = a.getExternalID(ctx, job.ID)
	if err != nil {
		return job, err
	}
	return job, nil
}

const externalIDsQuery = `
select job_steps.external_id
  from job_steps
 where job_steps.job_id = $1
 limit 1`

func (a *TimelordAnalyses) getExternalID(ctx context.Context, jobID string) (string, error) {
	var (
		err        error
		row        *sql.Row
		externalID string
	)

	row = a.dedb.QueryRowContext(
		ctx,
		externalIDsQuery,
		jobID,
	)
	if err = row.Scan(&externalID); err != nil {
		return "", err
	}

	return externalID, err
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
       jobs.subdomain,
       jobs.start_date,
       job_types.system_id,
       users.username,
       COALESCE((jobs.submission->>'notify_periodic')::bool, TRUE) AS notify_periodic,
       COALESCE((jobs.submission->>'periodic_period')::int, 0) AS periodic_period
  from jobs
  join job_types on jobs.job_type_id = job_types.id
  join users on jobs.user_id = users.id
 where jobs.status = $1
   and jobs.planned_end_date <= $2`

// JobsToKill returns a list of running jobs that are past their expiration date
// and can be killed off. 'api' should be the base URL for the analyses service.
func (a *TimelordAnalyses) JobsToKill(ctx context.Context) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	if rows, err = a.dedb.QueryContext(
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
		job, err := a.jobFromRow(ctx, rows)
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
       jobs.subdomain,
       jobs.start_date,
       job_types.system_id,
       users.username,
       COALESCE((jobs.submission->>'notify_periodic')::bool, TRUE) AS notify_periodic,
       COALESCE((jobs.submission->>'periodic_period')::int, 0) AS periodic_period
  FROM jobs
  JOIN job_types on jobs.job_type_id = job_types.id
  JOIN users on jobs.user_id = users.id
  LEFT join notif_statuses ON jobs.id = notif_statuses.analysis_id
 WHERE jobs.status = $1
   AND jobs.planned_end_date > now()
   AND (notif_statuses.last_periodic_warning is null
    OR notif_statuses.last_periodic_warning < now() - coalesce(notif_statuses.periodic_warning_period, '4 hours'::interval))
`

// JobPeriodicWarnings returns a list of running jobs that may need periodic notifications to be sent
func (a *TimelordAnalyses) JobPeriodicWarnings(ctx context.Context) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	if rows, err = a.dedb.QueryContext(
		ctx,
		periodicWarningsQuery,
		"Running",
	); err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := []Job{}

	for rows.Next() {
		job, err := a.jobFromRow(ctx, rows)
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
       jobs.subdomain,
       jobs.start_date,
       job_types.system_id,
       users.username,
       COALESCE((jobs.submission->>'notify_periodic')::bool, TRUE) AS notify_periodic,
       COALESCE((jobs.submission->>'periodic_period')::int, 0) AS periodic_period
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
func (a *TimelordAnalyses) JobKillWarnings(ctx context.Context, minutes int64) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	now := time.Now()
	// fmtstring := "2006-01-02 15:04:05.000000-07"
	// nowtimestamp := now.Format(fmtstring)
	// futuretimestamp := now.Add(time.Duration(minutes) * time.Minute).Format(fmtstring)

	if rows, err = a.dedb.QueryContext(
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
		job, err := a.jobFromRow(ctx, rows)
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
       COALESCE((jobs.submission->>'notify_periodic')::bool, TRUE) AS notify_periodic,
       COALESCE((jobs.submission->>'periodic_period')::int, 0) AS periodic_period,
       job_steps.external_id
  from jobs
  join job_types on jobs.job_type_id = job_types.id
  join users on jobs.user_id = users.id
  join job_steps on jobs.id = job_steps.job_id
 where job_steps.external_id = $1`

func (a *TimelordAnalyses) lookupByExternalID(ctx context.Context, externalID string) (*Job, error) {
	var (
		err            error
		job            *Job
		subdomain      sql.NullString
		startDate      pq.NullTime
		plannedEndDate pq.NullTime
	)

	job = &Job{}

	if err = a.dedb.QueryRowContext(ctx, jobByExternalIDQuery, externalID).Scan(
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
		&job.NotifyPeriodic,
		&job.PeriodicPeriod,
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

func (a *TimelordAnalyses) setSubdomain(ctx context.Context, analysisID, subdomain string) error {
	var err error

	if _, err = a.dedb.ExecContext(ctx, setSubdomainMutation, subdomain, analysisID); err != nil {
		return errors.Wrapf(err, "error setting subdomain for job %s to %s", analysisID, subdomain)
	}

	return err
}

const setPlannedEndDateMutation = `update only jobs set planned_end_date = $1 where id = $2`

// setPlannedEndDate takes in context, db, a job ID, and a number of milliseconds since the epoch and sets that value as the planned end date for that job
// previously, we were passing around semi-mangled timestamps, and needed to correct for having read in a local timestamp as a UTC one. This should no longer be necessary, but is noted here in case bugs crop up
func (a *TimelordAnalyses) setPlannedEndDate(ctx context.Context, id string, millisSinceEpoch int64) error {
	var err error

	plannedEndDate := time.UnixMilli(millisSinceEpoch).
		Format("2006-01-02 15:04:05.000000-07")

	if _, err = a.dedb.ExecContext(ctx, setPlannedEndDateMutation, plannedEndDate, id); err != nil {
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

func (a *TimelordAnalyses) isInteractive(ctx context.Context, id string) (bool, error) {
	var (
		err      error
		rows     *sql.Rows
		jobTypes []string
	)

	if rows, err = a.dedb.QueryContext(ctx, stepTypeQuery, id); err != nil {
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

func (a *TimelordAnalyses) getUserIDForJob(ctx context.Context, analysisID string) (string, error) {
	var (
		err    error
		userID string
	)
	if err = a.dedb.QueryRowContext(ctx, getUserIDQuery, analysisID).Scan(&userID); err != nil {
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

func (a *TimelordAnalyses) getTimeLimit(ctx context.Context, analysisID string) (int64, error) {
	var (
		err              error
		timeLimitSeconds int64
	)
	if err = a.dedb.QueryRowContext(ctx, getTimeLimitQuery, analysisID).Scan(&timeLimitSeconds); err != nil {
		return 0, err
	}
	return timeLimitSeconds, nil
}

// EnsureSubdomain makes sure the provided job has a subdomain set in the DB, returning it
func (a *TimelordAnalyses) EnsureSubdomain(ctx context.Context, analysis *Job) (string, error) {
	if analysis.Subdomain == "" {
		// make sure to use analysis.ID, not external ID here.
		userID, err := a.getUserIDForJob(ctx, analysis.ID)
		if err != nil {
			return "", errors.Wrapf(err, "error getting userID for job %s", analysis.ID)
		} else {
			log.Infof("user id is %s and invocation id is %s", userID, analysis.ExternalID)

			// make sure to use externalID, not analysis.ID here
			subdomain := generateSubdomain(userID, analysis.ExternalID)

			log.Infof("generated subdomain for analysis %s is %s, based on user ID %s and invocation ID %s", analysis.ID, subdomain, userID, analysis.ExternalID)

			if err = a.setSubdomain(ctx, analysis.ID, subdomain); err != nil {
				return "", errors.Wrapf(err, "error setting subdomain for analysis '%s' to '%s'", analysis.ID, subdomain)
			}
			return subdomain, nil
		}
	} else {
		return analysis.Subdomain, nil
	}
}

func (a *TimelordAnalyses) EnsurePlannedEndDate(ctx context.Context, analysis *Job) error {
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

	timeLimitSeconds, err := a.getTimeLimit(ctx, analysis.ID)
	if err != nil {
		return errors.Wrapf(err, "error fetching time limit for analysis %s", analysis.ID)
	}

	// StartDate is in milliseconds, so convert it to nanoseconds, add correct number of seconds,
	// then convert back to milliseconds.
	endDate := time.Unix(0, sdnano).Add(time.Duration(timeLimitSeconds)*time.Second).UnixNano() / 1000000
	if err = a.setPlannedEndDate(ctx, analysis.ID, endDate); err != nil {
		return errors.Wrapf(err, "error setting planned end date for analysis '%s' to '%d'", analysis.ID, endDate)
	}
	return nil
}

// CreateMessageHandler returns a function that can be used by the messaging
// package to handle job status messages. The handler will set the planned
// end date for an analysis if it's not already set.
func (a *TimelordAnalyses) CreateMessageHandler() func(context.Context, amqp.Delivery) {
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

		analysis, err := a.lookupByExternalID(ctx, externalID)
		if err != nil {
			msgLog.Error(errors.Wrapf(err, "error looking up analysis by external ID '%s'", externalID))
			return
		}
		msgLog = msgLog.WithFields(log.Fields{"ID": analysis.ID})

		analysisIsInteractive, err := a.isInteractive(ctx, analysis.ID)
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
		subdomain, err := a.EnsureSubdomain(ctx, analysis)
		if err != nil {
			msgLog.Error(errors.Wrap(err, "error ensuring subdomain for analysis"))
		}
		msgLog = msgLog.WithFields(log.Fields{"subdomain": subdomain})

		err = a.EnsurePlannedEndDate(ctx, analysis)
		if err != nil {
			msgLog.Error(errors.Wrap(err, "error ensuring planned end date for analysis"))
		}
	}
}
