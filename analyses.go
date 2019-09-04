package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/cloudflare/cfssl/log"
	pq "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v4"
)

// TimestampFromDBFormat is the format of the timestamps retrieved from the
// database through the GraphQL server. Shouldn't have timezone info.
const TimestampFromDBFormat = "2006-01-02T15:04:05"

// JobType contains the system ID for a job.
type JobType struct {
	SystemID string `json:"system_id"`
}

// JobUser contains user information associated with a job.
type JobUser struct {
	Username string `json:"username"`
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

// JobsToKill returns a list of running jobs that are past their expiration date
// and can be killed off. 'api' should be the base URL for the analyses service.
func JobsToKill(db *sql.DB) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	if rows, err = db.Query(
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
func JobKillWarnings(db *sql.DB, minutes int64) ([]Job, error) {
	var (
		err  error
		rows *sql.Rows
	)

	now := time.Now()
	// fmtstring := "2006-01-02 15:04:05.000000-07"
	// nowtimestamp := now.Format(fmtstring)
	// futuretimestamp := now.Add(time.Duration(minutes) * time.Minute).Format(fmtstring)

	if rows, err = db.Query(
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
func (j *JobKiller) KillJob(db *sql.DB, jobID, username string) error {
	if j.K8sEnabled {
		return j.killK8sJob(db, jobID)
	}
	return j.killCondorJob(jobID, username)

}

// killCondorJob uses the provided API at the base URL to kill a running job. This
// will probably be to the apps service. jobID should be the UUID for the Job,
// typically returned in the ID field by the analyses service. The username
// should be the short username for the user that launched the job.
func (j *JobKiller) killCondorJob(jobID, username string) error {
	apiURL, err := url.Parse(j.AppsBase)
	if err != nil {
		return err
	}

	apiURL.Path = filepath.Join(apiURL.Path, "analyses", jobID, "stop")

	req, err := http.NewRequest(http.MethodPost, apiURL.String(), nil)
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

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("response status code for POST %s was %d as %s", apiURL.String(), resp.StatusCode, username)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	logger.Infof("response from %s was: %s", req.URL, string(body))
	return nil
}

const externalIDsQuery = `
select job_steps.external_id
  from job_steps
 where job_steps.job_id = $1`

// killK8sJob uses the app-exposer API to make a job save its outputs and exit.
// JobID should be the external_id (AKA invocationID) for the job.
func (j *JobKiller) killK8sJob(db *sql.DB, jobID string) error {
	var (
		err         error
		rows        *sql.Rows
		externalIDs []string
	)

	origAPIURL, err := url.Parse(j.AppExposerBase)
	if err != nil {
		return err
	}

	if rows, err = db.Query(externalIDsQuery, jobID); err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return err
		}
		externalIDs = append(externalIDs, id)

	}

	for _, externalID := range externalIDs {
		var apiURL *url.URL
		apiURL, err = url.Parse(origAPIURL.String()) // lol
		if err != nil {
			return errors.Wrapf(err, "error parsing URL %s while processing external-id %s", origAPIURL.String(), externalID)
		}

		apiURL.Path = filepath.Join(apiURL.Path, "vice", externalID, "save-and-exit")

		req, err := http.NewRequest(http.MethodPost, apiURL.String(), nil)
		if err != nil {
			return errors.Wrapf(err, "error creating save-and-exit request for external-id %s", externalID)
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return errors.Wrapf(err, "error calling save-and-exit for external-id %s", externalID)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("response status code for POST %s was %d", apiURL.String(), resp.StatusCode)
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrapf(err, "error reading response body of save-and-exit call for external-id %s", externalID)
		}

		logger.Infof("response from %s was: %s", req.URL, string(body))

		resp.Body.Close()
	}

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

func lookupByExternalID(db *sql.DB, externalID string) (*Job, error) {
	var (
		err            error
		job            *Job
		subdomain      sql.NullString
		startDate      pq.NullTime
		plannedEndDate pq.NullTime
	)

	job = &Job{}

	if err = db.QueryRow(jobByExternalIDQuery, externalID).Scan(
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

func setSubdomain(db *sql.DB, analysisID, subdomain string) error {
	var err error

	if _, err = db.Exec(setSubdomainMutation, subdomain, analysisID); err != nil {
		return errors.Wrapf(err, "error setting subdomain for job %s to %s", analysisID, subdomain)
	}

	return err
}

const setPlannedEndDateMutation = `update only jobs set planned_end_date = $1 where id = $2`

func setPlannedEndDate(db *sql.DB, id string, millisSinceEpoch int64) error {
	var err error

	// Get the time zone offset from UTC in seconds
	_, offset := time.Now().Local().Zone()

	// Durations are tracked as as nanoseconds stored as an int64, so convert
	// the seconds into an int64 (which shouldn't lose precision), then
	// multiply by 1000000000 to convert to Nanoseconds. Next multiply by -1
	// to flip the sign on the offset, which is needed because we're doing
	// weird-ish stuff with timestamps in the database. Multiply all of that
	// by time.Nanosecond to make sure that we're using the right units.
	addition := time.Duration(int64(offset)*1000000000*-1) * time.Nanosecond

	plannedEndDate := time.Unix(0, millisSinceEpoch*1000000).
		Add(addition).
		Format("2006-01-02 15:04:05.000000-07")

	if _, err = db.Exec(setPlannedEndDateMutation, plannedEndDate, id); err != nil {
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

func isInteractive(db *sql.DB, id string) (bool, error) {
	var (
		err      error
		rows     *sql.Rows
		jobTypes []string
	)

	if rows, err = db.Query(stepTypeQuery, id); err != nil {
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

func getUserIDForJob(db *sql.DB, invocationID string) (string, error) {
	var (
		err    error
		userID string
	)
	if err = db.QueryRow(getUserIDQuery, invocationID).Scan(&userID); err != nil {
		return "", err
	}
	return userID, nil
}

// CreateMessageHandler returns a function that can be used by the messaging
// package to handle job status messages. The handler will set the planned
// end date for an analysis if it's not already set.
func CreateMessageHandler(db *sql.DB) func(amqp.Delivery) {
	return func(delivery amqp.Delivery) {
		var err error

		if err = delivery.Ack(false); err != nil {
			log.Error(err)
		}

		update := &messaging.UpdateMessage{}

		if err = json.Unmarshal(delivery.Body, update); err != nil {
			log.Error(errors.Wrap(err, "error unmarshaling body of update message"))
			return
		}

		var externalID string
		if update.Job.InvocationID == "" {
			log.Error("external ID was not provided as the invocation ID in the status update, ignoring update")
			return
		}
		externalID = update.Job.InvocationID

		analysis, err := lookupByExternalID(db, externalID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error looking up analysis by external ID '%s'", externalID))
			return
		}

		analysisIsInteractive, err := isInteractive(db, analysis.ID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error looking up interactive status for analysis %s", analysis.ID))
			return
		}

		if !analysisIsInteractive {
			log.Infof("analysis %s is not interactive, so move along", analysis.ID)
			return
		}

		if update.State != "Running" {
			log.Infof("job status update for %s was %s, moving along", analysis.ID, update.State)
			return
		}

		log.Infof("job status update for %s was %s", analysis.ID, update.State)

		// Set the subdomain
		if analysis.Subdomain == "" {
			log.Infof("invocationID is %s", externalID)

			userID, err := getUserIDForJob(db, externalID)
			if err != nil {
				log.Error(errors.Wrapf(err, "error getting userID for job %s", externalID))
			} else {
				log.Infof("user id is %s and invocation id is %s", userID, externalID)

				subdomain := generateSubdomain(userID, externalID)

				log.Infof("generated subdomain for analysis %s is %s, based on user ID %s and invocation ID %s", analysis.ID, subdomain, userID, externalID)

				if err = setSubdomain(db, analysis.ID, subdomain); err != nil {
					log.Error(errors.Wrapf(err, "error setting subdomain for analysis '%s' to '%s'", analysis.ID, subdomain))
				}
			}
		}

		// Check to see if the planned_end_date is set for the analysis
		if analysis.PlannedEndDate != "" {
			log.Infof("planned end date for %s is set to %s, nothing to do", analysis.ID, analysis.PlannedEndDate)
			return // it's already set, so move along.
		}

		startDate, err := time.Parse(TimestampFromDBFormat, analysis.StartDate)
		if err != nil {
			log.Error(errors.Wrapf(err, "error parsing start date field %s", analysis.StartDate))
			return
		}
		sdnano := startDate.UnixNano()

		// StartDate is in milliseconds, so convert it to nanoseconds, add 48 hours,
		// then convert back to milliseconds.
		endDate := time.Unix(0, sdnano).Add(72*time.Hour).UnixNano() / 1000000
		if err = setPlannedEndDate(db, analysis.ID, endDate); err != nil {
			log.Error(errors.Wrapf(err, "error setting planned end date for analysis '%s' to '%d'", analysis.ID, endDate))
		}
	}
}
