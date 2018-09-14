package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/cloudflare/cfssl/log"
	"github.com/machinebox/graphql"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v4"
)

// SystemIDInteractive is the system ID for interactive jobs.
const SystemIDInteractive = "interactive"

// TimestampFromDBFormat is the format of the timestamps retrieved from the
// database through the GraphQL server. Shouldn't have timezone info.
const TimestampFromDBFormat = "2006-01-02T03:04:05"

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
	ID             string  `json:"id"`
	AppID          string  `json:"app_id"`
	UserID         string  `json:"user_id"`
	Status         string  `json:"status"`
	Description    string  `json:"description"`
	Name           string  `json:"name"`
	ResultFolder   string  `json:"result_folder"`
	StartDate      string  `json:"start_date"`
	PlannedEndDate string  `json:"planned_end_date"`
	Subdomain      string  `json:"subdomain"`
	Type           JobType `json:"type"`
	User           JobUser `json:"user"`
}

// JobList is a list of Jobs, serializable in JSON the way that we typically
// serialize lists.
type JobList struct {
	Jobs []Job `json:"jobs"`
}

const jobsToKillQuery = `
query Jobs($status: String, $planned_end_date: timestamp){
  jobs(where: {status: {_eq: $status}, planned_end_date: {_lte: $planned_end_date}}) {
    id
    app_id
    user_id
    status
    description: job_description
    name: job_name
    result_folder: result_folder_path
    planned_end_date
		start_date
    type: jobTypesByjobTypeId {
      system_id
    }
    user: usersByuserId {
      username
    }
  }
}
`

// JobsToKill returns a list of running jobs that are past their expiration date
// and can be killed off. 'api' should be the base URL for the analyses service.
func JobsToKill(api string) ([]Job, error) {
	var (
		err error
		ok  bool
	)

	client := graphql.NewClient(api)

	req := graphql.NewRequest(jobsToKillQuery)
	req.Var("status", "Running")
	req.Var("planned_end_date", time.Now().Format("2006-01-02 03:04:05.000000-07"))

	data := map[string][]Job{}

	if err = client.Run(context.Background(), req, &data); err != nil {
		return nil, err
	}

	if _, ok = data["jobs"]; !ok {
		return nil, errors.New("missing jobs field in graphql response")
	}

	return data["jobs"], nil
}

const jobWarningsQuery = `
query JobWarnings($status: String, $now: timestamp, $future: timestamp){
  jobs(where: {status: {_eq: $status}, planned_end_date: {_gt: $now, _lte: $future}}) {
    id
    app_id
    user_id
    status
    description: job_description
    name: job_name
    result_folder: result_folder_path
    planned_end_date
		start_date
    type: jobTypesByjobTypeId {
      system_id
    }
    user: usersByuserId {
      username
    }
  }
}
`

// JobKillWarnings returns a list of running jobs that are set to be killed
// within the number of minutes specified. 'api' should be the base URL for the
// analyses service.
func JobKillWarnings(api string, minutes int64) ([]Job, error) {
	var (
		err error
		ok  bool
	)

	client := graphql.NewClient(api)

	now := time.Now()
	fmtstring := "2006-01-02 15:04:05.000000-07"
	nowtimestamp := now.Format(fmtstring)
	futuretimestamp := now.Add(time.Duration(minutes) * time.Minute).Format(fmtstring)

	req := graphql.NewRequest(jobWarningsQuery)
	req.Var("status", "Running")
	req.Var("now", nowtimestamp)
	req.Var("future", futuretimestamp)

	data := map[string][]Job{}

	if err = client.Run(context.Background(), req, &data); err != nil {
		return nil, err
	}

	if _, ok = data["jobs"]; !ok {
		return nil, errors.New("missing jobs field in graphql response")
	}

	return data["jobs"], nil
}

// KillJob uses the provided API at the base URL to kill a running job. This
// will probably be to the apps service. jobID should be the UUID for the Job,
// typically returned in the ID field by the analyses service. The username
// should be the short username for the user that launched the job.
func KillJob(api, jobID, username string) error {
	apiURL, err := url.Parse(api)
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
		return fmt.Errorf("response status code for GET %s was %d as %s", apiURL.String(), resp.StatusCode, username)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	logger.Infof("response from %s was: %s", req.URL, string(body))
	return nil
}

const jobByExternalIDQuery = `
query Jobs($externalID: String){
  jobs(where: {jobStepssByjobId: {external_id: {_eq: $externalID}}}) {
    id
    app_id
    user_id
    status
    description: job_description
    name: job_name
    result_folder: result_folder_path
    planned_end_date
		subdomain
		start_date
    type: jobTypesByjobTypeId {
      system_id
    }
    user: usersByuserId {
      username
    }
    steps: jobStepssByjobId {
      external_id
    }
  }
}
`

func lookupByExternalID(api, externalID string) (*Job, error) {
	var (
		err error
		ok  bool
	)

	client := graphql.NewClient(api)

	req := graphql.NewRequest(jobByExternalIDQuery)
	req.Var("externalID", externalID)

	data := map[string][]Job{}

	if err = client.Run(context.Background(), req, &data); err != nil {
		return nil, err
	}

	if _, ok = data["jobs"]; !ok {
		return nil, errors.New("missing jobs field in graphql response")
	}

	if len(data["jobs"]) <= 0 {
		return nil, fmt.Errorf("no job found for external ID %s", externalID)
	}

	return &data["jobs"][0], nil
}

func generateSubdomain(userID, externalID string) string {
	return fmt.Sprintf("a%x", sha256.Sum256([]byte(fmt.Sprintf("%s%s", userID, externalID))))[0:9]
}

const setSubdomainMutation = `
mutation SetSubdomain($id: uuid, $subdomain: String) {
	update_jobs(
		where: {id: {_eq: $id}},
		_set: {
			subdomain: $subdomain
		}
	) {
		returning {
			id
			subdomain
		}
	}
}
`

func setSubdomain(api, id, subdomain string) error {
	var (
		err error
		ok  bool
	)

	client := graphql.NewClient(api)
	req := graphql.NewRequest(setSubdomainMutation)
	req.Var("id", id)
	req.Var("subdomain", subdomain)

	data := map[string]map[string][]map[string]string{}

	if err = client.Run(context.Background(), req, &data); err != nil {
		return err
	}

	if _, ok = data["update_jobs"]; !ok {
		return errors.New("missing update_jobs field in graphql response")
	}

	if _, ok = data["update_jobs"]["returning"]; !ok {
		return errors.New("missing update_jobs.returning field in graphql response")
	}

	if len(data["update_jobs"]["returning"]) <= 0 {
		return errors.New("nothing returned by the SetPlannedEndDate mutation")
	}

	retval := data["update_jobs"]["returning"][0]

	if _, ok = retval["id"]; !ok {
		return errors.New("id wasn't returned by the SetPlannedEndDate mutation")
	}

	if _, ok = retval["subdomain"]; !ok {
		return errors.New("subdomain was not returned by the SetSubdomain mutation")
	}

	return nil
}

const setPlannedEndDateMutation = `
mutation SetPlannedEndDate($id: uuid, $planned_end_date: timestamp) {
  update_jobs(
    where: {id: {_eq: $id}},
    _set: {
      planned_end_date: $planned_end_date
    }
  ) {
    returning {
      id
      planned_end_date
    }
  }
}
`

// EndDatePatch will turn into a JSON body that can be passed to the analyses
// service to set the planned_end_date for an Analysis/Job.
type EndDatePatch struct {
	PlannedEndDate int64 `json:"planned_end_date"`
}

func setPlannedEndDate(api, id string, millisSinceEpoch int64) error {
	var (
		err error
		ok  bool
	)

	client := graphql.NewClient(api)

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
		Format("2006-01-02 03:04:05.000000-07")

	req := graphql.NewRequest(setPlannedEndDateMutation)
	req.Var("id", id)
	req.Var("planned_end_date", plannedEndDate)

	data := map[string]map[string][]map[string]string{}

	if err = client.Run(context.Background(), req, &data); err != nil {
		return err
	}

	if _, ok = data["update_jobs"]; !ok {
		return errors.New("missing update_jobs field in graphql response")
	}

	if _, ok = data["update_jobs"]["returning"]; !ok {
		return errors.New("missing update_jobs.returning field in graphql response")
	}

	if len(data["update_jobs"]["returning"]) <= 0 {
		return errors.New("nothing returned by the SetPlannedEndDate mutation")
	}

	retval := data["update_jobs"]["returning"][0]

	if _, ok = retval["id"]; !ok {
		return errors.New("id wasn't returned by the SetPlannedEndDate mutation")
	}

	if _, ok = retval["planned_end_date"]; !ok {
		return errors.New("planned_end_date was not returned by the SetPlannedEndDate mutation")
	}

	log.Infof("id: %s, planned_end_date: %s returned by the SetPlannedEndDate mutation", retval["id"], retval["planned_end_date"])

	return nil
}

const stepTypeQuery = `
query JobStepType($id: uuid) {
  steps: job_steps(where: {job_id: {_eq: $id}}) {
    type: jobTypesByjobTypeId {
      name
    }
  }
}
`

func isInteractive(api, id string) (bool, error) {
	var (
		ok  bool
		err error
	)

	client := graphql.NewClient(api)
	req := graphql.NewRequest(stepTypeQuery)
	req.Var("id", id)

	data := map[string][]map[string]map[string]string{}

	if err = client.Run(context.Background(), req, &data); err != nil {
		return false, err
	}

	if _, ok = data["steps"]; !ok {
		return false, errors.New("missing steps field in graphql response")
	}

	if len(data["steps"]) <= 0 {
		return false, fmt.Errorf("no steps found for analysis %s", id)
	}

	step := data["steps"][0]

	if _, ok = step["type"]; !ok {
		return false, fmt.Errorf("no type field found for analysis %s step", id)
	}

	if _, ok = step["type"]["name"]; !ok {
		return false, fmt.Errorf("no name field found for analysis %s step type", id)
	}

	return step["type"]["name"] == "Interactive", nil
}

// CreateMessageHandler returns a function that can be used by the messaging
// package to handle job status messages. The handler will set the planned
// end date for an analysis if it's not already set.
func CreateMessageHandler(graphqlBaseURL string) func(amqp.Delivery) {
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

		analysis, err := lookupByExternalID(graphqlBaseURL, externalID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error looking up analysis by external ID '%s'", externalID))
			return
		}

		analysisIsInteractive, err := isInteractive(graphqlBaseURL, analysis.ID)
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
			subdomain := generateSubdomain(update.Job.UserID, update.Job.InvocationID)
			if err = setSubdomain(graphqlBaseURL, analysis.ID, subdomain); err != nil {
				log.Error(errors.Wrapf(err, "error setting subdomain for analysis '%s' to '%s'", analysis.ID, subdomain))
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
		endDate := time.Unix(0, sdnano).Add(48*time.Hour).UnixNano() / 1000000
		if err = setPlannedEndDate(graphqlBaseURL, analysis.ID, endDate); err != nil {
			log.Error(errors.Wrapf(err, "error setting planned end date for analysis '%s' to '%d'", analysis.ID, endDate))
		}
	}
}
