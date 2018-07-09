package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/cloudflare/cfssl/log"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gopkg.in/cyverse-de/messaging.v4"
)

// SystemIDInteractive is the system ID for interactive jobs.
const SystemIDInteractive = "interactive"

// Job contains the information about an analysis that we're interested in.
type Job struct {
	ID             string `json:"id"`
	AppID          string `json:"app_id"`
	UserID         string `json:"user_id"`
	Username       string `json:"username"`
	Status         string `json:"status"`
	Description    string `json:"description"`
	Name           string `json:"name"`
	ResultFolder   string `json:"result_folder"`
	StartDate      int64  `json:"start_date"`
	PlannedEndDate *int64 `json:"planned_end_date"`
	SystemID       string `json:"system_id"`
}

// JobList is a list of Jobs, serializable in JSON the way that we typically
// serialize lists.
type JobList struct {
	Jobs []Job `json:"jobs"`
}

// JobsToKill returns a list of running jobs that are past their expiration date
// and can be killed off. 'api' should be the base URL for the analyses service.
func JobsToKill(api string) (*JobList, error) {
	apiURL, err := url.Parse(api)
	if err != nil {
		return nil, err
	}
	apiURL.Path = filepath.Join(apiURL.Path, "/expired/running")

	resp, err := http.Get(apiURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	joblist := &JobList{
		Jobs: []Job{},
	}

	if err = json.NewDecoder(resp.Body).Decode(joblist); err != nil {
		return nil, err
	}

	return joblist, nil
}

// JobKillWarnings returns a list of running jobs that are set to be killed
// within the next 10 minutes. 'api' should be the base URL for the analyses
// service.
func JobKillWarnings(api string, minutes int64) (*JobList, error) {
	apiURL, err := url.Parse(api)
	if err != nil {
		return nil, err
	}
	apiURL.Path = filepath.Join(apiURL.Path, fmt.Sprintf("/expires-in/%d/running", minutes))

	resp, err := http.Get(apiURL.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	joblist := &JobList{
		Jobs: []Job{},
	}

	if err = json.NewDecoder(resp.Body).Decode(joblist); err != nil {
		return nil, err
	}

	return joblist, nil
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

	req, err := http.NewRequest(http.MethodGet, apiURL.String(), nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Add("user", username)
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	logger.Infof("response from %s was: %s", req.URL, string(body))
	return nil
}

func lookupByExternalID(analysesURL, externalID string) (*Job, error) {
	apiURL, err := url.Parse(analysesURL)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing URL %s", analysesURL)
	}
	apiURL.Path = filepath.Join(apiURL.Path, "external-id", externalID)

	resp, err := http.Get(apiURL.String())
	if err != nil {
		return nil, errors.Wrapf(err, "error doing GET %s", apiURL.String())
	}
	defer resp.Body.Close()

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response body")
	}

	log.Infof("response body of external id lookup was: '%s'", string(b))

	j := &Job{}
	if err = json.Unmarshal(b, j); err != nil {
		return nil, errors.Wrap(err, "error unmarshaling json in response body")
	}

	return j, nil
}

// StatusUpdate contains the information contained in a status update for an
// analysis in the database
type StatusUpdate struct {
	ID                     string `json:"id"`          // The analysis ID
	ExternalID             string `json:"external_id"` // Also referred to as invocation ID
	Status                 string `json:"status"`
	SentFrom               string `json:"sent_from"`
	SentOn                 int64  `json:"sent_on"` // Not actually nullable.
	Propagated             bool   `json:"propagated"`
	PropagationAttempts    int64  `json:"propagation_attempts"`
	LastPropagationAttempt int64  `json:"last_propagation_attempt"`
	CreatedDate            int64  `json:"created_date"` // Not actually nullable.
}

// StatusUpdates is a list of StatusUpdates. Mostly exists for marshalling a
// list into JSON in a format our other services generally expect.
type StatusUpdates struct {
	Updates []StatusUpdate `json:"status_updates"`
}

func lookupStatusUpdates(analysesURL, id string) (*StatusUpdates, error) {
	apiURL, err := url.Parse(analysesURL)
	if err != nil {
		return nil, errors.Wrapf(err, "error parsing URL %s", analysesURL)
	}
	apiURL.Path = filepath.Join(apiURL.Path, "id", id, "status-updates")

	resp, err := http.Get(apiURL.String())
	if err != nil {
		return nil, errors.Wrapf(err, "error doing GET %s", apiURL.String())
	}
	defer resp.Body.Close()

	updates := &StatusUpdates{}
	if err = json.NewDecoder(resp.Body).Decode(updates); err != nil {
		return nil, errors.Wrap(err, "error decoding response body of status update lookup")
	}

	return updates, nil
}

// EndDatePatch will turn into a JSON body that can be passed to the analyses
// service to set the planned_end_date for an Analysis/Job.
type EndDatePatch struct {
	PlannedEndDate int64 `json:"planned_end_date"`
}

func setPlannedEndDate(analysesURL, id string, millisSinceEpoch int64) error {
	apiURL, err := url.Parse(analysesURL)
	if err != nil {
		return err
	}
	apiURL.Path = filepath.Join(apiURL.Path, "id", id)

	ped := &EndDatePatch{
		PlannedEndDate: millisSinceEpoch,
	}

	buf := bytes.NewBuffer([]byte{})

	if err = json.NewEncoder(buf).Encode(ped); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, apiURL.String(), buf)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	logger.Infof("response from %s was: %s", req.URL, string(body))

	return nil
}

func isInteractive(analysesURL, id string) (bool, error) {
	apiURL, err := url.Parse(analysesURL)
	if err != nil {
		return false, errors.Wrapf(err, "error parsing URL %s", analysesURL)
	}
	apiURL.Path = filepath.Join(apiURL.Path, "id", id, "interactive")

	resp, err := http.Get(apiURL.String())
	if err != nil {
		return false, errors.Wrapf(err, "error doing GET %s", apiURL.String())
	}
	defer resp.Body.Close()

	retval := map[string]bool{}

	if err = json.NewDecoder(resp.Body).Decode(&retval); err != nil {
		return false, errors.Wrap(err, "error decoding body of response")
	}

	if _, ok := retval["interactive"]; !ok {
		return false, errors.Wrapf(err, "key 'interactive' not found in map '%+v'", retval)
	}

	return retval["interactive"], nil
}

// CreateMessageHandler returns a function that can be used by the messaging
// package to handle job status messages. The handler will set the planned
// end date for an analysis if it's not already set.
func CreateMessageHandler(analysesBaseURL string) func(amqp.Delivery) {
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

		analysis, err := lookupByExternalID(analysesBaseURL, externalID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error looking up analysis by external ID '%s'", externalID))
			return
		}

		analysisIsInteractive, err := isInteractive(analysesBaseURL, analysis.ID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error looking up interactive status for analysis %s", analysis.ID))
			return
		}

		if !analysisIsInteractive {
			log.Infof("analysis %s is not interactive, so move along", analysis.ID)
			return
		}

		// Get the list of status updates from analyses
		updates, err := lookupStatusUpdates(analysesBaseURL, analysis.ID)
		if err != nil {
			log.Error(errors.Wrapf(err, "error looking up status updates for analysis '%s'", analysis.ID))
			return
		}

		// Count the running status updates
		var numRunning int
		for _, update := range updates.Updates {
			if update.Status == "Running" {
				numRunning = numRunning + 1
			}
		}

		if numRunning < 1 {
			log.Infof("number of Running updates is %d, skipping for now", numRunning)
			return
		}

		// Check to see if the planned_end_date is set for the analysis
		if *analysis.PlannedEndDate != 0 {
			// There's nothing to do here, move along
			return
		}

		// StartDate is in milliseconds, so convert it to nanoseconds, add 48 hours,
		// then convert back to milliseconds.
		endDate := time.Unix(0, analysis.StartDate*1000000).Add(48*time.Hour).UnixNano() / 1000000
		if err = setPlannedEndDate(analysesBaseURL, analysis.ID, endDate); err != nil {
			log.Error(errors.Wrapf(err, "error setting planned end date for analysis '%s' to '%d'", analysis.ID, endDate))
		}
	}
}
