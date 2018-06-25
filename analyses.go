package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
)

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
	PlannedEndDate int64  `json:"planned_end_date"`
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
