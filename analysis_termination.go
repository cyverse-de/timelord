package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

// JobKiller is responsible for killing jobs either in HTCondor or in K8s.
type JobKiller struct {
	K8sEnabled bool        // whether or not the VICE apps are running k8s
	AppsBase   string      // base URL for the apps service
	appExposer *AppExposer // client for the app-exposer service
	dedb       *sql.DB
	jslURL     *url.URL
}

func NewJobKiller(k8sEnabled bool, appsBase, aeBase string, dedb *sql.DB, jslURL *url.URL) (*JobKiller, error) {
	appExposer, err := NewAppExposer(aeBase)
	if err != nil {
		return nil, err
	}
	return &JobKiller{
		K8sEnabled: k8sEnabled,
		AppsBase:   appsBase,
		appExposer: appExposer,
		dedb:       dedb,
		jslURL:     jslURL,
	}, nil
}

// KillJob uses either the apps or app-exposer APIs to kill a VICE job.
func (j *JobKiller) KillJob(ctx context.Context, job *Job) error {
	if j.K8sEnabled {
		return j.killK8sJob(ctx, job)
	}
	return j.killCondorJob(ctx, job.ID, job.User)

}

// killCondorJob uses the provided API at the base URL to kill a running job. This
// will probably be to the apps service. jobID should be the UUID for the Job,
// typically returned in the ID field by the analyses service. The username
// should be the short username for the user that launched the job.
func (j *JobKiller) killCondorJob(ctx context.Context, jobID, username string) error {
	return j.appExposer.StopAnalyses(ctx, jobID, username)
}

func (j *JobKiller) requestAnalysisTermination(ctx context.Context, job *Job) error {
	return j.appExposer.VICESaveAndExit(ctx, job)
}

func (j *JobKiller) sendCompletedStatus(ctx context.Context, job *Job) error {
	updateEndpoint := *j.jslURL
	updateEndpoint.Path = path.Join(updateEndpoint.Path, job.ExternalID, "status")

	postBody := map[string]string{
		"Hostname": "timelord",
		"Message":  "Set state to Completed",
		"State":    "Completed",
	}

	postJSON, err := json.Marshal(postBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, updateEndpoint.String(), bytes.NewBuffer(postJSON))
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status code from job-status-listener was %d", resp.StatusCode)
	}

	return nil
}

// checkForAnalysisInCluster returns whether the analysis is present in cluster in some
// state. Calls out to app-exposer.
func (j *JobKiller) checkForAnalysisInCluster(ctx context.Context, analysis *Analysis) (bool, error) {
	var (
		err   error
		found bool
	)

	listing, err := j.appExposer.AdminListing(ctx, analysis)
	if err != nil {
		return found, err
	}

	if len(listing.Deployments) > 0 {
		found = true
	}

	return found, nil
}

// killK8sJob uses the app-exposer API to make a job save its outputs and exit.
// JobID should be the external_id (AKA invocationID) for the job.
func (j *JobKiller) killK8sJob(ctx context.Context, job *Job) error {
	return j.requestAnalysisTermination(ctx, job)
}
