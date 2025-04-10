package main

import (
	"context"
	"database/sql"
)

// JobKiller is responsible for killing jobs either in HTCondor or in K8s.
type JobKiller struct {
	K8sEnabled bool        // whether or not the VICE apps are running k8s
	AppsBase   string      // base URL for the apps service
	appExposer *AppExposer // base URL for the app-exposer serivce
}

func NewJobKiller(k8sEnabled bool, appsBase, aeBase string) (*JobKiller, error) {
	appExposer, err := NewAppExposer(aeBase)
	if err != nil {
		return nil, err
	}
	return &JobKiller{
		K8sEnabled: k8sEnabled,
		AppsBase:   appsBase,
		appExposer: appExposer,
	}, nil
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
	return j.appExposer.StopAnalyses(ctx, jobID, username)
}

func (j *JobKiller) requestAnalysisTermination(ctx context.Context, job *Job) error {
	return j.appExposer.VICESaveAndExit(ctx, job)
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
func (j *JobKiller) killK8sJob(ctx context.Context, dedb *sql.DB, job *Job) error {
	found, err := j.checkForAnalysisInCluster(ctx, job)
	if err != nil {
		return err
	}

	// Only terminate an analysis if it's actually running in the cluster.
	if found {
		return j.requestAnalysisTermination(ctx, job)
	}

	// TODO: Set the status of the analysis in the DE to "Completed" if it's
	// not in the cluster. That way it won't get picked up again to terminate.

	return nil
}
