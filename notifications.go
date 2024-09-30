package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

// NotifsURI the default URI for notification requests.
var NotifsURI string

// NotifsInit sets the default URI to send notifications to.
func NotifsInit(newuri string) {
	NotifsURI = newuri
}

// KillMessageFormat contains the parameterized message that gets sent to users when
// their job expires.
const KillMessageFormat = `Analysis "%s" (%s) had a configured end date of "%s" (%s), which has passed.

Output files should be available in the %s folder in iRODS.`

// KillSubjectFormat is the parameterized email subject that is used for the email
// that is sent to users when their job expires.
const KillSubjectFormat = "Analysis %s canceled due to time limit restrictions."

// WarningMessageFormat is the parameterized message that gets send to users
// when their job is going to expire in the near future.
const WarningMessageFormat = `Analysis "%s" (%s) is set to expire on "%s" (%s).

Please finish any work that is in progress. Output files will be transferred to the %s folder in iRODS when the application shuts down.`

// WarningSubjectFormat is the parameterized subject for the email that is sent
// to users when their job is going to terminate in the near future.
const WarningSubjectFormat = "Analysis %s will terminate on %s (%s)."

// PeriodicMessageFormat is the parameterized message that gets sent to users
// when it's time to send a regular reminder the job is still running
// parameters: analysis name, current duration, duration until planned end date
const PeriodicMessageFormat = `Analysis "%s" has been running for %s and will stop in %s.`

// PeriodicSubjectFormat is the subject for the email that is sent
// to users as a regular reminder of a running job
// no parameters, as it is vague on purpose to encourage opening the email with full details
const PeriodicSubjectFormat = `CyVerse: Your analysis is still running`

// Notification is a message intended as a notification to some upstream service
// or the DE UI.
type Notification struct {
	URI           string   `json:"-"`       // The endpoint to POST the message to. Ignored for marshalling.
	Type          string   `json:"type"`    // The notification type.
	User          string   `json:"user"`    // The user that the message is sent to.
	Subject       string   `json:"subject"` // The subject line of the message. Usually contains the msg.
	Message       string   `json:"message"`
	Email         bool     `json:"email"`
	EmailTemplate string   `json:"email_template"`
	Payload       *Payload `json:"payload"`
}

// Payload is the information needed for Analysis notifications
type Payload struct {
	AnalysisName          string `json:"analysisname"`
	AnalysisDescription   string `json:"analysisdescription"`
	AnalysisStatus        string `json:"analysisstatus"`
	StartDate             string `json:"startdate"`
	AnalysisResultsFolder string `json:"analysisresultsfolder"`
	RunDuration           string `json:"runduration"`
	EndDuration           string `json:"endduration"`
	AccessURL             string `json:"access_url"`
	Email                 string `json:"email_address"`
	Action                string `json:"action"`
	User                  string `json:"user"`
}

// NewPayload returns a newly constructed *Payload with the Action set to "job_status_change"
func NewPayload() *Payload {
	return &Payload{
		Action: "job_status_change",
	}
}

// NewNotification returns a newly initialized *Notification.
func NewNotification(user, subject, msg string, email bool, emailTemplate string, payload *Payload) *Notification {
	return &Notification{
		URI:           NotifsURI,
		Type:          "analysis",
		User:          user,
		Subject:       subject,
		Message:       msg,
		Email:         email,
		EmailTemplate: emailTemplate,
		Payload:       payload,
	}
}

// Send POSTs the notification to the URI.
func (n *Notification) Send(ctx context.Context) (*http.Response, error) {
	msg, err := json.Marshal(n)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal message for user %s with subject '%s'", n.User, n.Subject)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URI, bytes.NewBuffer(msg))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to post notification")
	}
	req.Header.Set("content-type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to post notification")
	}

	return resp, nil
}
