package main

import (
	"bytes"
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

// MessageFormat contains the parameterized message that gets sent to users when
// their job expires.
const MessageFormat = `Analysis "%s" (%s) had a configured end date of "%s" (%s), which has passed.

Output files should be available in the %s folder in iRODS.`

// SubjectFormat is the parameterized email subject that is used for the email
// that is sent to users when their job expires.
const SubjectFormat = "Analysis %s canceled due to time limit restrictions."

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
	AnalysisStartDate     string `json:"analysisstartdate"`
	AnalysisResultsFolder string `json:"analysisresultsfolder"`
	Email                 string `json:"email_address"`
	Action                string `json:"action"`
	User                  string `json:"user"`
}

// NewPayload returns a newly constructed *Payload
func NewPayload() *Payload {
	return &Payload{
		Action: "job_status_change",
	}
}

// NewNotification returns a newly initialized *Notification.
func NewNotification(user, subject, msg string, payload *Payload) *Notification {
	return &Notification{
		URI:           NotifsURI,
		Type:          "analysis",
		User:          user,
		Subject:       subject,
		Message:       msg,
		Email:         true,
		EmailTemplate: "analysis_status_change",
		Payload:       payload,
	}
}

// Send POSTs the notification to the URI.
func (n *Notification) Send() (*http.Response, error) {
	msg, err := json.Marshal(n)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal message for user %s with subject '%s'", n.User, n.Subject)
	}

	resp, err := http.Post(n.URI, "application/json", bytes.NewBuffer(msg))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to post notification")
	}

	return resp, nil
}
