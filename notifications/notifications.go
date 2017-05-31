package notifications

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

// URI the default URI for notification requests.
var URI string

// Init sets the default URI to send notifications to.
func Init(newuri string) {
	URI = newuri
}

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

// New returns a newly initialized *Notification.
func New(user, subject, msg string, payload *Payload) *Notification {
	return &Notification{
		URI:           URI,
		Type:          "Analysis",
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
