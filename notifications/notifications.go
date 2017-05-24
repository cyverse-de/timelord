package notifications

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pkg/errors"
)

var uri string // The default URI to post notifications to.

// Init sets the default URI to send notifications to.
func Init(newuri string) {
	uri = newuri
}

// Notification is a message intended as a notification to some upstream service
// or the DE UI.
type Notification struct {
	URI     string `json:"-"`       // The endpoint to POST the message to. Ignored for marshalling.
	Type    string `json:"type"`    // The notification type.
	User    string `json:"user"`    // The user that the message is sent to.
	Subject string `json:"subject"` // The subject line of the message. Usually contains the msg.
}

// New returns a newly initialized *Notification.
func New(user, subject string) *Notification {
	return &Notification{
		URI:     uri,
		Type:    "Analysis",
		User:    user,
		Subject: subject,
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
