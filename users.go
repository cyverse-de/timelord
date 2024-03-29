package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
)

// UsersURI the default URI for user lookup requests
var UsersURI string

// UsersInit initializes the default URI used for requests to the iplant-groups service.
func UsersInit(u string) {
	UsersURI = u
}

// User contains information about a user that was returned by various services
// in the backend. For now, it all comes from the iplant-groups service.
type User struct {
	URI         string `json:"-"`
	ID          string `json:"id"`   // The non-UUID identifier for a user. Usually the username.
	Name        string `json:"name"` // The full name
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	Email       string `json:"email"`
	Institution string `json:"institution"`
	SourceID    string `json:"source_id"`
}

// NewUser returns a newly instantiated *User.
func NewUser(id string) *User {
	return &User{
		URI: UsersURI,
		ID:  id,
	}
}

// Get populates the *User with information. Blocks and makes calls to at least
// the iplant-groups service.
func (u *User) Get(ctx context.Context) error {
	url, err := url.Parse(u.URI)
	if err != nil {
		return errors.Wrap(err, "failed to parse user lookup URL")
	}

	url.Path = fmt.Sprintf("/subjects/%s", u.ID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url.String(), nil)
	if err != nil {
		return errors.Wrapf(err, "failed to GET user information from %s", url.String())
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to GET user information from %s", url.String())
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body for user lookup request")
	}

	if resp.StatusCode < 200 || resp.StatusCode > 200 {
		return errors.Wrapf(err, "failed user lookup (status: %s, msg %s)", resp.Status, b)
	}

	if err = json.Unmarshal(b, u); err != nil {
		return errors.Wrap(err, "failed to unmarshal user lookup response")
	}

	return nil
}

// ParseID returns a user's ID from their username. Right now it's basically
// anything to the left of the last @ in their username.
func ParseID(username string) string {
	hasAt := strings.Contains(username, "@")
	if !hasAt {
		return username
	}
	parts := strings.Split(username, "@")
	return strings.Join(parts[:len(parts)-1], "@")
}
