//nolint
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUsersInit(t *testing.T) {
	expected := "test"
	UsersInit(expected)
	actual := UsersURI

	if actual != expected {
		t.Errorf("URI was %s, not %s", actual, expected)
	}
}

func TestNewUser(t *testing.T) {
	expectedURI := "test-URI"
	expectedID := "test-user"
	UsersInit(expectedURI)
	u := NewUser(expectedID)

	if u.URI != expectedURI {
		t.Errorf("URI was %s, not %s", u.URI, expectedURI)
	}

	if u.ID != expectedID {
		t.Errorf("id was %s, not %s", u.ID, expectedID)
	}
}

func TestGet(t *testing.T) {
	expectedURI := "URI"
	UsersInit(expectedURI)

	expected := NewUser("id")
	expected.Name = "first-name last-name"
	expected.FirstName = "first-name"
	expected.LastName = "last-name"
	expected.Email = "id@example.com"
	expected.Institution = "institution"
	expected.SourceID = "source-id"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		actualPath := r.URL.Path
		expectedPath := fmt.Sprintf("/subjects/%s", expected.ID)
		if actualPath != expectedPath {
			t.Errorf("path was %s, not %s", actualPath, expectedPath)
		}
		msg, err := json.Marshal(expected)
		if err != nil {
			t.Error(err)
		}
		w.Write(msg)
	}))
	defer srv.Close()

	actual := NewUser("id")
	actual.URI = srv.URL
	err := actual.Get()
	if err != nil {
		t.Error(err)
	}

	if actual.ID != expected.ID {
		t.Errorf("id was %s, not %s", actual.ID, expected.ID)
	}

	if actual.Name != expected.Name {
		t.Errorf("name was %s, not %s", actual.Name, expected.Name)
	}

	if actual.FirstName != expected.FirstName {
		t.Errorf("first name was %s, not %s", actual.FirstName, expected.FirstName)
	}

	if actual.LastName != expected.LastName {
		t.Errorf("last name was %s, not %s", actual.LastName, expected.LastName)
	}

	if actual.Email != expected.Email {
		t.Errorf("email was %s, not %s", actual.Email, expected.Email)
	}

	if actual.Institution != expected.Institution {
		t.Errorf("institution was %s, not %s", actual.Institution, expected.Institution)
	}

	if actual.SourceID != expected.SourceID {
		t.Errorf("source ID was %s, not %s", actual.SourceID, expected.SourceID)
	}
}

func TestParseID(t *testing.T) {
	tests := map[string]string{
		"test-user":                 "test-user",
		"test-user@example.com":     "test-user",
		"test@user@example.com":     "test@user",
		"test@user@one@example.com": "test@user@one",
	}
	for k, expected := range tests {
		actual := ParseID(k)
		if actual != expected {
			t.Errorf("id was %s, not %s", actual, expected)
		}
	}
}
