package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNotifsInit(t *testing.T) {
	expected := "foo"
	NotifsInit(expected)
	actual := NotifsURI
	if actual != expected {
		t.Errorf("uri was %s, not %s", actual, expected)
	}
}

func TestNewNotification(t *testing.T) {
	expectedURI := "foo"
	expectedUser := "user"
	expectedSubject := "subject"
	expectedTemplate := "analysis_status_change"
	NotifsInit(expectedURI)
	n := NewNotification(expectedUser, expectedSubject, "", true, expectedTemplate, nil)
	if n.URI != expectedURI {
		t.Errorf("URI was %s, not %s", n.URI, expectedURI)
	}
	if n.User != expectedUser {
		t.Errorf("User was %s, not %s", n.User, expectedUser)
	}
	if n.Subject != expectedSubject {
		t.Errorf("Subject was %s, not %s", n.Subject, expectedSubject)
	}
	if n.EmailTemplate != expectedTemplate {
		t.Errorf("EmailTemplate was %s, not %s", n.EmailTemplate, expectedTemplate)
	}
}

func TestSend(t *testing.T) {
	expectedUser := "test-user"
	expectedSubject := "test-subject"
	n := NewNotification(expectedUser, expectedSubject, "", true, "analysis_status_change", nil)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		n1 := &Notification{}
		err = json.Unmarshal(b, n1)
		if err != nil {
			t.Error(err)
		}
		if n1.User != expectedUser {
			t.Errorf("user was %s, not %s", n1.User, expectedUser)
		}
		if n1.Subject != expectedSubject {
			t.Errorf("subject was %s, not %s", n1.Subject, expectedSubject)
		}
		if n1.Type != "analysis" {
			t.Errorf("type was %s, not 'Analysis'", n1.Type)
		}
	}))
	defer srv.Close()

	n.URI = srv.URL

	resp, err := n.Send(context.Background())
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != 200 {
		t.Errorf("status code was %d, not 200", resp.StatusCode)
	}
}
