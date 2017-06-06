package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cyverse-de/timelord/notifications"
	"github.com/cyverse-de/timelord/queries"
	"github.com/cyverse-de/timelord/users"
)

func TestEnforceLimits(t *testing.T) {
	j := &queries.RunningJob{
		InvocationID: "invocation-id",
		TimeLimit:    1,
		StartOn:      time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).UnixNano() / 1000000,
	}
	toggle := false
	testcb := func(j *queries.RunningJob) error {
		toggle = true
		return nil
	}
	err := enforceLimit(j, testcb)
	if err != nil {
		t.Error(err)
	}
	if !toggle {
		t.Errorf("time enforcement did not trigger")
	}

	// test skipping jobs that have a time limit of 0
	j.TimeLimit = 0
	toggle = false
	if err = enforceLimit(j, testcb); err != nil {
		t.Error(err)
	}
	if toggle {
		t.Errorf("time was enforced when the limit was 0")
	}

	// test skipping the enforcement when the limit is still in the future.
	j.TimeLimit = 10                            //seconds
	j.StartOn = time.Now().UnixNano() / 1000000 // now in milliseconds since epoch
	toggle = false
	if err = enforceLimit(j, testcb); err != nil {
		t.Error(err)
	}
	if toggle {
		t.Errorf("time was enforced when the limit was in the future")
	}

	// test error from enforcement callback
	j.TimeLimit = 1
	j.StartOn = time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).UnixNano() / 1000000
	toggle = false
	testcb2 := func(j *queries.RunningJob) error {
		toggle = true
		return errors.New("test")
	}
	if err = enforceLimit(j, testcb2); err == nil {
		t.Error("no error returned from callback")
	}
}

func TestSendNotif(t *testing.T) {
	notifts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("OK")
	}))
	notifications.URI = notifts.URL

	usersts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		expected := users.New("id")
		expected.Name = "first-name last-name"
		expected.FirstName = "first-name"
		expected.LastName = "last-name"
		expected.Email = "id@example.com"
		expected.Institution = "institution"
		expected.SourceID = "source-id"

		msg, err := json.Marshal(expected)
		if err != nil {
			t.Error(err)
		}
		w.Write(msg)
	}))
	users.URI = usersts.URL

	j := &queries.RunningJob{
		InvocationID: "invocation-id",
		TimeLimit:    1,
		StartOn:      time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).UnixNano() / 1000000,
	}

	err := sendNotif(j, "test-subject", "test-msg")
	if err != nil {
		t.Error(err)
	}
}
