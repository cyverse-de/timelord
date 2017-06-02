package notifications

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInit(t *testing.T) {
	expected := "foo"
	Init(expected)
	actual := URI
	if actual != expected {
		t.Errorf("uri was %s, not %s", actual, expected)
	}
}

func TestNew(t *testing.T) {
	expectedURI := "foo"
	expectedUser := "user"
	expectedSubject := "subject"
	Init(expectedURI)
	n := New(expectedUser, expectedSubject, "", nil)
	if n.URI != expectedURI {
		t.Errorf("URI was %s, not %s", n.URI, expectedURI)
	}
	if n.User != expectedUser {
		t.Errorf("User was %s, not %s", n.User, expectedUser)
	}
	if n.Subject != expectedSubject {
		t.Errorf("Subject was %s, not %s", n.Subject, expectedSubject)
	}
}

func TestSend(t *testing.T) {
	expectedUser := "test-user"
	expectedSubject := "test-subject"
	n := New(expectedUser, expectedSubject, "", nil)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
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
		if n1.Type != "Analysis" {
			t.Errorf("type was %s, not 'Analysis'", n1.Type)
		}
	}))
	defer srv.Close()

	n.URI = srv.URL

	resp, err := n.Send()
	if err != nil {
		t.Error(err)
	}

	if resp.StatusCode != 200 {
		t.Errorf("status code was %d, not 200", resp.StatusCode)
	}
}