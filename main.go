package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"

	_ "expvar"

	"github.com/cyverse-de/configurate"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"
)

const defaultConfig = `db:
  uri: "db:5432"
amqp:
  uri: "amqp://amqp:60000/de/de"
  exchange:
    name: "de"
    type: "topic"
notifications:
  base: http://notifications:60000
iplant_groups:
  base: http://iplant-groups
  user: grouper-user
`

var logger = logrus.WithFields(logrus.Fields{
	"service": "timelord",
	"art-id":  "timelord",
	"group":   "org.cyverse",
})

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}

func sendNotif(j *RunningJob, subject, msg string) error {
	var err error

	// Don't send notification if things aren't configured correctly. It's
	// technically not an error, for now.
	if NotifsURI == "" || UsersURI == "" {
		logger.Info("notification URI is %s and iplant-groups URI is %s", NotifsURI, UsersURI)
		return nil
	}

	// We need to get the user's email address from the iplant-groups service.
	user := NewUser(ParseID(j.Username))
	if err = user.Get(); err != nil {
		return errors.Wrap(err, "failed to get user info")
	}

	u := ParseID(j.Username)

	p := NewPayload()
	p.AnalysisName = j.AnalysisName
	p.AnalysisDescription = j.AnalysisDescription
	p.AnalysisStatus = j.AnalysisStatus
	p.AnalysisStartDate = j.AnalysisStartDate
	p.AnalysisResultsFolder = j.AnalysisResultFolderPath
	p.Email = user.Email
	p.User = u

	notif := NewNotification(u, subject, msg, p)

	resp, err := notif.Send()
	if err != nil {
		return errors.Wrap(err, "failed to send notification")
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read notification response body")
	}

	logger.Infof("notification: (invocation_id: %s, status: %s, body: %s)", j.InvocationID, resp.Status, b)

	return nil
}

///// Setting the planned_end_date
// Wait for the "Running" job status updates.
// Map the invocation ID to an analysis ID with the /admin/analyses/by-external-id/{external-id} endpoint.
// Get the app ID from the /analyses/{analysis-id}/parameters endpoint.
// Get the list of tools used in the analysis from the /apps/{system-id}/{app-id}/tools endpoint.
// If the tool is interactive, add up all of the time limits. If a tool has a time limit of 0, then add in the default of 8 hours.
// Use the PATCH /analyses/{analysis-id} to set the planned_end_date for the analysis.

///// Enforcing the planned_end_date
//

func main() {
	var (
		err        error
		cfg        *viper.Viper
		notifPath  = "/notification"
		configPath = flag.String("config", "/etc/iplant/de/timelord.yml", "The path to the YAML config file.")
		expvarPort = flag.String("port", "60000", "The path to listen for expvar requests on.")
	)

	flag.Parse()

	// make sure the configuration object has sane defaults.
	if cfg, err = configurate.InitDefaults(*configPath, defaultConfig); err != nil {
		log.Fatal(err)
	}

	// configure the notification emitters
	notifBase := cfg.GetString("notifications.base")
	notifURL, err := url.Parse(notifBase)
	if err != nil {
		logger.Error(errors.Wrapf(err, "failed to parse %s", notifBase))
	}
	notifURL.Path = notifPath
	NotifsInit(notifURL.String())

	// configure the user lookups
	groupsBase := cfg.GetString("iplant_groups.base")
	groupsUser := cfg.GetString("iplant_groups.user")
	groupsURL, err := url.Parse(groupsBase)
	if err != nil {
		logger.Error(errors.Wrapf(err, "failed to parse %s", groupsBase))
	}
	q := groupsURL.Query()
	q.Set("user", groupsUser)
	groupsURL.RawQuery = q.Encode()
	UsersInit(groupsURL.String())

	listenAddr := fmt.Sprintf(":%s", *expvarPort)
	logger.Infof("listening for expvar requests on %s", listenAddr)
	sock, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatal(err)
	}
	http.Serve(sock, nil)
}
