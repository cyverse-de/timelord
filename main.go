package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	_ "expvar"

	"github.com/cyverse-de/configurate"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"

	"gopkg.in/cyverse-de/messaging.v4"
)

const defaultConfig = `db:
  uri: "db:5432"
notifications:
  base: http://notification-agent
iplant_groups:
  base: http://iplant-groups
  user: grouper-user
`

const warningSentKey = "warningsent"
const oneDayWarningKey = "onedaywarning"

func sendNotif(j *Job, status, subject, msg string) error {
	var err error

	// Don't send notification if things aren't configured correctly. It's
	// technically not an error, for now.
	if NotifsURI == "" || UsersURI == "" {
		log.Infof("notification URI is %s and iplant-groups URI is %s", NotifsURI, UsersURI)
		return nil
	}

	// We need to get the user's email address from the iplant-groups service.
	user := NewUser(ParseID(j.User))
	if err = user.Get(); err != nil {
		return errors.Wrap(err, "failed to get user info")
	}

	u := ParseID(j.User)
	sd, err := time.Parse(TimestampFromDBFormat, j.StartDate)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %s", j.StartDate)
	}
	sdmillis := sd.UnixNano() / 1000000

	p := NewPayload()
	p.AnalysisName = j.Name
	p.AnalysisDescription = j.Description
	p.AnalysisStatus = status
	p.AnalysisStartDate = strconv.FormatInt(sdmillis, 10)
	p.AnalysisResultsFolder = j.ResultFolder
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

	log.Infof("notification: (invocation_id: %s, status: %s, body: %s)", j.ID, resp.Status, b)

	return nil
}

// ConfigureNotifications sets up the notification emitters.
func ConfigureNotifications(cfg *viper.Viper, notifPath string) error {
	notifBase := cfg.GetString("notifications.base")
	notifURL, err := url.Parse(notifBase)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %s", notifBase)
	}
	notifURL.Path = notifPath
	NotifsInit(notifURL.String())
	return nil
}

// ConfigureUserLookups sets up the api for getting user information.
func ConfigureUserLookups(cfg *viper.Viper) error {
	groupsBase := cfg.GetString("iplant_groups.base")
	groupsUser := cfg.GetString("iplant_groups.user")
	groupsURL, err := url.Parse(groupsBase)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %s", groupsBase)
	}
	q := groupsURL.Query()
	q.Set("user", groupsUser)
	groupsURL.RawQuery = q.Encode()
	UsersInit(groupsURL.String())
	return nil
}

// SendKillNotification sends a notification to the user telling them that
// their job has been killed.
func SendKillNotification(j *Job, killNotifKey string) error {
	subject := fmt.Sprintf(KillSubjectFormat, j.Name)
	endtime, err := time.Parse(TimestampFromDBFormat, j.PlannedEndDate)
	if err != nil {
		return errors.Wrapf(err, "failed to parse planned end date %s", j.PlannedEndDate)
	}
	msg := fmt.Sprintf(
		KillMessageFormat,
		j.Name,
		j.ID,
		endtime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"),
		endtime.UTC().Format(time.UnixDate),
		j.ResultFolder,
	)
	err = sendNotif(j, "Canceled", subject, msg)
	return err
}

// SendWarningNotification sends a notification to the user telling them that
// their job will be killed in the near future.
func SendWarningNotification(j *Job) error {
	endtime, err := time.Parse(TimestampFromDBFormat, j.PlannedEndDate)
	if err != nil {
		return errors.Wrapf(err, "failed to parse planned end date %s", j.PlannedEndDate)
	}
	endtimeMST := endtime.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	endtimeUTC := endtime.UTC().Format(time.UnixDate)
	subject := fmt.Sprintf(WarningSubjectFormat, j.Name, endtimeMST, endtimeUTC)

	msg := fmt.Sprintf(
		WarningMessageFormat,
		j.Name,
		j.ID,
		endtimeMST,
		endtimeUTC,
		j.ResultFolder,
	)

	return sendNotif(j, j.Status, subject, msg)
}

const maxAttempts = 3

func sendWarning(db *sql.DB, vicedb *VICEDatabaser, warningInterval int64, warningKey string) {
	jobs, err := JobKillWarnings(db, warningInterval)
	if err != nil {
		log.Error(err)
	} else {
		for _, j := range jobs {
			var (
				wasSent            bool
				notifStatuses      *NotifStatuses
				failureCount       int
				updateWarningSent  func(*Job, bool) error
				updateFailureCount func(*Job, int) error
			)

			analysisRecordExists := vicedb.AnalysisRecordExists(j.ID)

			if !analysisRecordExists {
				if _, err = vicedb.AddNotifRecord(&j); err != nil {
					log.Error(err)
					continue
				}
			}

			notifStatuses, err = vicedb.NotifStatuses(&j)
			if err != nil {
				log.Error(err)
				continue
			}

			switch warningKey {
			case warningSentKey: // one hour warning
				wasSent = notifStatuses.HourWarningSent
				failureCount = notifStatuses.HourWarningFailureCount
				updateWarningSent = vicedb.SetHourWarningSent
				updateFailureCount = vicedb.SetHourWarningFailureCount
			case oneDayWarningKey: // one day warning
				wasSent = notifStatuses.DayWarningSent
				failureCount = notifStatuses.DayWarningFailureCount
				updateWarningSent = vicedb.SetDayWarningSent
				updateFailureCount = vicedb.SetDayWarningFailureCount
			default:
				err = fmt.Errorf("unknown warning key: %s", warningKey)
			}

			if err != nil {
				log.Error(err)
				continue
			}

			log.Warnf("external ID %s has been warned of possible termination: %v", j.ExternalID, wasSent)

			if !wasSent {
				if err = SendWarningNotification(&j); err != nil {
					log.Error(errors.Wrapf(err, "error sending warning notification for analysis %s", j.ExternalID))

					failureCount = failureCount + 1

					if err = updateFailureCount(&j, failureCount); err != nil {
						log.Error(err)
					}
				}

				if err == nil || failureCount >= maxAttempts {
					if err = updateWarningSent(&j, true); err != nil {
						log.Error(err)
						continue
					}
				}
			}
		}
	}
}

func main() {
	log.SetReportCaller(true)

	var (
		err error
		cfg *viper.Viper

		notifPath       = "/notification"
		configPath      = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the YAML config file.")
		expvarPort      = flag.String("port", "60000", "The path to listen for expvar requests on.")
		appExposerBase  = flag.String("app-exposer", "http://app-exposer", "The base URL for the app-exposer service.")
		killNotifKey    = flag.String("kill-notif-key", "killnotifsent", "The key for the annotation detailing whether the notification about job termination was sent.")
		warningInterval = flag.Int64("warning-interval", 60, "The number of minutes in advance to warn users about job kills.")
		warningSentKey  = flag.String("warning-sent-key", warningSentKey, "The key for the annotation detailing whether the job termination warning was sent.")
	)
	flag.Parse()

	// make sure the configuration object has sane defaults.
	if cfg, err = configurate.InitDefaults(*configPath, defaultConfig); err != nil {
		log.Fatal(err)
	}

	log.Info("configuring notification support...")
	// configure the notification emitters
	if err = ConfigureNotifications(cfg, notifPath); err != nil {
		log.Fatal(err)
	}
	log.Info("done configuring notification support")

	log.Info("configuring user lookups...")
	// configure the user lookups
	if err = ConfigureUserLookups(cfg); err != nil {
		log.Fatal(err)
	}
	log.Info("done configuring user lookups")

	k8sEnabled := cfg.GetBool("vice.k8s-enabled")
	appsBase := cfg.GetString("apps.base")

	if appsBase == "" {
		log.Fatal("apps.base must be set in the configuration file")
	}

	amqpURI := cfg.GetString("amqp.uri")
	if amqpURI == "" {
		log.Fatal("amqp.uri must be set in the config file")
	}

	exchange := cfg.GetString("amqp.exchange.name")
	if exchange == "" {
		log.Fatal("amqp.exchange.name must be set in the config file")
	}

	exchangeType := cfg.GetString("amqp.exchange.type")
	if exchangeType == "" {
		log.Fatal("amqp.exchange.type must be set in the config file")
	}

	dbURI := cfg.GetString("db.uri")
	if dbURI == "" {
		log.Fatal("db.uri must be set in the config file")
	}

	db, err := sql.Open("postgres", dbURI)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "error connecting to database %s", dbURI))
	}

	if err = db.Ping(); err != nil {
		log.Fatal(errors.Wrapf(err, "error pinging database %s", dbURI))
	}

	vicedb := &VICEDatabaser{
		db: db,
	}

	log.Info("configuring messaging support...")
	amqpclient, err := messaging.NewClient(amqpURI, false)
	if err != nil {
		log.Fatal(err)
	}
	defer amqpclient.Close()

	go amqpclient.Listen()

	amqpclient.AddConsumer(
		exchange,
		exchangeType,
		"timelord",
		messaging.UpdatesKey,
		CreateMessageHandler(db),
		100,
	)
	log.Info("done configuring messaging support")

	jobKiller := &JobKiller{
		K8sEnabled:     k8sEnabled,
		AppsBase:       appsBase,
		AppExposerBase: *appExposerBase,
	}

	go func() {
		var jl []Job

		for {
			// 1 hour warning
			sendWarning(db, vicedb, *warningInterval, *warningSentKey)

			// 1 day warning
			sendWarning(db, vicedb, 1440, oneDayWarningKey)

			jl, err = JobsToKill(db)
			if err != nil {
				log.Error(errors.Wrap(err, "error getting list of jobs to kill"))
				continue
			}

			for _, j := range jl {
				analysisRecordExists := vicedb.AnalysisRecordExists(j.ID)

				if !analysisRecordExists {
					if _, err = vicedb.AddNotifRecord(&j); err != nil {
						log.Error(err)
						continue
					}
				}

				var notifStatuses *NotifStatuses

				notifStatuses, err = vicedb.NotifStatuses(&j)
				if err != nil {
					log.Error(err)
					continue
				}

				if !notifStatuses.KillWarningSent {
					err = jobKiller.KillJob(db, &j)
					if err != nil {
						log.Error(errors.Wrapf(err, "error terminating analysis '%s'", j.ID))
					} else {

						err = SendKillNotification(&j, *killNotifKey)
						if err != nil {
							log.Error(errors.Wrapf(err, "error sending notification that %s has been terminated", j.ID))
						}
					}

					if err != nil {
						notifStatuses.KillWarningFailureCount = notifStatuses.KillWarningFailureCount + 1

						if err = vicedb.SetKillWarningFailureCount(&j, notifStatuses.KillWarningFailureCount); err != nil {
							log.Error(err)
							continue
						}
					}

					if err == nil || notifStatuses.KillWarningFailureCount >= maxAttempts {
						if err = vicedb.SetKillWarningSent(&j, true); err != nil {
							log.Error(err)
							continue
						}
					}
				}
			}

			time.Sleep(time.Second * 10)
		}
	}()

	listenAddr := fmt.Sprintf(":%s", *expvarPort)
	log.Infof("listening for expvar requests on %s", listenAddr)
	sock, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	http.Serve(sock, nil)
}
