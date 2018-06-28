package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	_ "expvar"

	"github.com/cyverse-de/configurate"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"

	"gopkg.in/cyverse-de/messaging.v4"
)

const defaultConfig = `db:
  uri: "db:5432"
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

func sendNotif(j *Job, subject, msg string) error {
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
	p.AnalysisName = j.Name
	p.AnalysisDescription = j.Description
	p.AnalysisStatus = j.Status
	p.AnalysisStartDate = strconv.FormatInt(j.StartDate, 10)
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

	logger.Infof("notification: (invocation_id: %s, status: %s, body: %s)", j.ID, resp.Status, b)

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
func SendKillNotification(j *Job) error {
	subject := fmt.Sprintf(KillSubjectFormat, j.ID)
	endtime := time.Unix(0, j.PlannedEndDate*1000000)
	msg := fmt.Sprintf(
		KillMessageFormat,
		j.Name,
		j.ID,
		endtime.Format("Mon Jan 2 15:04:05 -0700 MST 2006"),
		endtime.UTC().Format(time.UnixDate),
		j.ResultFolder,
	)

	return sendNotif(j, subject, msg)
}

// SendWarningNotification sends a notification to the user telling them that
// their job will be killed in the near future.
func SendWarningNotification(j *Job) error {
	endtime := time.Unix(0, j.PlannedEndDate*1000000)
	endtimeMST := endtime.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	endtimeUTC := endtime.UTC().Format(time.UnixDate)
	subject := fmt.Sprintf(WarningSubjectFormat, j.ID, endtimeMST, endtimeUTC)

	msg := fmt.Sprintf(
		WarningMessageFormat,
		j.Name,
		j.ID,
		endtimeMST,
		endtimeUTC,
		j.ResultFolder,
	)

	return sendNotif(j, subject, msg)
}

func main() {
	var (
		err             error
		cfg             *viper.Viper
		notifPath       = "/notification"
		configPath      = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the YAML config file.")
		expvarPort      = flag.String("port", "60000", "The path to listen for expvar requests on.")
		appsBase        = flag.String("apps", "http://apps", "The base URL for the apps service.")
		analysesBase    = flag.String("analyses", "http://analyses", "The base URL for analyses service.")
		warningInterval = flag.Int64("warning-interval", 10, "The number of minutes in advance to warn users about job kills.")
		warningSentKey  = flag.String("warning-sent-key", "warningsent", "The key for the Redis set containing job IDs as members. Used to track warning notifications.")
	)

	flag.Parse()

	// make sure the configuration object has sane defaults.
	if cfg, err = configurate.InitDefaults(*configPath, defaultConfig); err != nil {
		log.Fatal(err)
	}

	// configure the notification emitters
	if err = ConfigureNotifications(cfg, notifPath); err != nil {
		log.Fatal(err)
	}

	// configure the user lookups
	if err = ConfigureUserLookups(cfg); err != nil {
		log.Fatal(err)
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

	amqpclient, err := messaging.NewClient(amqpURI, false)
	if err != nil {
		log.Fatal(err)
	}
	defer amqpclient.Close()

	go amqpclient.Listen()

	redishost := cfg.GetString("redis.host")
	if redishost == "" {
		log.Fatal("redis.host must be set in the config file")
	}

	redisport := cfg.GetInt("redis.port")
	if redisport == 0 {
		log.Fatal("redis.port must be set in the config file")
	}

	redispass := cfg.GetString("redis.password")
	if redispass == "" {
		log.Fatal("redis.password must be set in the config file")
	}

	redisdb := cfg.GetInt("redis.db.number")

	redisclient := redis.NewClient(
		&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", redishost, redisport),
			Password: redispass,
			DB:       redisdb,
		},
	)

	_, err = redisclient.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		var (
			jl, warnings *JobList
			sent         bool
		)

		for {
			warnings, err = JobKillWarnings(*analysesBase, *warningInterval)
			if err != nil {
				logger.Error(err)
			} else {
				for _, w := range warnings.Jobs {
					sent, err = redisclient.SIsMember(*warningSentKey, w.ID).Result()
					if err != nil {
						logger.Error(err)
						continue
					}

					if !sent {
						if err = SendWarningNotification(&w); err != nil {
							logger.Error(err)
						} else {
							if err = redisclient.SAdd(*warningSentKey, w.ID).Err(); err != nil {
								logger.Error(err)
							}
						}
					}
				}
			}

			jl, err = JobsToKill(*analysesBase)
			if err != nil {
				logger.Error(err)
				continue
			}

			for _, j := range jl.Jobs {
				if err = KillJob(*appsBase, j.ID, j.Username); err != nil {
					logger.Error(err)
				} else {
					if err = SendKillNotification(&j); err != nil {
						logger.Error(err)
					}
					if _, err = redisclient.SRem(*warningSentKey, j.ID).Result(); err != nil {
						logger.Error(err)
					}
				}
			}

			time.Sleep(time.Second * 10)
		}
	}()

	listenAddr := fmt.Sprintf(":%s", *expvarPort)
	logger.Infof("listening for expvar requests on %s", listenAddr)
	sock, err := net.Listen("tcp", listenAddr)
	if err != nil {
		logger.Fatal(err)
	}
	http.Serve(sock, nil)
}
