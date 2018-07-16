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
  base: http://notification-agent
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

func sendNotif(j *Job, status, subject, msg string) error {
	var err error

	// Don't send notification if things aren't configured correctly. It's
	// technically not an error, for now.
	if NotifsURI == "" || UsersURI == "" {
		logger.Infof("notification URI is %s and iplant-groups URI is %s", NotifsURI, UsersURI)
		return nil
	}

	// We need to get the user's email address from the iplant-groups service.
	user := NewUser(ParseID(j.User.Username))
	if err = user.Get(); err != nil {
		return errors.Wrap(err, "failed to get user info")
	}

	u := ParseID(j.User.Username)
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
	return sendNotif(j, "Canceled", subject, msg)
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

func main() {
	var (
		err             error
		cfg             *viper.Viper
		notifPath       = "/notification"
		configPath      = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the YAML config file.")
		expvarPort      = flag.String("port", "60000", "The path to listen for expvar requests on.")
		appsBase        = flag.String("apps", "http://apps", "The base URL for the apps service.")
		analysesBase    = flag.String("analyses", "http://analyses", "The base URL for analyses service.")
		graphqlBase     = flag.String("graphql", "http://hasura/v1alpha1/graphql", "The base URL for the graphql provider.")
		warningInterval = flag.Int64("warning-interval", 60, "The number of minutes in advance to warn users about job kills.")
		warningSentKey  = flag.String("warning-sent-key", "warningsent", "The key for the Redis set containing job IDs as members. Used to track warning notifications.")
	)

	flag.Parse()

	// make sure the configuration object has sane defaults.
	if cfg, err = configurate.InitDefaults(*configPath, defaultConfig); err != nil {
		log.Fatal(err)
	}

	logger.Info("configuring notification support...")
	// configure the notification emitters
	if err = ConfigureNotifications(cfg, notifPath); err != nil {
		log.Fatal(err)
	}
	logger.Info("done configuring notification support")

	logger.Info("configuring user lookups...")
	// configure the user lookups
	if err = ConfigureUserLookups(cfg); err != nil {
		log.Fatal(err)
	}
	logger.Info("done configuring user lookups")

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

	logger.Info("configuring messaging support...")
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
		CreateMessageHandler(*graphqlBase, *analysesBase),
		0,
	)
	logger.Info("done configuring messaging support")

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

	logger.Info("configuring redis support...")
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
	logger.Info("done configuring redis support")

	go func() {
		var (
			jl       []Job
			warnings []Job
			sent     bool
		)

		for {
			warnings, err = JobKillWarnings(*graphqlBase, *warningInterval)
			if err != nil {
				logger.Error(err)
			} else {
				for _, w := range warnings {
					logger.Infof("checking redis set to see if warning has already been sent for analysis %s", w.ID)
					sent, err = redisclient.SIsMember(*warningSentKey, w.ID).Result()
					if err != nil {
						logger.Error(err)
						continue
					}

					if !sent {
						logger.Infof("sending warning notification for analysis %s", w.ID)
						if err = SendWarningNotification(&w); err != nil {
							logger.Error(err)
						} else {
							logger.Infof("adding analysis ID %s to redis set to mark warning as having been sent", w.ID)
							if err = redisclient.SAdd(*warningSentKey, w.ID).Err(); err != nil {
								logger.Error(err)
							}
						}
					}
				}
			}

			logger.Info("getting list of analyses that need to be terminated")
			jl, err = JobsToKill(*graphqlBase)
			if err != nil {
				logger.Error(err)
				continue
			}

			for _, j := range jl {
				logger.Infof("analysis %s is being terminated", j.ID)
				if err = KillJob(*appsBase, j.ID, j.User.Username); err != nil {
					logger.Error(err)
				} else {
					logger.Infof("sending notification that %s has been terminated", j.ID)
					if err = SendKillNotification(&j); err != nil {
						logger.Error(err)
					}
					logger.Infof("removing analysis ID %s from the redis set", j.ID)
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
