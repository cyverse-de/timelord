package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/messaging"
	"github.com/cyverse-de/timelord/queries"
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
`

var logger = logrus.WithFields(logrus.Fields{
	"service": "timelord",
	"art-id":  "timelord",
	"group":   "org.cyverse",
})

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
}

type enforcer func(j *queries.RunningJob) error

func jobStopperCallback(client *messaging.Client) enforcer {
	return func(j *queries.RunningJob) error {
		return client.SendStopRequest(
			j.InvocationID,
			"timelord",
			"time limit exceeded",
		)
	}
}

func enforceLimit(j *queries.RunningJob, e enforcer) error {
	// don't enforce a time limit if it's set to 0.
	if j.TimeLimit == 0 {
		return nil
	}

	limit, err := time.ParseDuration(fmt.Sprintf("%ds", j.TimeLimit))
	if err != nil {
		return errors.Wrapf(err, "failed to parse duration for %d", j.TimeLimit)
	}

	sentOn := time.Unix(0, j.StartOn*1000000) // convert milliseconds to nanoseconds
	n := time.Now()
	limitDate := sentOn.Add(limit)

	if n.After(limitDate) {
		logger.Info("current date %s is after time limit date %s", n, limitDate)

		if err = e(j); err != nil {
			return errors.Wrap(err, "failed to enforce limit")
		}
	}

	return nil
}

func action(db *sql.DB, client *messaging.Client, cb enforcer) error {
	var err error

	// try pinging the database to make sure the connection works
	if err = db.Ping(); err != nil {
		return errors.Wrapf(err, "error pinging database")
	}

	jobs, err := queries.LookupRunningJobs(db)
	if err != nil {
		return errors.Wrapf(err, "failed to look up running jobs")
	}
	logger.Infof("found %d running jobs", len(jobs))

	for _, j := range jobs {
		logger.Infof("checking time limits for %s", j.InvocationID)

		if err = enforceLimit(&j, cb); err != nil {
			logger.Error(errors.Wrapf(err, "failed to enforce limit for %s", j.InvocationID))
		}
	}

	return nil
}

func main() {
	var (
		err        error
		cfg        *viper.Viper
		configPath = flag.String("config", "/etc/iplant/de/timelord.yml", "The path to the YAML config file.")
	)

	flag.Parse()

	// set up the info level logger for the messaging package
	infolog := logger.WriterLevel(logrus.InfoLevel)
	defer infolog.Close()
	messaging.Info = log.New(infolog, "", log.Lshortfile)

	// set up the error level logger for the messaging package
	errorlog := logger.WriterLevel(logrus.ErrorLevel)
	defer errorlog.Close()
	messaging.Error = log.New(errorlog, "", log.Lshortfile)

	// set up the warn level logger for the messaging package
	warnlog := logger.WriterLevel(logrus.WarnLevel)
	defer warnlog.Close()
	messaging.Warn = log.New(warnlog, "", log.Lshortfile)

	// make sure the configuration object has sane defaults.
	if cfg, err = configurate.InitDefaults(*configPath, defaultConfig); err != nil {
		log.Fatal(err)
	}

	// set up the amqp connection
	amqpURI := cfg.GetString("amqp.uri")
	client, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// make sure we can publish over the configured amqp exchange
	exchangeName := cfg.GetString("amqp.exchange.name")
	client.SetupPublishing(exchangeName)

	// set up the database connection
	dbURI := cfg.GetString("db.uri")
	db, err := sql.Open("postgres", dbURI)
	if err != nil {
		logger.Fatal(errors.Wrapf(err, "error opening database"))
	}

	cb := jobStopperCallback(client)

	for {
		if err = action(db, client, cb); err != nil {
			logger.Error(err)
		}

		// could use a time.Ticker here, but this way we don't have a channel getting
		// backed up if the query takes a while or if AMQP gets backed up.
		time.Sleep(time.Second * 15)
	}
}
