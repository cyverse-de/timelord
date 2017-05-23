package main

import (
	"database/sql"
	"flag"
	"log"

	"github.com/Sirupsen/logrus"
	"github.com/cyverse-de/configurate"
	"github.com/cyverse-de/messaging"
	"github.com/pkg/errors"
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

	// try pinging the database to make sure the connection works
	if err = db.Ping(); err != nil {
		logger.Fatal(errors.Wrapf(err, "error pinging database"))
	}
}
