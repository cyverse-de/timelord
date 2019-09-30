package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	_ "expvar"

	"github.com/cyverse-de/configurate"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/lib/pq"

	"gopkg.in/cyverse-de/messaging.v4"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const defaultConfig = `db:
  uri: "db:5432"
notifications:
  base: http://notification-agent
iplant_groups:
  base: http://iplant-groups
  user: grouper-user
`

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
func SendKillNotification(j *Job, k8s *K8sClient, killNotifKey string) error {
	var (
		err        error
		deployment *appsv1.Deployment
		wasSent    bool
	)

	log.Warnf("getting deployment for job ID %s", j.ExternalID)

	deployment, err = k8s.getDeployment(j.ExternalID)
	if err != nil {
		err = errors.Wrapf(err, "error getting deployment for job ID %s", j.ExternalID)
		log.Error(err)
		return err
	}

	// check for the annotation saying the warning has been sent
	wasSent = k8s.deploymentHasAnnotation(deployment, killNotifKey)
	if err != nil {
		err = errors.Wrapf(err, "error getting annotation key %s for deployment %s", killNotifKey, j.ExternalID)
		log.Error(err)
		return err
	}

	log.Warnf("external ID %s has been told of job termination: %v", j.ExternalID, wasSent)

	if !wasSent {
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
	}
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

func sendWarning(db *sql.DB, k8s *K8sClient, warningInterval int64, warningKey string) {
	jobs, err := JobKillWarnings(db, warningInterval)
	if err != nil {
		log.Error(err)
	} else {
		for _, j := range jobs {
			// get the deployment
			var deployment *appsv1.Deployment

			log.Warnf("getting deployment for job ID %s", j.ExternalID)

			deployment, err := k8s.getDeployment(j.ExternalID)
			if err != nil {
				log.Error(errors.Wrapf(err, "error getting deployment for job ID %s", j.ExternalID))
				continue
			}

			// check for the annotation saying the warning has been sent
			wasSent := k8s.deploymentHasAnnotation(deployment, warningKey)
			if err != nil {
				log.Error(errors.Wrapf(err, "error getting annotation key %s for deployment %s", warningKey, j.ExternalID))
				continue
			}

			log.Warnf("external ID %s has been warned of possible termination: %v", j.ExternalID, wasSent)

			if !wasSent {
				err = SendWarningNotification(&j)
				if err != nil {
					log.Error(errors.Wrapf(err, "error sending warnining notification for analysis %s", j.ExternalID))
					continue
				}

				// Set the annotation saying that the 1 hour warning was sent
				k8s.annotateDeployment(deployment, warningKey, "true")
			}
		}
	}
}

func main() {
	log.SetReportCaller(true)

	var (
		err        error
		kubeconfig *string
		cfg        *viper.Viper

		notifPath       = "/notification"
		configPath      = flag.String("config", "/etc/iplant/de/jobservices.yml", "The path to the YAML config file.")
		expvarPort      = flag.String("port", "60000", "The path to listen for expvar requests on.")
		appsBase        = flag.String("apps", "http://apps", "The base URL for the apps service.")
		namespace       = flag.String("namespace", "vice-apps", "The namespace that VICE analyses run in.")
		appExposerBase  = flag.String("app-exposer", "http://app-exposer", "The base URL for the app-exposer service.")
		killNotifKey    = flag.String("kill-notif-key", "killnotifsent", "The key for the annotation detailing whether the notification about job termination was sent.")
		warningInterval = flag.Int64("warning-interval", 60, "The number of minutes in advance to warn users about job kills.")
		warningSentKey  = flag.String("warning-sent-key", "warningsent", "The key for the annotation detailing whether the job termination warning was sent.")
	)

	// if cluster is set, then
	if cluster := os.Getenv("CLUSTER"); cluster != "" {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	} else {
		// If the home directory exists, then assume that the kube config will be read
		// from ~/.kube/config.
		if home := os.Getenv("HOME"); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			// If the home directory doesn't exist, then allow the user to specify a path.
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
	}

	// Print error and exit if *kubeconfig is not empty and doesn't actually
	// exist. If *kubeconfig is blank, then the app may be running inside the
	// cluster, so let things proceed.
	if *kubeconfig != "" {
		_, err = os.Stat(*kubeconfig)
		if err != nil {
			if os.IsNotExist(err) {
				log.Fatalf("config %s does not exist", *kubeconfig)
			}
			log.Fatal(errors.Wrapf(err, "error stat'ing the kubeconfig %s", *kubeconfig))
		}
	}

	var config *rest.Config
	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error building config from flags using kubeconfig %s", *kubeconfig))
		}
	} else {
		// If the home directory doesn't exist and the user doesn't specify a path,
		// then assume that we're running inside a cluster.
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error loading the config inside the cluster"))
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "error creating clientset from config"))
	}

	k8s := &K8sClient{
		clientset: clientset,
		namespace: *namespace,
	}

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
		AppsBase:       *appsBase,
		AppExposerBase: *appExposerBase,
	}

	go func() {
		var jl []Job

		for {
			// 1 hour warning
			sendWarning(db, k8s, *warningInterval, *warningSentKey)

			// 1 day warning
			sendWarning(db, k8s, 1440, "onedaywarning")

			jl, err = JobsToKill(db)
			if err != nil {
				log.Error(errors.Wrap(err, "error getting list of jobs to kill"))
				continue
			}

			for _, j := range jl {
				if err = jobKiller.KillJob(db, &j); err != nil {
					log.Error(errors.Wrapf(err, "error terminating analysis '%s'", j.ID))
					continue // skip to kill next job
				}

				if err = SendKillNotification(&j, k8s, *killNotifKey); err != nil {
					log.Error(errors.Wrapf(err, "error sending notification that %s has been terminated", j.ID))
					continue // technically not necessary, but added in case another statement is added to the loop
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
