package main

import (
	"database/sql"

	log "github.com/sirupsen/logrus"
)

const getNotifStatusIDQuery = `
select id
  from notif_statuses
 where analysis_id = $1
`

const addNotifRecordQuery = `
insert into notif_statuses (analysis_id, external_id) values ($1, $2) returning id
`

// VICEDatabaser interacts with the VICE database.
type VICEDatabaser struct {
	db *sql.DB
}

// AnalysisRecordExists checks whether the given analysisID is already in the database or not.
func (v *VICEDatabaser) AnalysisRecordExists(analysisID string) bool {
	var (
		err     error
		row     *sql.Row
		notifID string
	)

	row = v.db.QueryRow(
		getNotifStatusIDQuery,
		analysisID,
	)
	if err = row.Scan(&notifID); err != nil {
		log.Error(err)
		return false
	}
	return true
}

// AddNotifRecord adds a new record to the notif_statuses table for the provided analysis.
// Returns the ID of the new record.
func (v *VICEDatabaser) AddNotifRecord(job *Job) (string, error) {
	var (
		err     error
		notifID string
	)

	if err = v.db.QueryRow(
		addNotifRecordQuery,
		job.ID,
		job.ExternalID,
	).Scan(&notifID); err != nil {
		return "", err
	}
	return notifID, err
}

const getHourWarningQuery = `
select hour_warning_sent
  from notif_statuses
 where analysis_id = $1
`

// HourWarningSent returns true if the 1-hour warning was already sent for the analysis.
func (v *VICEDatabaser) HourWarningSent(job *Job) (bool, error) {
	var (
		err     error
		wasSent bool
	)

	if err = v.db.QueryRow(
		getHourWarningQuery,
		job.ID,
	).Scan(&wasSent); err != nil {
		return false, err
	}
	return wasSent, nil
}

const getDayWarningQuery = `
select day_warning_sent
  from notif_statuses
 where analysis_id = $1
`

// DayWarningSent returns true if the 1-day warning was already sent for the analysis.
func (v *VICEDatabaser) DayWarningSent(job *Job) (bool, error) {
	var (
		err     error
		wasSent bool
	)

	if err = v.db.QueryRow(
		getDayWarningQuery,
		job.ID,
	).Scan(&wasSent); err != nil {
		return false, err
	}

	return wasSent, nil
}

const getKillWarningQuery = `
select kill_warning_sent
  from notif_statuses
 where analysis_id = $1
`

// KillWarningSent returns true if the job termination warning was already sent for the analyis.
func (v *VICEDatabaser) KillWarningSent(job *Job) (bool, error) {
	var (
		err     error
		wasSent bool
	)

	if err = v.db.QueryRow(
		getKillWarningQuery,
		job.ID,
	).Scan(&wasSent); err != nil {
		return false, err
	}

	return wasSent, nil
}

const setDayWarningSentQuery = `
update notif_statuses set day_warning_sent = $1 where analysis_id = $2
`

// SetDayWarningSent sets the day_warning_sent field to the value of wasSent in the
// record for the analysis represented by job.
func (v *VICEDatabaser) SetDayWarningSent(job *Job, wasSent bool) error {
	var err error

	_, err = v.db.Exec(
		setDayWarningSentQuery,
		wasSent,
		job.ID,
	)
	return err
}

const setHourWarningSentQuery = `
update notif_statuses set hour_warning_sent = $1 where analysis_id = $2
`

// SetHourWarningSent sets the hour_warning_sent field to the value of wasSent in the
// record for the analysis represented by job.
func (v *VICEDatabaser) SetHourWarningSent(job *Job, wasSent bool) error {
	var err error

	_, err = v.db.Exec(
		setHourWarningSentQuery,
		wasSent,
		job.ID,
	)
	return err
}

const setKillWarningSentQuery = `
update notif_statuses set kill_warning_sent = $1 where analysis_id = $2
`

// SetKillWarningSent sets the kill_warning_sent field to the value of wasSent in the
// record for the analysis represented by job.
func (v *VICEDatabaser) SetKillWarningSent(job *Job, wasSent bool) error {
	var err error

	_, err = v.db.Exec(
		setKillWarningSentQuery,
		wasSent,
		job.ID,
	)
	return err
}
