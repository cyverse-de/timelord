package main

import (
	"context"
	"database/sql"

	log "github.com/sirupsen/logrus"
)

// VICEDatabaser interacts with the VICE database.
type VICEDatabaser struct {
	db *sql.DB
}

// NotifStatuses contains the info about what statuses were sent for each analysis.
type NotifStatuses struct {
	AnalysisID              string
	ExternalID              string
	HourWarningSent         bool
	HourWarningFailureCount int
	DayWarningSent          bool
	DayWarningFailureCount  int
	KillWarningSent         bool
	KillWarningFailureCount int
}

const notifStatusQuery = `
	select analysis_id,
		   external_id,
		   hour_warning_sent,
		   hour_warning_failure_count,
		   day_warning_sent,
		   day_warning_failure_count,
		   kill_warning_sent,
		   kill_warning_failure_count
	  from notif_statuses
	 where analysis_id = $1
`

// NotifStatuses returns a filled out *NotifStatuses or nothing at all.
func (v *VICEDatabaser) NotifStatuses(ctx context.Context, job *Job) (*NotifStatuses, error) {
	var (
		err           error
		notifStatuses *NotifStatuses
	)

	notifStatuses = &NotifStatuses{}

	if err = v.db.QueryRowContext(
		ctx,
		notifStatusQuery,
		job.ID,
	).Scan(
		&notifStatuses.AnalysisID,
		&notifStatuses.ExternalID,
		&notifStatuses.HourWarningSent,
		&notifStatuses.HourWarningFailureCount,
		&notifStatuses.DayWarningSent,
		&notifStatuses.DayWarningFailureCount,
		&notifStatuses.KillWarningSent,
		&notifStatuses.KillWarningFailureCount,
	); err != nil {
		return nil, err
	}

	return notifStatuses, nil
}

const getNotifStatusIDQuery = `
select id
  from notif_statuses
 where analysis_id = $1
`

// AnalysisRecordExists checks whether the given analysisID is already in the database or not.
func (v *VICEDatabaser) AnalysisRecordExists(ctx context.Context, analysisID string) bool {
	var (
		err     error
		row     *sql.Row
		notifID string
	)

	row = v.db.QueryRowContext(
		ctx,
		getNotifStatusIDQuery,
		analysisID,
	)
	if err = row.Scan(&notifID); err != nil {
		log.Error(err)
		return false
	}
	return true
}

const addNotifRecordQuery = `
insert into notif_statuses (analysis_id, external_id) values ($1, $2) returning id
`

// AddNotifRecord adds a new record to the notif_statuses table for the provided analysis.
// Returns the ID of the new record.
func (v *VICEDatabaser) AddNotifRecord(ctx context.Context, job *Job) (string, error) {
	var (
		err     error
		notifID string
	)

	if err = v.db.QueryRowContext(
		ctx,
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
func (v *VICEDatabaser) HourWarningSent(ctx context.Context, job *Job) (bool, error) {
	var (
		err     error
		wasSent bool
	)

	if err = v.db.QueryRowContext(
		ctx,
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
func (v *VICEDatabaser) DayWarningSent(ctx context.Context, job *Job) (bool, error) {
	var (
		err     error
		wasSent bool
	)

	if err = v.db.QueryRowContext(
		ctx,
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
func (v *VICEDatabaser) KillWarningSent(ctx context.Context, job *Job) (bool, error) {
	var (
		err     error
		wasSent bool
	)

	if err = v.db.QueryRowContext(
		ctx,
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
func (v *VICEDatabaser) SetDayWarningSent(ctx context.Context, job *Job, wasSent bool) error {
	var err error

	_, err = v.db.ExecContext(
		ctx,
		setDayWarningSentQuery,
		wasSent,
		job.ID,
	)
	return err
}

const setDayWarningFailureCountQuery = `
update notif_statuses set day_warning_failure_count = $1 where analysis_id = $2
`

// SetDayWarningFailureCount sets the new value for the kill_warning_failure_count field.
func (v *VICEDatabaser) SetDayWarningFailureCount(ctx context.Context, job *Job, failureCount int) error {
	var err error

	_, err = v.db.ExecContext(
		ctx,
		setDayWarningFailureCountQuery,
		failureCount,
		job.ID,
	)
	return err
}

const setHourWarningSentQuery = `
update notif_statuses set hour_warning_sent = $1 where analysis_id = $2
`

// SetHourWarningSent sets the hour_warning_sent field to the value of wasSent in the
// record for the analysis represented by job.
func (v *VICEDatabaser) SetHourWarningSent(ctx context.Context, job *Job, wasSent bool) error {
	var err error

	_, err = v.db.ExecContext(
		ctx,
		setHourWarningSentQuery,
		wasSent,
		job.ID,
	)
	return err
}

const setHourWarningFailureCountQuery = `
update notif_statuses set hour_warning_failure_count = $1 where analysis_id = $2
`

// SetHourWarningFailureCount sets the new value for the kill_warning_failure_count field.
func (v *VICEDatabaser) SetHourWarningFailureCount(ctx context.Context, job *Job, failureCount int) error {
	var err error

	_, err = v.db.ExecContext(
		ctx,
		setHourWarningFailureCountQuery,
		failureCount,
		job.ID,
	)
	return err
}

const setKillWarningSentQuery = `
update notif_statuses set kill_warning_sent = $1 where analysis_id = $2
`

// SetKillWarningSent sets the kill_warning_sent field to the value of wasSent in the
// record for the analysis represented by job.
func (v *VICEDatabaser) SetKillWarningSent(ctx context.Context, job *Job, wasSent bool) error {
	var err error

	_, err = v.db.ExecContext(
		ctx,
		setKillWarningSentQuery,
		wasSent,
		job.ID,
	)
	return err
}

const setKillWarningFailureCountQuery = `
update notif_statuses set kill_warning_failure_count = $1 where analysis_id = $2
`

// SetKillWarningFailureCount sets the new value for the kill_warning_failure_count field.
func (v *VICEDatabaser) SetKillWarningFailureCount(ctx context.Context, job *Job, failureCount int) error {
	var err error

	_, err = v.db.ExecContext(
		ctx,
		setKillWarningFailureCountQuery,
		failureCount,
		job.ID,
	)
	return err
}
