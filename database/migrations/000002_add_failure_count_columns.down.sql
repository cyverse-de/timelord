ALTER TABLE IF EXISTS notif_statuses
    DROP COLUMN IF EXISTS hour_warning_failure_count,
    DROP COLUMN IF EXISTS day_warning_failure_count,
    DROP COLUMN IF EXISTS kill_warning_failure_count;