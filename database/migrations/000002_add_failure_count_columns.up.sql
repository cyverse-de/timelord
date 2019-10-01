ALTER TABLE IF EXISTS notif_statuses
    ADD COLUMN IF NOT EXISTS hour_warning_failure_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS day_warning_failure_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS kill_warning_failure_count INT NOT NULL DEFAULT 0;