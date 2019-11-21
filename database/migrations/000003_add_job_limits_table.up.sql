CREATE TABLE IF NOT EXISTS job_limits (
       id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
       launcher STRING UNIQUE DEFAULT NULL,
       concurrent_jobs INT NOT NULL
);

-- The default job limits.
UPSERT INTO job_limits (concurrent_jobs) VALUES (8);
