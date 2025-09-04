-- Insert cleaned + partitioned data into processed_logs
INSERT OVERWRITE TABLE processed_logs PARTITION (log_date)
SELECT
    host,
    identity,
    `user`,
    request,
    status,
    CASE size WHEN '-' THEN 0 ELSE CAST(size AS INT) END AS size,
    substr(time, 1, 11) AS log_date  -- Extract first 11 chars, e.g., "01/Jul/1995"
FROM raw_logs;
