-- Enable dynamic partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Create internal processed_logs table
CREATE TABLE IF NOT EXISTS processed_logs (
    host STRING,
    identity STRING,
    `user` STRING,
    request STRING,
    status INT,
    size INT
)
PARTITIONED BY (log_date STRING)
CLUSTERED BY (host) INTO 8 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
