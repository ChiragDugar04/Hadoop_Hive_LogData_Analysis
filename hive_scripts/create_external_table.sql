-- Create external table for raw logs
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs (
    host STRING,
    identity STRING,
    `user` STRING,
    time STRING,
    request STRING,
    status INT,
    size STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^(\\S+) (\\S+) (\\S+) \\[(.*?)\\] \"(.*?)\" (\\d{3}) (\\S+)"
)
LOCATION '/user/chirag/raw_logs';