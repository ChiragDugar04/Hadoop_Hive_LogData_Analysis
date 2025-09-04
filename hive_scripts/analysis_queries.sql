-- Top 10 most requested URLs
SELECT request, COUNT(*) AS total_requests
FROM processed_logs
GROUP BY request
ORDER BY total_requests DESC
LIMIT 10;

-- Requests per day
SELECT log_date, COUNT(*) AS daily_requests
FROM processed_logs
GROUP BY log_date
ORDER BY log_date;

-- Unique users (hosts) per month
SELECT substr(log_date, 4, 3) AS month, COUNT(DISTINCT host) AS unique_users
FROM processed_logs
GROUP BY substr(log_date, 4, 3)
ORDER BY month;

-- Count of 404 (Not Found) errors
SELECT COUNT(*) AS total_404s
FROM processed_logs
WHERE status = 404;

-- Top 10 hosts that made the most requests
SELECT host, COUNT(*) AS total_requests
FROM processed_logs
GROUP BY host
ORDER BY total_requests DESC
LIMIT 10;

-- Average size of responses by status code
SELECT status, AVG(size) AS avg_response_size
FROM processed_logs
GROUP BY status
ORDER BY status;
