CREATE OR REPLACE VIEW trips_monthly_metrics AS
SELECT
    date_trunc('month', started_at) AS month,
    member_casual,
    rideable_type,
    COUNT(*) AS total_trips,
    AVG(
      date_diff('second', started_at, ended_at) / 60.0
    ) AS avg_duration_min
FROM trips
GROUP BY 1, 2, 3;
