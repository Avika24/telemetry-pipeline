-- 1. View all rejected events
SELECT *
FROM rejected_events;


-- 2. Count rejection reasons
SELECT failure_reason, COUNT(*) AS total
FROM rejected_events
GROUP BY failure_reason
ORDER BY total DESC;


-- 3. Count clean and rejected events
SELECT
    (SELECT COUNT(*) FROM clean_events) AS clean,
    (SELECT COUNT(*) FROM rejected_events) AS rejected;