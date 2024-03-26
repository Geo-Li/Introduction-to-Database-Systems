-- 08
SELECT rank, MAX(sustained_count) AS max_sustained_allegation
FROM data_officer
GROUP BY rank
ORDER BY max_sustained_allegation DESC;