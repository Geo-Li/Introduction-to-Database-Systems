-- 07
WITH trr_count AS
(SELECT o.id, COUNT(DISTINCT t.id) AS trr_count
 FROM data_officer o
 LEFT JOIN trr_trr t ON o.id = t.officer_id
 GROUP BY o.id)
SELECT MAX(trr_count) AS max_trr,
       MIN(trr_count) AS min_trr,
       SUM(trr_count) * 1.0 / COUNT(trr_count) AS avg_trr
FROM trr_count;