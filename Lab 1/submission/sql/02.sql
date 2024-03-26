-- 02
SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM data_allegation) AS percentage
FROM data_allegation
WHERE is_officer_complaint = TRUE;