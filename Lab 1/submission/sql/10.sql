-- 10
SELECT ac.category, ac.allegation_name, COUNT(*) AS num_complaints
FROM data_allegation a
JOIN data_officerallegation oa ON a.crid = oa.allegation_id
JOIN data_allegationcategory ac ON ac.id = oa.allegation_category_id
WHERE a.is_officer_complaint = FALSE
GROUP BY ac.category, ac.allegation_name
ORDER BY num_complaints DESC
LIMIT 5;