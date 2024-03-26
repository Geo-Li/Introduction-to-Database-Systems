-- 04
SELECT id, first_name, last_name
FROM data_officer
WHERE id IN (SELECT officer_id
             FROM data_officerallegation
             GROUP BY officer_id
             HAVING COUNT(DISTINCT allegation_id) >= 3)
ORDER BY id;