-- 06
SELECT DISTINCT a1.officer_id, a2.officer_id, o1.last_unit_id, o2.last_unit_id
FROM data_officerallegation a1
JOIN data_officerallegation a2
    ON a1.allegation_id = a2.allegation_id
JOIN data_officer o1
    ON a1.officer_id = o1.id
JOIN data_officer o2
    ON a2.officer_id = o2.id
WHERE a1.officer_id < a2.officer_id
    AND o1.last_unit_id <> o2.last_unit_id
ORDER BY a1.officer_id, a2.officer_id;