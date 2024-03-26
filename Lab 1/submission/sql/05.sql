-- 05
WITH districts AS
(SELECT a.id,
        NULLIF(REGEXP_REPLACE(name, '\D', '', 'g'), '')::numeric AS district,
        SUM(r.count) AS tot_pop
FROM data_area a
JOIN data_racepopulation r ON r.area_id = a.id
WHERE area_type = 'police-districts'
GROUP BY a.id, district)

SELECT p.unit_name, (COUNT(o.id) * 10000.0 / SUM(d.tot_pop)) AS officers_per_capita
FROM data_policeunit p
JOIN districts d ON d.district = CAST(p.unit_name AS INT)
JOIN data_officer o ON o.last_unit_id = d.district
WHERE o.active ='Yes'
GROUP BY d.district, p.unit_name
ORDER BY officers_per_capita DESC;