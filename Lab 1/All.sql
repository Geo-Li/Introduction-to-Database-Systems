-- 01
SELECT first_name, middle_initial, last_name
FROM data_officer
WHERE appointed_date >= '2020-03-15'
ORDER BY last_name, first_name;

-- 02
SELECT COUNT(*) * 100.0 / (SELECT COUNT(*) FROM data_allegation) AS percentage
FROM data_allegation
WHERE is_officer_complaint = TRUE;

-- 03
SELECT first_name, middle_initial, last_name
FROM data_officer
WHERE appointed_date = (SELECT MAX(appointed_date) FROM data_officer)
ORDER BY id;

-- 04
SELECT id, first_name, last_name
FROM data_officer
WHERE id IN (SELECT officer_id
             FROM data_officerallegation
             GROUP BY officer_id
             HAVING COUNT(DISTINCT allegation_id) >= 3)
ORDER BY id;

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

-- 08
SELECT rank, MAX(sustained_count) AS max_sustained_allegation
FROM data_officer
GROUP BY rank
ORDER BY max_sustained_allegation DESC;

-- 09
SELECT EXTRACT(YEAR FROM end_date) AS allegation_year,
       COUNT(final_finding) AS num_allegation
FROM data_officerallegation
WHERE final_finding = 'SU' OR final_finding = 'EX'
GROUP BY allegation_year
ORDER BY allegation_year;

-- 10
SELECT ac.category, ac.allegation_name, COUNT(*) AS num_complaints
FROM data_allegation a
JOIN data_officerallegation oa ON a.crid = oa.allegation_id
JOIN data_allegationcategory ac ON ac.id = oa.allegation_category_id
WHERE a.is_officer_complaint = FALSE
GROUP BY ac.category, ac.allegation_name
ORDER BY num_complaints DESC
LIMIT 5;