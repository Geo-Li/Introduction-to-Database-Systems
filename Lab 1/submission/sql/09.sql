-- 09
SELECT EXTRACT(YEAR FROM end_date) AS allegation_year,
       COUNT(final_finding) AS num_allegation
FROM data_officerallegation
WHERE final_finding = 'SU' OR final_finding = 'EX'
GROUP BY allegation_year
ORDER BY allegation_year;