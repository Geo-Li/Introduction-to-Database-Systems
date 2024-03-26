-- 01
SELECT first_name, middle_initial, last_name
FROM data_officer
WHERE appointed_date >= '2020-03-15'
ORDER BY last_name, first_name;