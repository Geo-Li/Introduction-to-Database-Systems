-- 03
SELECT first_name, middle_initial, last_name
FROM data_officer
WHERE appointed_date = (SELECT MAX(appointed_date) FROM data_officer)
ORDER BY id;