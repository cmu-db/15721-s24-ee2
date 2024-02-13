CREATE TABLE cmu_students (cmu_id INT, name VARCHAR);
INSERT INTO cmu_students VALUES (3, 'Andy'), (7, 'Pavlo');
--PARSE
SELECT * FROM cmu_students ORDER BY cmu_id ASC, name DESC;
