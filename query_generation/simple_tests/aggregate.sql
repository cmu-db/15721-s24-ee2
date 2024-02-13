CREATE TABLE cmu_students (cmu_id INT, name VARCHAR);
INSERT INTO cmu_students VALUES (3, 'Andy'), (7, 'Pavlo');
--PARSE
SELECT COUNT(*) FROM cmu_students GROUP BY name;
