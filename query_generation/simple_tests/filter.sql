CREATE TABLE cmu_students (cmu_id INT, name VARCHAR);
INSERT INTO cmu_students VALUES (3, 'Andy'), (7, 'Pavlo');
--PARSE
SELECT * FROM (SELECT SUM(cmu_id) AS id_sum FROM cmu_students GROUP BY name) WHERE id_sum > 7;
