CREATE TABLE cmu_students (cmu_id INT, name VARCHAR);
CREATE TABLE cmu_enrolled (cmu_id INT, course_id INT);
INSERT INTO cmu_students VALUES (3, 'Andy'), (5, 'Pavlo');
INSERT INTO cmu_enrolled VALUES (3, 15721), (4, 15555);
--PARSE
SELECT * FROM cmu_students CROSS JOIN cmu_enrolled;
