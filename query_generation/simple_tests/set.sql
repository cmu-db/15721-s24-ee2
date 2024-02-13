CREATE TABLE cmu_students (cmu_id INT, name VARCHAR);
CREATE TABLE pitt_students (cmu_id INT, name VARCHAR);
CREATE TABLE penn_students (cmu_id INT, name VARCHAR);
INSERT INTO cmu_students VALUES (3, 'Andy'), (5, 'Pavlo');
INSERT INTO pitt_students VALUES (7, 'Bla'), (9, 'Cla');
INSERT INTO penn_students VALUES (11, 'Dla'), (13, 'Ela');

--PARSE
SELECT * FROM cmu_students UNION ALL SELECT * FROM pitt_students UNION ALL SELECT * FROM penn_students;
