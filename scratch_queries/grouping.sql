CREATE TEMP TABLE t1 (a INT);
INSERT INTO t1 VALUES (1), (2), (3);

SELECT a, max(a) FROM t1 GROUP BY CUBE(a);

SELECT * EXCEPT (query_id) FROM execution_profile();
