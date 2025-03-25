CREATE TEMP TABLE t1 (i INT);
INSERT INTO t1 VALUES (1), (2), (3), (NULL);

WITH cte AS MATERIALIZED (
  SELECT * FROM t1 a1, t1 a2 WHERE a1.i = a2.i
)
SELECT * FROM cte;

-- SELECT * EXCEPT (query_id) FROM execution_profile();

