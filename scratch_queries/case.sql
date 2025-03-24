CREATE TEMP TABLE vals (a INT);
INSERT INTO vals VALUES (4), (5), (6), (NULL);


SELECT a, CASE WHEN a < 3          THEN a / 0
               WHEN a > 3 OR a < 7 THEN a
               WHEN a IS NULL      THEN 22
               WHEN a > 7          THEN 1+2 / 0
               WHEN 1/0 = 4        THEN 44
          END
  FROM vals;
