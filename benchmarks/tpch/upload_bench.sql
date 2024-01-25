CREATE TABLE IF NOT EXISTS public.benchmarks (
    testing_db TEXT,
    testing_db_version TEXT,
    query_no INT,
    duration DOUBLE,
    github_ref TEXT,
    github_run TEXT,
    scale_factor INT,
    system_meta TEXT,
    timestamp TIMESTAMP,
);

-- Manipulate the data and save it in a temporary table.
CREATE TEMP TABLE current_run AS
WITH timings (testing_db, testing_db_version, query_no, duration) AS (
    SELECT
        solution,
        version,
        query_no,
        AVG("duration[s]")
    FROM 'timings.csv'
    WHERE success IS TRUE
    GROUP BY solution, version, query_no
)
SELECT * FROM timings
CROSS JOIN 'meta.csv'
CROSS JOIN (SELECT NOW() timestamp);

-- Just for logging.
SELECT * FROM current_run
    ORDER BY
        testing_db,
        testing_db_version,
        query_no;

-- Actually insert the data.
INSERT INTO public.benchmarks
    SELECT * FROM current_run
        ORDER BY
            testing_db,
            testing_db_version,
            query_no;
