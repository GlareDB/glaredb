SELECT 1;
-- NOTE: Until the change gets merged (where we support file as an argument)
-- and that gets into release, the only way we can run an SQL file is by
-- `cat`ting it and passing it as an argument. But due to this, we can't
-- really pass a comment as first line in this script otherwise clap thinks
-- of the argument as a flag (why not! SQL has comments starting with --).
-- Until then, let there be this "SELECT 1" on top.
--
-- See: https://github.com/GlareDB/glaredb/pull/1913

-- Create the table to store benchmarks.
--
-- TODO: Need to wait for `IF NOT EXISTS` to be fixed in the next release.
-- See: https://github.com/GlareDB/glaredb/pull/1911
--
-- CREATE TABLE IF NOT EXISTS public.benchmarks (
--     testing_db TEXT,
--     testing_db_version TEXT,
--     query_no INT,
--     duration DOUBLE,
--     github_ref TEXT,
--     github_run TEXT,
--     scale_factor INT,
--     system_meta TEXT,
--     timestamp TIMESTAMP,
-- );

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
