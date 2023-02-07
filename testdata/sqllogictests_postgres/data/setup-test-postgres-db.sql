-- Setup a simple table with all the supported postgres datatypes.
CREATE TABLE IF NOT EXISTS datatypes (
    c1  BOOL,
    c2  INT2,
    c3  INT4,
    c4  INT8,
    c5  FLOAT4,
    c6  FLOAT8,
    c7  CHAR,
    c8  BPCHAR,
    c9  VARCHAR,
    c10 TEXT,
    c11 JSON,
    c12 JSONB,
    c13 UUID,
    c14 BYTEA,
    c15 TIMESTAMP,
    c16 TIME,
    c17 DATE,
    c18 TIMESTAMPTZ
);

INSERT INTO datatypes
VALUES (
    true,
    1,
    2,
    3,
    4.5,
    6.7,
    'a',
    'b',
    'cde',
    'fghi',
    '{"a": [1, 2]}',
    '[{"b": null}, {"c": true}]',
    '292a485f-a56a-4938-8f1a-bbbbbbbbbbb1',
    'bin',
    '1999-09-30 16:32:34',
    '04:32:34 PM',
    '1999-09-30',
    '2004-10-19 16:32:34 IST'
);

INSERT INTO datatypes(c1) VALUES (NULL); -- inserts nulls
