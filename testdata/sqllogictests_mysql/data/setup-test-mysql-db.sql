-- Setup a simple table with all the supported mysql datatypes.
CREATE TABLE IF NOT EXISTS glaredb_test.numeric_datatypes (
    c1  BOOL,
    c2  BOOLEAN,
    c3  TINYINT,
    c4  TINYINT UNSIGNED,
    c5  SMALLINT,
    c6  SMALLINT UNSIGNED,
    c7  MEDIUMINT,
    c8  MEDIUMINT UNSIGNED,
    c9  INT,
    c10 INT UNSIGNED,
    c11 BIGINT,
    c12 BIGINT UNSIGNED,
    c13 FLOAT,
    c14 DOUBLE,
    c15 DECIMAL(5,2)
);

INSERT INTO glaredb_test.numeric_datatypes
VALUES (
    true,
    false,
    -128,
    255,
    -32768,
    65535,
    -8388608,
    16777215,
    -2147483648,
    4294967295,
    -300000000,
    5000000000,
    4.5,
    6.7,
    123.45
),
(
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE IF NOT EXISTS glaredb_test.string_datatypes (
    c1 CHAR(100),
    c2 VARCHAR(100),
    c3 TEXT,
    c4 JSON,
    c5 BLOB
);

INSERT INTO glaredb_test.string_datatypes
VALUES (
    'a',
    'bc',
    'def',
    '{"a": [1, 2]}',
    'bin'
),
(
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE IF NOT EXISTS glaredb_test.date_time_datatypes (
    c1 DATE,
    c2 DATETIME(6),
    c3 TIME,
    c4 YEAR,
    c5 TIMESTAMP(3)
);

INSERT INTO glaredb_test.date_time_datatypes
VALUES (
    '2023-01-01',
    '2023-01-02 12:12:12.123456',
    '14:14:14',
    '2023',
    '2023-12-31 13:13:13.789'
),
(
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
);

CREATE TABLE IF NOT EXISTS glaredb_test.column_attributes (
    C1 INT NULL,
    C2 INT NOT NULL,
    C3 VARCHAR(100) DEFAULT "default"
);

INSERT INTO glaredb_test.column_attributes
VALUES (
    NULL,
    1,
    DEFAULT
);
