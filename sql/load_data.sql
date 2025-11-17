CREATE OR REPLACE TABLE labor AS
SELECT * FROM read_csv('/dev/stdin', nullstr = 'NA');

DESCRIBE labor;

EXPORT DATABASE './archives/db' (COMPRESSION zstd);
