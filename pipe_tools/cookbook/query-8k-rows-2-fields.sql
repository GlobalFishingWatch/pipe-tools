SELECT
  bike_number AS mmsi,
  start_date AS timestamp
FROM
  [bigquery-public-data:san_francisco.bikeshare_trips]
WHERE
  start_date >= '2016-01-01 00:00'
  AND start_date < '2016-01-15 00:00'
