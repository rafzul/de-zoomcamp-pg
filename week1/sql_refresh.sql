-- select first 100 row in table
SELECT
    *
FROM
    trips
LIMIT 100;

-- select all rows in trip table joined w their zones information
SELECT
    *
FROM
    trips t,
    zones zpu,
    zones zdo
WHERE
    t."PULocationID" = zpu."LocationID" AND
    t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- only displaying pickup and dropoff time, amount, and additionally information of borough/zone of pickup and dropoff location through implicit inner joins
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
    trips t,
    zones zpu,
    zones zdo
WHERE
    t."PULocationID" = zpu."LocationID" AND
    t."DOLocationID" = zdo."LocationID"
LIMIT 100;

-- sama tapi pake JOIN yg eksplisit (inner join)
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
    CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
    trips t JOIN zones zpu
        ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo
        ON t."DOLocationID" = zdo."LocationID"
LIMIT 100;

--cari kolom yg PU location ID nya null
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	trips t 
WHERE
	"PULocationID" is NULL
LIMIT 100;
	
-- cari kolom yg DOLocationID nya ngga ada di LocationID
SELECT
	tpep_pickup_datetime,
	tpep_dropoff_datetime,
	total_amount,
	"PULocationID",
	"DOLocationID"
FROM
	trips t 
WHERE
	"DOLocationID" NOT IN(
        SELECT "LocationID" FROM zones
    )
LIMIT 100;

-- Delete rows di Zone yang locationID nya 142
DELETE FROM zones WHERE "LocationID" = 142;
--
