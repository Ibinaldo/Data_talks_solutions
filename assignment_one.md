<h1><center> DE Zoom camp 2023 - Homework 1 </center></h1>

## Question 1. Knowing docker tags

Run the command to get information on Docker

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file*

** Answer **: The command ```docker build -help``` was ran and from the displayed options, the answer is **--iidfile string**

## Question 2. Understanding docker first run

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list).
How many python packages/modules are installed?

**Answer**: The command ```docker run -it --entrypoint bash python:3/9``` was ran. Then the command ```pip list``` was run. The answer is **3** packages (pip, setuptools & wheel)

## Question 3. Count records

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.


**Answer**:
``` sql
SELECT
  COUNT(1)
FROM
  green_trip_data_2019
WHERE
  DATE(lpep_pickup_datetime) = '2019-01-15' AND
  DATE(lpep_dropoff_datetime) = '2019-01-15';
```
The answer from this query was **20530**

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

**Answer**:

```sql
SELECT
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  trip_distance
FROM
  green_trip_data_2019
ORDER BY
  3 DESC
LIMIT 1;
```
The answer from this query was **2019-01-15**

## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?

**Answer**:

```sql
SELECT
  passenger_count,
  COUNT(1) AS "counts"
FROM
  green_trip_data_2019
WHERE
  DATE(lpep_pickup_datetime) = '2019-01-01'
GROUP BY
  1;
  ```

The answer from this query was **2: 1282 ; 3: 254**

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

**Answer**:

```sql
SELECT
  a."DOLocationID",
  a."PULocationID",
  a.tip_amount,
  b."Zone"
FROM
  green_trip_data_2019 a
LEFT JOIN zones b
ON
  a."DOLocationID" = b."LocationID"
WHERE
  a."PULocationID" = 7  
ORDER BY
  3 DESC
LIMIT
  1;
```

The answer to this question is **Long island City/Queens Plaza**
