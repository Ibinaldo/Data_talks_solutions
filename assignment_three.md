## Question 1. What is the count for fhv vehicle records for year 2019?
First we create both the external and BQ tables:

```
-- External table
CREATE OR REPLACE EXTERNAL TABLE `dtc-terraform-de.de_zoomcamp_prefect.fhv_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-fhv-data/fhv_tripdata_2019-*.gz']
);
```

```
-- BQ table
CREATE OR REPLACE TABLE `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_non_partitioned` AS
(SELECT * FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data` )
```
We then query the table for count of vehicl records
-- SELECT COUNT(*) FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_non_partitioned`
**Answer**: 43,244,696

## Question 2: Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

**Answer**:
```
-- Number of base numbers for external table
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data`
```
0 B for external table

```
-- Number of base numbers for BQ table
-- SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_non_partitioned`
```
317.94 MB for bq table

## Question 3: How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```
SELECT COUNT(*) FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_non_partitioned`
WHERE PUlocationID IS NULL AND DOlocationID IS NULL
```
**Answer**: 717,748

## Question 4: What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
**Answer**: Partition by pickup_datetime Cluster on affiliated_base_number

## Question 5: Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive). Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches

First we create the clustered table
```
CREATE OR REPLACE TABLE `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_partitioned_clustered`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data`
```

Next get distinct affiliated based_number for non partitioned
```
 SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_non_partitioned`
WHERE DATE(pickup_datetime) >= "2019-03-01" AND DATE(pickup_datetime) <= "2019-03-31"
```
This processes 647.87 MB

Now look at clustered table
```
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `dtc-terraform-de.de_zoomcamp_prefect.fhv_data_partitioned_clustered`
WHERE DATE(pickup_datetime) >= "2019-03-01" AND DATE(pickup_datetime) <= "2019-03-31"
```
This processes 23.05 MB

**Answer**: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

## Question 6: Where is the data stored in the External Table you created?
**Answer**: While the external table stores the metadata and schema in BigQuery storage, the data itself resides in the external source, which in our case is a GCP bucket


## Question 7: It is best practice in Big Query to always cluster your data:
**Answer**: The answer is **false**. If your table is smaller than 1 GB, clustering doesn't offer significant performance gains.
