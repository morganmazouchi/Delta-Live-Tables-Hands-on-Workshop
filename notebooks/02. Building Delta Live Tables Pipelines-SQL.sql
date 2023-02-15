-- Databricks notebook source
-- MAGIC %md
-- MAGIC When creating the pipeline add the following configuration to point to the data source:
-- MAGIC 
-- MAGIC `data_source_path` : `/databricks-datasets/online_retail/data-001/`

-- COMMAND ----------

-- DBTITLE 1,Read in Raw Data via Autoloader in SQL
CREATE STREAMING LIVE TABLE raw_retail
COMMENT "This is the RAW bronze input data read in with Autoloader - no optimizations or expectations"
PARTITIONED BY (Country)
TBLPROPERTIES ("quality" = "bronze")
AS (
      SELECT
      *,
      input_file_name() AS inputFileName
      FROM cloud_files( '${data_source_path}', 'csv', 
            map("schema", "InvoiceNo STRING, StockCode STRING, Description STRING, Quantity FLOAT, InvoiceDate STRING, UnitPrice FLOAT, CustomerID STRING, Country STRING",  
            "header", "true"))
  )


-- COMMAND ----------

-- DBTITLE 1,Optimize Data Layout for Performance
-- SET pipelines.trigger.interval='1 hour';

CREATE STREAMING LIVE TABLE cleaned_retail
PARTITIONED BY (Country)
COMMENT "This is the raw bronze table with data cleaned (dates, etc.), data partitioned, and optimized"
TBLPROPERTIES --Can be spark, delta, or DLT confs
("quality"="bronze",
"pipelines.autoOptimize.managed"="true",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo"
 )
 AS 
 SELECT * 
 FROM STREAM(LIVE.raw_retail)

-- COMMAND ----------

-- DBTITLE 1,Perform ETL & Enforce Quality Expectations
-- SET pipelines.trigger.interval='1 hour';

CREATE STREAMING LIVE TABLE quality_retail
(
  CONSTRAINT has_customer EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_invoice EXPECT (InvoiceNo IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date_time EXPECT (CAST(InvoiceDatetime AS TIMESTAMP) IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (Country)
COMMENT "This is the raw bronze table with data cleaned (dates, etc.), data partitioned, and optimized"
TBLPROPERTIES 
("quality"="silver",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo"
 )
 AS (
   WITH step1 AS 
    (SELECT 
    *,
    split(InvoiceDate, " ") AS Timestamp_Parts
    FROM STREAM(LIVE.cleaned_retail)
    ),
      step2 AS (
      SELECT 
      *,
      split(Timestamp_Parts[0], "/") AS DateParts,
      Timestamp_Parts[1] AS RawTime
      FROM step1
      ),
          step3 AS (
          SELECT
          *,
          concat(lpad(DateParts[2], 4, "20"), "-", lpad(DateParts[0], 2, "0"),"-", lpad(DateParts[1], 2, "0")) AS CleanDate,
          lpad(RawTime, 5, '0') AS CleanTime
          FROM step2
          )
  SELECT
  InvoiceNo,
  StockCode,
  Description,
  Quantity,
  CleanDate AS InvoiceDate,
  CleanTime AS InvoiceTime,
  concat(CleanDate, " ", CleanTime)::timestamp AS InvoiceDatetime,
  UnitPrice,
  CustomerID,
  Country
  FROM step3
 )

-- COMMAND ----------

-- DBTITLE 1,Quarantine Data with Expectations
-- SET pipelines.trigger.interval='1 hour';

CREATE STREAMING LIVE TABLE quarantined_retail
(
  CONSTRAINT has_customer EXPECT (CustomerID IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_invoice EXPECT (InvoiceNo IS NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date_time EXPECT (InvoiceDate IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES 
("quality"="bronze",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo"
 )
AS 
SELECT 
 InvoiceNo,
  StockCode,
  Description,
  Quantity,
  InvoiceDate,
  UnitPrice,
  CustomerID,
  Country
FROM STREAM(LIVE.cleaned_retail);

-- COMMAND ----------

-- DBTITLE 1,Create Complete Tables -- Use Case #1 --  for metadata downstream
CREATE OR REFRESH LIVE TABLE distinct_countries_retail
AS
SELECT DISTINCT Country
FROM LIVE.quality_retail;

-- COMMAND ----------

-- DBTITLE 1,Create Complete Tables -- Summary Analytics
CREATE OR REFRESH LIVE TABLE sales_by_day
AS 
SELECT
date_trunc('day', InvoiceDatetime) AS Date,
SUM(Quantity) AS TotalSales
FROM (LIVE.retail_sales_all_countries)
GROUP BY date_trunc('day', InvoiceDatetime)
ORDER BY Date;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE sales_by_country
AS 
SELECT
Country,
SUM(Quantity) AS TotalSales
FROM (LIVE.retail_sales_all_countries)
GROUP BY Country
ORDER BY TotalSales DESC;

-- COMMAND ----------

-- SET pipelines.trigger.interval='1 hour';
CREATE OR REFRESH LIVE TABLE top_ten_customers
AS 
SELECT
CustomerID,
SUM(Quantity) AS TotalSales
FROM LIVE.retail_sales_all_countries
GROUP BY CustomerID
ORDER BY TotalSales DESC
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Upsert New Data APPLY CHANGES INTO
-- SET pipelines.trigger.interval='1 hour';

CREATE STREAMING LIVE TABLE retail_sales_all_countries
TBLPROPERTIES 
("quality"="silver",
"delta.tuneFileSizesForRewrites"="true",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo"
 );

APPLY CHANGES INTO LIVE.retail_sales_all_countries
FROM STREAM(LIVE.quality_retail)
KEYS (CustomerID, InvoiceNo)
SEQUENCE BY InvoiceDateTime

-- COMMAND ----------

-- DBTITLE 1,Just for Visuals -- Separate pipeline to split by country
-- SET pipelines.trigger.interval='1 hour';

CREATE STREAMING LIVE TABLE quality_retail_split_by_country
(
  CONSTRAINT has_customer EXPECT (CustomerID IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_invoice EXPECT (InvoiceNo IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_date_time EXPECT (CAST(InvoiceDatetime AS TIMESTAMP) IS NOT NULL) ON VIOLATION DROP ROW
)
PARTITIONED BY (Country)
COMMENT "This is the raw bronze table with data cleaned (dates, etc.), data partitioned, and optimized"
TBLPROPERTIES 
("quality"="silver",
"pipelines.autoOptimize.zOrderCols"="CustomerID, InvoiceNo"
 )
 AS 
 SELECT * FROM STREAM(LIVE.quality_retail)
