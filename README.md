# healthcare-data-pipeline
A PySpark-based healthcare ETL pipeline for data cleaning, transforming and analytics
# Healthcare Data Pipeline (Big Data Project)

## Overview
This project demonstrates a Big Data ETL pipeline for healthcare analytics using PySpark.  
It simulates patient and visit data processing from raw ingestion to cleaned, transformed datasets ready for analysis.

*Tech Stack:*
- Apache Spark (PySpark)
- HDFS
- Hive
- AWS S3 (optional)
- Python

---

## Project Architecture
1. *Raw Data Ingestion* → Load CSV files from source.
2. *Data Cleaning* → Handle nulls, remove duplicates, fix schema mismatches.
3. *Transformation* → Join datasets, create analytics tables.
4. *Data Output* → Store cleaned data in CSV/Parquet for BI tools.

---

## Folder Structure
