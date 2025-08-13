from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, when

def main():
    spark = SparkSession.builder.appName("HealthcarePipeline").getOrCreate()

    patients = spark.read.csv("data/raw/patients.csv", header=True, inferSchema=True)
    visits = spark.read.csv("data/raw/visits.csv", header=True, inferSchema=True)

    # Cleaning
    patients = patients.dropDuplicates(["patient_id"]).na.fill({"city": "Unknown"})
    visits = visits.dropDuplicates(["visit_id"]).na.drop(subset=["diagnosis"])

    # Parse date
    visits = visits.withColumn("visit_date", to_date(col("visit_date"), "yyyy-MM-dd"))

    # Join
    joined = visits.join(patients, on="patient_id", how="left")

    # Example aggregation (gold)
    billing_by_city = joined.groupBy("city").sum("cost").withColumnRenamed("sum(cost)", "total_billed")
    billing_by_city.show(truncate=False)

    # Save outputs locally (CSV / Parquet)
    joined.write.mode("overwrite").parquet("output/silver/visits_enriched")
    billing_by_city.write.mode("overwrite").csv("output/gold/billing_by_city", header=True)

    spark.stop()

if _name_ == "_main_":
    main()
