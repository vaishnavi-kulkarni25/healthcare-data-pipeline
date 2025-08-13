import pandas as pd

patients = pd.read_csv("data/raw/patients.csv")
visits = pd.read_csv("data/raw/visits.csv")

# Basic cleaning
patients = patients.drop_duplicates(subset=["patient_id"]).fillna({"city":"Unknown"})
visits = visits.drop_duplicates(subset=["visit_id"]).dropna(subset=["diagnosis"])

# Join
joined = visits.merge(patients, on="patient_id", how="left")

# Aggregation
billing_by_city = joined.groupby("city")["cost"].sum().reset_index().rename(columns={"cost":"total_billed"})
billing_by_city.to_csv("output/gold_billing_by_city.csv", index=False)

joined.to_csv("output/visits_enriched.csv", index=False)
print("Pandas ETL done. Check output/ folder.")
