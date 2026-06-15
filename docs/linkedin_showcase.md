# LinkedIn Showcase Post Template

Copy and customize this template to share your project on LinkedIn and attract attention from Data Engineering recruiters and peers!

---

### Suggested Post Draft

🚀 **Excited to share my latest Data Engineering project: An End-to-End Automated Data Lakehouse Pipeline built on Databricks!** 🚖

In modern data platforms, ingestion isn't just about moving data from A to B—it's about building a robust, cost-effective ecosystem that handles late-arriving data, tracks dimension changes, and scales seamlessly. 

For this project, I built a batch-processing lakehouse using **NYC Taxi (TLC) trip record data**, applying the **Medallion Architecture** governed by **Unity Catalog**.

Here are the key technical pillars of the implementation:

🔹 **Medallion Layering (Delta Lake & Spark):**
- **Bronze (Raw Ingestion):** Replicates the landing parquet schema with added audit timestamps to maintain lineage.
- **Silver (Cleansing & Enrichment):** Standardizes field names, translates codes to strings, calculates duration metrics, and handles geographical mapping via dual joins to pick up and drop off coordinates.
- **Gold (BI Ingestion):** Pre-aggregates metrics (fares, distance, passenger totals, revenues) grouped daily for BI consumption.

🔹 **Database Modeling with Slowly Changing Dimensions (SCD Type 2):**
Implemented a custom three-pass merge pattern on the `taxi_zone_lookup` dimension table in Silver. This ensures that changes to location zones (renames, zone shifts) are tracked historically using `effective_date`, `end_date`, and record active flags, ensuring absolute reporting accuracy.

🔹 **Dynamic Incremental Processing & Handling Vendor Lag:**
Data pipelines must handle real-world scenarios. In this pipeline:
- Historical backfill covers **Oct 2025 - Mar 2026** using `overwrite` patterns.
- Monthly runs target the month with a **2-month vendor reporting lag** (processing April 2026 data in June 2026).
- If the file is not yet posted or is already ingested, the pipeline dynamically triggers conditional logic to exit early, saving compute costs.

🔹 **Workflow Orchestration (Databricks Jobs):**
Orchestrated the entire end-to-end flow as a serverless DAG. It leverages conditional task nodes (`continue_downstream_lookup` and `continue_downstream_yellow`) to check dependencies before running heavy transformations.

📁 **GitHub Repository:** [Insert your GitHub URL here]

I learned a lot about Spark optimization, Unity Catalog governance, and designing workflows for real-world vendor delays. 

If you are a Data Engineer, Recruiter, or Analytics professional, I would love to connect and hear your thoughts on optimization strategies!

#DataEngineering #Databricks #ApacheSpark #DeltaLake #CloudComputing #Python #BigData #UnityCatalog

---

### Tips for Maximizing Engagement:
1. **Visuals are Key:** Attach `docs/medallion_final.gif` or `docs/orchestration.png` to the post. Animated images (or PDFs uploaded as slides) get significantly higher impressions on LinkedIn.
2. **Tag Tech Stack Leaders:** Consider tagging **Databricks** or **Delta Lake** if appropriate.
3. **Pin it:** Once posted, feature this on your LinkedIn profile's "Featured" section so recruiters see it first when viewing your profile.
