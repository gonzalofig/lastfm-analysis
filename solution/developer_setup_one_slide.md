# Developer Setup - One Slide Draft

## Goal

Deliver a reproducible, scalable Spark workflow for session analysis and a lightweight ML forecast notebook for the selected top user.

## Preferred setup

- **Language:** Python
- **Distributed engine:** Spark
- **Storage format:** Raw TSV for ingestion, Parquet for repeated analytical reads
- **Development interface:** Jupyter notebooks
- **Version control:** GitHub

## Workflow

1. Load the raw Last.fm TSV from `data/raw/`
2. Normalize and stage the dataset once to Parquet
3. Run EDA to validate IDs, names, and timestamp quality
4. Use walkthrough notebooks to explain the logic step by step
5. Use production notebooks to regenerate the final answers quickly

## Why this setup

- **Scalable:** Spark handles the full dataset and sessionization logic
- **Efficient:** Parquet reduces repeated I/O and speeds up downstream queries
- **Reviewable:** The notebook flow separates understanding from final export
- **Reproducible:** The README provides the exact run order and expected outputs
