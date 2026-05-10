# Coding Challenge - Last.fm Case Study

This repository contains a notebook-first solution for the Last.fm coding and ML exercises.

The raw dataset is delivered as TSV, but the first notebook stages it to Parquet so the rest of the analysis runs in a format that is much more efficient for repeated Spark reads. The review path is intentionally split in two:

1. `EDA + walkthrough notebooks` to understand the data and the reasoning step by step.
2. `Production notebooks` to rerun the final answers quickly once the approach is clear.

## Submission artifacts

- `exercise_2_answer.tsv`: required tab-separated answer file for Exercise 2.
- `exercise_3_answer.tsv`: required tab-separated answer file for Exercise 3.
- `outputs/`: exported TSV/parquet artifacts, summaries, and executed notebook copies.
- `solution/`: very short reviewer-facing summary of the final answers.

## Data bootstrap

No manual dataset placement is required.

On the first run, `00_setup_and_load_data.ipynb` downloads the official `lastfm-dataset-1K.tar.gz` archive from Zenodo into `data/raw/`, verifies the archive checksum, extracts the required TSV files, and stages the event data to Parquet.

After the first successful run, the extracted event file will be available at a path like:

`data/raw/lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv`

## Repository structure

- `notebooks/`: all analysis and final-solution notebooks.
- `src/`: reusable Spark and forecasting helpers imported by the notebooks.
- `solution/`: concise answer package.
- `outputs/`: final exports and executed notebooks.
- `docker/requirements.txt`: Python dependency list for local notebook execution.
- `exercise_2_answer.tsv`: formal Exercise 2 submission file.
- `exercise_3_answer.tsv`: formal Exercise 3 submission file.
- `data/raw/`: raw extracted source data, not committed.
- `data/processed/`: staged Parquet dataset, generated on first run and not committed.

## Notebook order

Run the notebooks in this order:

1. `notebooks/00_setup_and_load_data.ipynb`
2. `notebooks/01_data_eda_and_assumptions.ipynb`
3. `notebooks/02_exercise_2_walkthrough.ipynb`
4. `notebooks/03_exercise_2_production_solution.ipynb`
5. `notebooks/04_exercise_3_walkthrough.ipynb`
6. `notebooks/05_exercise_3_production_solution.ipynb`

Why this order:

- `00` prepares the environment and stages the TSV to Parquet.
- `01` documents the EDA and the assumptions, including IDs, names, and timestamp quality.
- `02` explains Exercise 2 step by step.
- `03` is the fast rerun notebook for the final Exercise 2 outputs.
- `04` explains Exercise 3 step by step.
- `05` is the fast rerun notebook for the final Exercise 3 outputs.

## How to run the application step by step

### Option A: Local notebook execution

1. Clone the repository.
2. Create and activate a Python environment.
3. Install the dependencies:

   ```bash
   python -m pip install -r docker/requirements.txt
   python -m pip install ipykernel nbconvert
   ```

4. Make sure the machine has internet access for the first run, because the dataset archive is downloaded automatically from Zenodo.
5. Open Jupyter Lab or VS Code in the repository root.
6. Open `notebooks/00_setup_and_load_data.ipynb` and select your Python kernel.
7. Run `notebooks/00_setup_and_load_data.ipynb`. This notebook will:
   - download `lastfm-dataset-1K.tar.gz` automatically if it is not already present
   - verify the archive checksum
   - extract the required TSV files
   - stage the event dataset to Parquet
   - validate that the normalized events table loads correctly
8. Run the remaining notebooks in the order listed in the previous section.
9. After `03_exercise_2_production_solution.ipynb` finishes, the formal Exercise 2 answer will be available at:

   `exercise_2_answer.tsv`

10. After `05_exercise_3_production_solution.ipynb` finishes, the formal Exercise 3 answer will be available at:

   `exercise_3_answer.tsv`

11. After `05_exercise_3_production_solution.ipynb` finishes, the Exercise 3 forecast artifacts will be refreshed under:

   `outputs/`

### Option B: Headless execution from the terminal

If you prefer to execute the notebooks without opening the UI:

```bash
python -m nbconvert --to notebook --execute --inplace notebooks/00_setup_and_load_data.ipynb
python -m nbconvert --to notebook --execute --inplace notebooks/01_data_eda_and_assumptions.ipynb
python -m nbconvert --to notebook --execute --inplace notebooks/02_exercise_2_walkthrough.ipynb
python -m nbconvert --to notebook --execute --inplace notebooks/03_exercise_2_production_solution.ipynb
python -m nbconvert --to notebook --execute --inplace notebooks/04_exercise_3_walkthrough.ipynb
python -m nbconvert --to notebook --execute --inplace notebooks/05_exercise_3_production_solution.ipynb
```

## What each notebook produces

- `00_setup_and_load_data.ipynb`
  - downloads the dataset archive automatically if needed
  - verifies the archive checksum
  - extracts the required TSV files
  - stages the raw TSV to Parquet
  - validates schema, row counts, and basic date coverage
- `01_data_eda_and_assumptions.ipynb`
  - inspects missing IDs and names
  - inspects timestamp parse quality and date coverage
  - tests whether exact string-based ID backfilling is safe enough to use
  - records the assumptions used by the final solution
- `02_exercise_2_walkthrough.ipynb`
  - shows the session rule on a sample user
  - builds session summaries
  - derives the top 50 longest sessions and the top 10 songs
- `03_exercise_2_production_solution.ipynb`
  - exports `exercise_2_answer.tsv`
  - exports `outputs/exercise_2_answer.tsv`
  - exports `outputs/exercise_2_answer.tsv`
  - exports `outputs/top_10_songs_in_top_50_sessions.tsv`
  - exports `outputs/top_50_longest_sessions.tsv`
  - exports `outputs/analysis_summary.md`
- `04_exercise_3_walkthrough.ipynb`
  - identifies the top user by session count
  - builds the monthly history for the selected metric
  - shows the feature engineering and recursive forecast logic step by step
- `05_exercise_3_production_solution.ipynb`
  - exports `exercise_3_answer.tsv`
  - exports `outputs/exercise_3_monthly_session_history_top_user.tsv`
  - exports `outputs/exercise_3_answer.tsv`
  - exports `outputs/exercise_3_forecast_next_3_months.tsv`
  - exports `outputs/exercise_3_model_coefficients.tsv`
  - exports `outputs/exercise_3_summary.md`

## Core helper function reference

Each notebook starts with a function reference in the format `description / input / output / efficiency rationale`. This is the quick map of the public helpers used throughout the repo.

### `create_spark_session(app_name, master='local[*]', shuffle_partitions=64)`

- **Description:** Builds the Spark session with local Java/Hadoop setup and Spark defaults tuned for this project.
- **Input:** Application name plus optional local execution settings.
- **Output:** Configured `SparkSession`.
- **Why selected for efficiency:** Centralizes adaptive execution, shuffle configuration, compression, and temp-directory settings so every notebook benefits from the same scalable defaults.

### `resolve_lastfm_input_path(project_root)`

- **Description:** Finds the extracted Last.fm TSV under `data/raw/`.
- **Input:** Repository root path.
- **Output:** Absolute `Path` to the TSV input file.
- **Why selected for efficiency:** Avoids hardcoded paths and prevents repetitive manual setup.

### `ensure_lastfm_dataset_available(project_root, force_download=False, force_extract=False, verify_md5=True)`

- **Description:** Downloads the official Last.fm archive if needed, verifies the checksum, and extracts the required TSV files into `data/raw/`.
- **Input:** Repository root path plus optional refresh and verification controls.
- **Output:** Absolute `Path` to the extracted event TSV.
- **Why selected for efficiency:** Removes the human setup step while keeping the bootstrap idempotent. If the archive and extracted files already exist, the function returns immediately instead of downloading again.

### `stage_lastfm_events_to_parquet(spark, project_root, overwrite=False, parquet_partitions=None)`

- **Description:** Reads the raw TSV, normalizes the event schema, repartitions by user, and writes the result to Parquet.
- **Input:** Spark session, project root, and optional overwrite/partition controls.
- **Output:** Path to the staged Parquet dataset.
- **Why selected for efficiency:** The expensive TSV parse happens once; later runs reuse a compressed columnar format that Spark reads much faster.

### `load_lastfm_events(spark, project_root_or_input_path, prefer_parquet=True, refresh_parquet=False, parquet_partitions=None)`

- **Description:** Loads the normalized event dataset from Parquet by default, or directly from TSV when needed.
- **Input:** Spark session plus the project root or a direct file path.
- **Output:** Spark DataFrame of normalized play events.
- **Why selected for efficiency:** Preserves a flexible input path while making Parquet the default fast path.

### `compute_sessions(events_df, gap_minutes=20)`

- **Description:** Assigns each play event to a user session using the 20-minute gap rule.
- **Input:** Event-level Spark DataFrame and the session gap threshold.
- **Output:** Event-level Spark DataFrame enriched with session metadata.
- **Why selected for efficiency:** Best used in walkthroughs and validation because it makes the session rule visible row by row.

### `summarize_sessions_from_events(events_df, gap_minutes=20)`

- **Description:** Builds one row per session directly from the event table with Spark `session_window`.
- **Input:** Event-level Spark DataFrame and the session gap threshold.
- **Output:** Session summary Spark DataFrame with counts, boundaries, durations, and generated session IDs.
- **Why selected for efficiency:** This is the scalable sessionization path because it avoids storing a huge row-level sessionized table for the whole dataset.

### `top_longest_sessions_by_track_count(sessionized_df=None, session_summary_df=None, top_session_count=50)`

- **Description:** Selects the top sessions ordered by track count.
- **Input:** Either a sessionized event DataFrame or a session summary DataFrame.
- **Output:** Top-session Spark DataFrame.
- **Why selected for efficiency:** Reduces the search space early, which keeps later joins and aggregations much smaller.

### `top_songs_from_longest_sessions(events_or_sessionized_df, session_summary_df=None, top_session_count=50, top_song_count=10)`

- **Description:** Computes the final song ranking inside the top sessions.
- **Input:** Event-level or sessionized DataFrame plus the session summary when needed.
- **Output:** Ranked Spark DataFrame of top songs.
- **Why selected for efficiency:** Broadcasts the small top-session result back to the events table instead of carrying the full session space through every step.

### `top_users_by_session_count(sessionized_df=None, session_summary_df=None, top_user_count=10)`

- **Description:** Ranks users by total number of sessions.
- **Input:** Sessionized or session-summary DataFrame.
- **Output:** Spark DataFrame of top users with date coverage.
- **Why selected for efficiency:** The ML exercise only needs the top user, so the rest of the workflow can quickly narrow to a tiny subset.

### `get_top_user_for_ml(session_summary_df)`

- **Description:** Returns the single user selected for the ML forecast exercise.
- **Input:** Session summary Spark DataFrame.
- **Output:** Python dictionary with user ID, session count, and date range.
- **Why selected for efficiency:** Encapsulates the user-selection step so the ML notebook remains focused on the forecast logic.

### `build_monthly_metric_history(session_summary_df, user_id, metric='session_count')`

- **Description:** Builds a continuous monthly history for one user and one selected metric.
- **Input:** Session summary Spark DataFrame, user ID, and metric name.
- **Output:** Pandas DataFrame with one row per month.
- **Why selected for efficiency:** Spark handles the large aggregation first; only the small per-user monthly series is moved into Pandas for forecasting.

### `run_monthly_forecast_for_top_user(session_summary_df, metric='session_count', forecast_periods=3)`

- **Description:** Runs the full Exercise 3 forecast pipeline for the selected top user.
- **Input:** Session summary Spark DataFrame, selected metric, and forecast horizon.
- **Output:** `ForecastArtifacts` dataclass containing history, forecast, coefficients, and validation metrics.
- **Why selected for efficiency:** Keeps distributed session computation in Spark while solving the small forecasting task with lightweight Pandas/NumPy code.

## Assumptions

- A new session starts when the gap between consecutive track start times for the same user is strictly greater than 20 minutes.
- The raw dataset is treated as the source of truth. No fuzzy entity resolution is applied.
- Missing identifier rates observed in the EDA:
  - `track_id` is missing in `2,168,587` rows, about `11.32%`.
  - `artist_id` is missing in `602,165` rows, about `3.14%`.
- Exact string-based ID recovery was tested, but it is too small to justify making it part of the core pipeline:
  - only `421` missing `track_id` rows can be backfilled safely from an exact unique `(artist_name, track_name)` mapping
  - only `1,248` missing `artist_id` rows can be backfilled safely from an exact unique `artist_name` mapping
- Because exact recoverable coverage is negligible, the final Exercise 2 answer groups songs by exact `artist_name + track_name`, which preserves valid plays that would be lost if only `track_id` were used.
- Timestamp parsing uses the original UTC format in the dataset, and rows with unparsable timestamps are excluded after inspection in the EDA notebook.
- For scalability, the raw TSV is converted once into a Parquet dataset and all repeated reads use Parquet.
- For Exercise 3, the selected metric is `number of sessions per month`.
- The top user for Exercise 3 is the user with the highest total number of sessions in the full dataset.
- The monthly timeline is made continuous by filling missing months with `0` before training the model.
- The forecast starts in the month immediately after the last observed month for the selected user.

## Final outputs

The key deliverables are:

- `exercise_2_answer.tsv`
- `exercise_3_answer.tsv`
- `outputs/exercise_2_answer.tsv`
- `outputs/exercise_2_answer.tsv`
- `outputs/top_10_songs_in_top_50_sessions.tsv`
- `outputs/top_50_longest_sessions.tsv`
- `outputs/exercise_3_answer.tsv`
- `outputs/exercise_3_forecast_next_3_months.tsv`
- `outputs/analysis_summary.md`
- `outputs/exercise_3_summary.md`

## Things I would improve with more time

- Create a simple pipeline for the full workflow.
- Add unit tests for session boundaries and monthly feature generation.
- Add basic data-quality checks for timestamps, duplicates, and missing metadata.
- Try different forecasting models and compare results.
- Automate notebook and output validation in CI.
