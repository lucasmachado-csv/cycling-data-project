# Cycling Dashboards

Dash apps for exploring London, NYC, and combined bike-ride data stored in `cycling.duckdb`.

## Prerequisites
- Python 3.10+ (miniconda recommended)
- Packages: `dash`, `duckdb`, `pandas`, `plotly`

Install (once):
```bash
pip install dash duckdb pandas plotly
```

## Data expectations
- Database file: `cycling.duckdb` in the project root.
- Tables:
  - `london_bike_data` (columns incl. `start_date`, `end_date`, `duration_seconds`, station info, `bike_model`).
  - `nyc_biking_data` (columns incl. `start_time`, `end_time`, station info, `user_type`; duration computed on the fly).
  - `joint_bike_data` (optional combined table with at least `start_time`, `end_time`, `duration_seconds`, `city`).

Example to build a simple combined table:
```sql
CREATE OR REPLACE TABLE joint_bike_data AS
SELECT start_date AS start_time,
       end_date   AS end_time,
       duration_seconds,
       'London' AS city
FROM london_bike_data
UNION ALL
SELECT start_time,
       end_time,
       date_diff('second', start_time, end_time) AS duration_seconds,
       'NYC' AS city
FROM nyc_biking_data
WHERE start_time IS NOT NULL AND end_time IS NOT NULL;
```

## Data extraction (via notebooks)
- **London** (`download_data_london.ipynb`): pulls TfL usage-stats CSVs (filters by Journey/Data/Extract), writes them to `data/`, then ingests to DuckDB using `read_csv(..., union_by_name=True)` and a COALESCE/CAST unification into `london_bike_data` (handles schema shifts and date formats).
- **NYC** (`duckdb_process_data.ipynb`): downloads Citi Bike monthly zips from S3 (excluding JC), unzips (nested zips handled), ingests CSVs with `read_csv(..., union_by_name=True)`, normalizes columns, computes `duration_seconds` as `end_time - start_time`, and writes `nyc_biking_data`.
- **Combined** (`duckdb_process_data.ipynb` step or standalone SQL): unions the two tables into `joint_bike_data` (see SQL above), adding `city`.
- **Cleanup/filters**: Notebooks show optional duration trimming (e.g., removing extreme rides) before persisting final tables.

## Apps
Run each in a separate terminal. The app listens on the port shown.

### London dashboard
```bash
/Users/lucas/miniconda3/bin/python app_london.py
# http://127.0.0.1:8050
```
Features: time series (rides, avg duration minutes, total days), top start/end stations, day/hour charts, top routes, bike model split, print-friendly layout.

### NYC dashboard
```bash
/Users/lucas/miniconda3/bin/python app_nyc.py
# http://127.0.0.1:8051
```
Features: same as London plus user_type filter and start-location density map; top routes included; print-friendly layout.

### Combined (London + NYC)
```bash
/Users/lucas/miniconda3/bin/python app_joint.py
# http://127.0.0.1:8052
```
Features: per-city time series (rides, avg minutes, total days), day-of-week and hour-of-day comparisons, top routes.

## Print-friendly tips
- Use the built-in “Download PDF” button (opens browser print dialog) or browser Print → Save as PDF.
- Charts use light templates and reduced padding; mode bars are hidden for cleaner printouts.

## Stopping apps
Press `Ctrl+C` in the terminal running the app. If a port is stuck:
```bash
lsof -i :8050   # or 8051/8052
kill <PID>
```

## Notes
- NYC durations are computed as `end_time - start_time` in seconds; London uses `duration_seconds` provided in the data.
- Top routes are capped at 10 and require non-null start/end station names.

