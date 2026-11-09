# Rearc Data Quest: BLS Productivity + US Population (Databricks)

Idempotent ingestion of the BLS "Major Sector Productivity and Costs" (`pr`) time series and a US population API into a Unity Catalog Volume, modeled as a Bronze / Silver / Gold Spark Declarative Pipeline.

Design decisions, data findings and trade-offs are in [PROCESS.md](PROCESS.md).

## Repository layout

```
.
├── ingestion/
│   └── ingest.py                         # BLS sync + population API pull into the Volume
├── pipeline/                             # Spark Declarative Pipeline source files
│   ├── bronze.py                         # raw layer (materialized views)
│   ├── silver.py                         # typed, deduplicated, expectations
│   ├── gold.sql                          # Q1 and Q2 (SQL primary)
│   └── gold_q3.sql                       # Q3 (SQL primary)
├── validation/
│   └── alternatives_and_validation.py    # PySpark/SQL alternatives + equivalence checks
├── docs/                                 # screenshots
├── PROCESS.md
└── README.md
```

## Environment

| Item | Value |
|---|---|
| Platform | Databricks Free Edition (serverless), Unity Catalog |
| Raw files | Volume `rearc.quest.raw` (`/Volumes/rearc/quest/raw`) |
| Bronze tables | `rearc.bronze.bls_data`, `bls_series`, `population` |
| Silver tables | `rearc.silver.bls_data`, `series`, `population` |
| Gold tables | `rearc.gold.population_stats`, `best_year_per_series`, `prs30006032_q01_with_population` |

Volume layout:

```
/Volumes/rearc/quest/raw/
├── bls/
│   ├── _manifest.json          # ETag / Last-Modified / Content-Length per file
│   └── pr/                     # every file in the BLS pr/ directory
└── population/
    └── population.json
```

## How to run

1. **Create the objects once** (SQL Editor):
   ```sql
   CREATE SCHEMA IF NOT EXISTS rearc.quest;
   CREATE VOLUME IF NOT EXISTS rearc.quest.raw;
   CREATE SCHEMA IF NOT EXISTS rearc.bronze;
   CREATE SCHEMA IF NOT EXISTS rearc.silver;
   CREATE SCHEMA IF NOT EXISTS rearc.gold;
   ```
2. **Ingest.** Open `ingestion/ingest.py` as a Python notebook. Set your real name and email in `HEADERS` (BLS requires a User-Agent with contact info; without it requests are blocked). Run it.
   - The first run downloads every file in the BLS directory.
   - A re-run with an unchanged source downloads nothing (`'downloaded': []`).
3. **Create the pipeline.** Jobs & Pipelines > Create > ETL pipeline, default catalog `rearc`, and add the four files in `pipeline/` as source files. Table names are fully qualified in the code, so each layer lands in its own schema. Click **Run pipeline**.
4. **Validate.** Run `validation/alternatives_and_validation.py` in a Python notebook after the pipeline finishes. It should print three `OK` lines.

## Results

| Question | Result |
|---|---|
| Q1: mean / std dev of US population, 2013-2018 | mean **322,069,808**; sample std dev **4,158,441.04** (`rearc.gold.population_stats`) |
| Q2: best year per series | 337 series, each with a human-readable `series_label` (`rearc.gold.best_year_per_series`) |
| Q3: `PRS30006032`, `Q01`, with population | 39 yearly rows, 1988-2026; population filled for 2013-2019 and 2021-2024 (`rearc.gold.prs30006032_q01_with_population`) |

## SQL vs PySpark

Each Gold question is implemented in both languages. The primary version feeds the Gold table inside the pipeline, and the other is the documented alternative.

| Question | Primary (feeds Gold) | Alternative |
|---|---|---|
| Q1 | Spark SQL (`gold.sql`) | PySpark (`validation/alternatives_and_validation.py`) |
| Q2 | Spark SQL (`gold.sql`) | PySpark (`validation/alternatives_and_validation.py`) |
| Q3 | Spark SQL (`gold_q3.sql`) | Standalone `spark.sql` join query in `validation/alternatives_and_validation.py` (see the note in PROCESS.md) |

The validation script compares each alternative with its Gold table: Q2 and Q3 with `exceptAll` in both directions (exact), and Q1 with a relative tolerance of 1e-6 because floating-point aggregates can differ in the last digits.

## Access control (read-only analyst)

Intended grants for an analyst consuming only the Gold layer. These are shown as SQL and were not executed in this workspace.

```sql
GRANT USE CATALOG ON CATALOG rearc TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA rearc.gold TO `analysts`;
```

## Screenshots

| File | Shows |
|---|---|
| `docs/pipeline_dag.png` | Pipeline graph: all nine tables green, 0 errors |
| `docs/data_quality_silver_bls_data.png` | Expectation results on `silver.bls_data` (92,328 written, 0 dropped) |
| `docs/gold_layer_data.png` | Q2 output with `series_label` |
| `docs/idempotency_second_run.png` | Second ingestion run: `downloaded: []` |
| `docs/validation_ok.png` | SQL vs PySpark validation: three `OK` lines |
