# ETL Pipeline Project

## Overview

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline built using Python. It processes app and review datasets, performs data cleaning and transformation, and loads the final results into a SQLite database.

The pipeline is designed with a modular structure and includes logging and validation steps to simulate a production-style workflow.

---

## Tech Stack

* Python
* Pandas
* SQLite
* Logging

---

## Pipeline Workflow

### 1. Extract

* Reads data from CSV files (`apps_data.csv`, `review_data.csv`)
* Logs dataset shape and column information for visibility

### 2. Transform

* Removes duplicate records from both datasets
* Filters apps based on a selected category
* Aggregates review sentiment using `groupby()`
* Merges app and review data using `pd.merge()`
* Cleans data:

  * Converts `Reviews` column to numeric using `pd.to_numeric(errors="coerce")`
  * Handles invalid values safely
  * Drops rows only when both `Reviews` and `Rating` are missing
* Filters top apps based on:

  * Minimum rating threshold
  * Minimum number of reviews
* Sorts results by rating and review count
* Saves the transformed dataset to `top_apps.csv`

---

## 3. Load

* Loads the transformed dataset into a SQLite database (`market_research.db`)
* Stores data in table: `top_apps`
* Validates data by comparing row counts before and after loading

---

## Key Features

* Modular ETL pipeline (`extract`, `transform`, `load`)
* Structured logging for tracking pipeline execution
* Safe handling of missing and invalid data
* SQL-style joins using `pd.merge()`
* Data validation after loading

---

## Project Structure

```text
project/
│── data/
│   ├── apps_data.csv
│   ├── review_data.csv
│
│── output/
│   ├── top_apps.csv
│   ├── market_research.db
│
│── src/
│   └── etl_pipeline.py
│
│── README.md
```

---

## How to Run

```bash
python etl_pipeline.py
```

---

## Output

* `top_apps.csv` → cleaned and filtered dataset
* `market_research.db` → SQLite database containing processed data

---

## Future Improvements

* Add Apache Airflow for scheduling and orchestration
* Integrate cloud storage (e.g., AWS S3)
* Implement logging to file
* Add more robust data validation checks

---

## Acknowledgement

This project was inspired by learning resources from DataCamp.
The dataset and initial concepts were adapted, while the pipeline design, transformations, logging, and overall structure were independently implemented and enhanced.
