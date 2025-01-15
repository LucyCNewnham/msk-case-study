# msk-case-study

## Overview

This repository contains a case study solution. The code is a data processing pipeline that integrates scheduling, transformation, and loading of patient data. It demonstrates the use of Python to handle database operations, scheduling repetitive tasks, and error handling.

---

## Features

### 1. **Scheduling**

The pipeline runs at a regular interval as defined in the code. While an event-driven architecture (e.g., a FastAPI app with database event listeners) would be ideal for a production system, this case study uses the Python `schedule` library for simplicity. The `time.sleep` function is used to prevent excessive CPU usage between scheduled runs.

### 2. **Data Transformation and Loading**

The pipeline performs the following steps:

- **Fetch data**: Retrieves necessary tables from the database.
- **Transform data**: Processes and merges data to create the required structure, while ensuring duplicate rows are excluded based on pre-existing database records.
- **Load data**: Writes the transformed data back into a table (`patient_journey_schedule_window`) in the database.

### 3. **Failure Logging**

The pipeline tracks and logs failures, including errors in parsing schedule slugs or database operations. Failures are saved in a separate table in the same database for troubleshooting.

---

## Requirements

### Python Dependencies

Install the necessary Python dependencies using:

```bash
pip install -r requirements.txt
```

For unit testing, additional dependencies can be installed using:

```bash
pip install -r requirements-test.txt
```

### Environment Variables
The pipeline requires access to a database. Create a .env file in the root directory with the following format:

``` dotenv
DATABASE_URL=your_database_url_here
```
Replace your_database_url_here with the actual connection string for your database.

## How to Use
1. Setup the Environment
Ensure you have the necessary dependencies and access to the database. Install the requirements as described above.

2. Run the Pipeline
To execute the pipeline, run:

```bash
python main.py
```
This will:

Trigger the pipeline immediately.
Schedule the pipeline to run every hour.
If you want to test the pipeline more frequently:

Open main.py.

Locate the line:

```python
schedule.every(1).hour.do(run_pipeline, db_url=db_url)
```
Replace it with:

```python
schedule.every(1).minute.do(run_pipeline, db_url=db_url)
```
3. Inspect Failures
Failures encountered during the pipeline run are logged in failures.log. You can open this file to review any parsing errors or issues encountered while processing data.

## Project Structure

```bash
msk-case-study/
├── application/
│   ├── patient_journey_pipeline.py   # Main logic for data pipeline
├── tests/
│   ├── test_patient_journey.py       # Unit tests for the pipeline
├── requirements.txt                  # Core dependencies
├── requirements-test.txt             # Additional testing dependencies
├── main.py                           # Entry point to run the pipeline
├── .env                              # Environment variables (ignored in Git)
```
## How it Works
### Fetching Data

The DataPipeline class fetches the required tables (schedule, activity, patient_journey, survey_result) from the database.

### Transformation

Data is processed and transformed:

Schedule slugs are parsed into structured components (e.g., offsets, milestones).
Existing records are excluded from the new dataset to avoid duplicates.
Milestone slugs are formatted and matched with the appropriate milestone dates.

### Loading

The transformed data is written back into the database. New rows are appended to the patient_journey_schedule_window table.

### Error Handling

Parsing failures and other issues are logged for debugging, and failures are also stored in a created table (`pipeline_failures`) in the database.

### Unit Testing
The project includes a suite of unit tests to verify the correctness of the pipeline.

#### To run the tests:

```bash
pytest tests/
```