import time
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from application.patient_journey_pipeline import DataPipeline
import schedule

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_pipeline(db_url):
    """
    Function to fetch, transform, and load data into the database.
    """
    db_engine = create_engine(db_url)

    pipeline = DataPipeline(db_engine)

    # Fetch data from the database
    schedule_data, activity_data, patient_journey_data, survey_result_data = pipeline.fetch_data()

    # Transform the data
    transformed_data = pipeline.transform(schedule_data, activity_data, patient_journey_data, survey_result_data)

    if not transformed_data.empty:
        # Load the transformed data
        pipeline.load(transformed_data)


def main():
    # Get database URL from .env
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("DATABASE_URL is not set in the environment.")

    # Run the pipeline initially
    run_pipeline(db_url)

    # Schedule it to run every hour
    schedule.every(1).hour.do(run_pipeline, db_url=db_url)

    logging.info("Pipeline scheduled to run every hour.")

    while True:
        schedule.run_pending()
        time.sleep(1)  # Prevent busy-waiting


if __name__ == "__main__":
    main()
