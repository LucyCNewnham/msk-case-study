import time
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from application.patient_journey_pipeline import DataPipeline
import schedule
from datetime import datetime, timedelta


load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def track_sla(pipeline_start_time, sla_threshold=timedelta(minutes=10)):
    elapsed_time = datetime.now() - pipeline_start_time
    if elapsed_time > sla_threshold:
        logging.warning(f"Pipeline run exceeded SLA of {sla_threshold}. Elapsed: {elapsed_time}")


def run_pipeline(db_url):
    """
    Function to fetch, transform, and load data into the database.
    """
    pipeline_start_time = datetime.now()

    db_engine = create_engine(db_url)
    pipeline = DataPipeline(db_engine)    

    try:
        schedule_data, activity_data, patient_journey_data, survey_result_data = pipeline.fetch_data()
        transformed_data = pipeline.transform(schedule_data, activity_data, patient_journey_data, survey_result_data)

        if not transformed_data.empty:
            # Load the transformed data
            pipeline.load(transformed_data)

    except Exception as e:
        logging.error(f"Pipeline run failed: {str(e)}")

    finally:
        track_sla(pipeline_start_time)
        if pipeline.get_failures():
            logging.error(f"Failures logged: {pipeline.get_failures()}")


def main():
    # Get database URL from .env
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("DATABASE_URL is not set in the environment.")

    # Run the pipeline initially
    run_pipeline(db_url)

    # repeat run every hour
    schedule.every(1).hour.do(run_pipeline, db_url=db_url)

    logging.info("Pipeline scheduled to run every hour.")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
