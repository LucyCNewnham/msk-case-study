import pandas as pd
from datetime import datetime
import re
import logging
from sqlalchemy import create_engine 
from dotenv import load_dotenv 
import os
from application.patient_journey_pipeline import DataPipeline

load_dotenv()

def main():
    # Get database URL from .env
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise EnvironmentError("DATABASE_URL is not set in the environment.")

    db_engine = create_engine(db_url)

    pipeline = DataPipeline(db_engine)
    
    # Fetch data from the database
    schedule_data, activity_data, patient_journey_data, survey_result_data = pipeline.fetch_data()

    # Transform the data
    transformed_data = pipeline.transform(schedule_data, activity_data, patient_journey_data, survey_result_data)

    # Load the transformed data
    pipeline.load(transformed_data)

if __name__ == "__main__":
    main()
