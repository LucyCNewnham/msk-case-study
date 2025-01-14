import pandas as pd
from datetime import datetime
import re
import logging
from sqlalchemy import create_engine 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ScheduleParser:
    """
    Class to parse schedule slugs and extract schedule-related details.
    """

    @staticmethod
    def parse_schedule_slug(schedule_slug):
        """
        Parse the schedule slug and extract start offset, end offset, and milestone slug.
        Handles various slug formats. Full disclosure, some edge cases may not be taken into consideration
        """

        pattern = "42d-pre-op"
        match = re.match(pattern, schedule_slug)
        if match:
            print(1)

        # Check for formats like '1d-pre-1dpo' ## This is an edge case # TODO: see how i actually want to deal with this
        pattern = r"(\d+)([dwmy])-([a-z-]+)-(\d+)([dwmy])([a-z-]+)"
        match = re.match(pattern, schedule_slug)
        if match:
            start_offset, start_unit, text, end_offset, end_unit, milestone_slug = match.groups()
            start_offset_days = ScheduleParser.convert_to_days(int(start_offset), start_unit)
            end_offset_days = -ScheduleParser.convert_to_days(int(end_offset), end_unit)
            
            return start_offset_days, end_offset_days, milestone_slug

        # Check for formats like '4d-2d-pre-op'
        pattern = r"(\d+)([dwmy])-(\d+)([dwmy])-([a-z-]+)"
        match = re.match(pattern, schedule_slug)
        if match:
            start_offset, start_unit, end_offset, end_unit, milestone_slug = match.groups()
            start_offset_days = ScheduleParser.convert_to_days(int(start_offset), start_unit)
            end_offset_days = ScheduleParser.convert_to_days(int(end_offset), end_unit)

            # Adjust for negative days if 'pre' or similar is in the milestone_slug
            if 'pre' in milestone_slug:
                start_offset_days = -start_offset_days
                end_offset_days = -end_offset_days

            return start_offset_days, end_offset_days, milestone_slug

        # Check for formats like 'reg' or 'inv'
        pattern = r"([a-z-]+)$"
        match = re.match(pattern, schedule_slug)
        if match:
            print(schedule_slug)
            return None, None, schedule_slug
        
        # Check for formats like 'op-10d-post-op'
        pattern = r"([a-z-]+)-(\d+)([dwmy])-([a-z-]+)"
        match = re.match(pattern, schedule_slug)
        if match:
            start_offset, end_offset, end_unit, milestone_slug = match.groups()
            end_offset_days = ScheduleParser.convert_to_days(int(end_offset), end_unit)

            # Adjust for negative days if 'pre' or similar is in the milestone_slug
            if 'pre' in milestone_slug:
                end_offset_days = -end_offset_days

            return 0, end_offset_days, milestone_slug

        # Check for single time frame slugs like '3m-post-op' OR formats like '4d-op-pre-op'
        pattern = r"(\d+)([dwmy])-([a-z-]+)-([a-z-]+)"
        match = re.match(pattern, schedule_slug)
        if match:
            start_offset, start_unit, end, milestone_slug = match.groups()
            start_offset_days = ScheduleParser.convert_to_days(int(start_offset), start_unit)

            # Adjust for negative days if 'pre' or similar is in the milestone_slug
            if 'pre' in end or 'pre' in milestone_slug:
                start_offset_days = -start_offset_days

            if end == 'pre' or end == 'post':
                # this means cases like '3m-post-op' have ben incorrectly split up
                milestone_slug = end + '-' + milestone_slug
                return start_offset_days, None, milestone_slug

            return start_offset_days, 0, milestone_slug

        logging.warning(f"Unable to parse schedule_slug: {schedule_slug}")
        return None, None, None

    @staticmethod
    def convert_to_days(value, unit):
        if unit == 'd':
            return value
        elif unit == 'm':
            return value * 30  # Approximation for months
        elif unit == 'w':
            return value * 7
        elif unit == 'y':
            return value * 365 # not including leap years
        else:
            raise ValueError(f"Unknown time unit: {unit}")


class DataPipeline:
    """
    Data pipeline to transform and load the denormalized data.
    """

    def __init__(self, db_engine):
        self.db_engine = db_engine

    def fetch_data(self):
        """
        Fetch data from the database.
        """
        logging.info("Fetching data from database...")

        def query(table):
            return f"SELECT * FROM {table}"

        schedule_data = pd.read_sql(query('schedule'), self.db_engine)
        activity_data = pd.read_sql(query('activity'), self.db_engine)
        patient_journey_data = pd.read_sql(query('patient_journey'), self.db_engine)
        survey_result_data = pd.read_sql(query('survey_result'), self.db_engine)

        logging.info("Data fetched successfully.")
        return schedule_data, activity_data,  patient_journey_data, survey_result_data

    def transform(self, schedule_data, activity_data, patient_journey_data, survey_result_data):
        """
        Transform the data to populate the patient_journey_schedule_window table.
        """
        logging.info("Starting data transformation...")

        # Merge data sources
        merged_data = patient_journey_data[['id','patient_id']].merge(activity_data, on='id', suffixes=("_patient_journey", "_activity")) # COLS: id, patient_id, content_slug, schedule_id
        merged_data = merged_data.merge(schedule_data, on='id', suffixes=("", "_schedule")) # COLS: slug (schedule)
        merged_data = merged_data.merge(survey_result_data[['id', 'activity_id', 'patient_journey_id']], on='id', suffixes=("", "survey_result")) # COLS: 'activity_id', 'patient_journey_id'

        # Extract schedule details
        transformed_data = []
        for _, row in merged_data.iterrows():
            start_offset, end_offset, milestone_slug = ScheduleParser.parse_schedule_slug(row['slug'])
            # if start_offset is not None:
            transformed_data.append({
                'patient_id': row['patient_id'],
                'patient_journey_id': row['patient_journey_id'],
                'activity_id': row['activity_id'],
                'activity_content_slug': row['content_slug'],
                'schedule_id': row['schedule_id'],
                'schedule_slug': row['slug'],
                'schedule_start_offset_days': start_offset,
                'schedule_end_offset_days': end_offset,
                'schedule_milestone_slug': milestone_slug
            })

        logging.info("Transformation complete.")
        return pd.DataFrame(transformed_data)

    def load(self, transformed_data):
        """
        Load the transformed data into the target database table.
        """
        logging.info("Loading data into patient_journey_schedule_window...")
        # TODO: might not want this to replace
        transformed_data.to_sql('patient_journey_schedule_window', self.db_engine, if_exists='replace', index=False)
        logging.info("Data loaded successfully.")

