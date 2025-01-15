import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from application.patient_journey_pipeline import ScheduleParser, DataPipeline

@pytest.fixture
def sample_data():
    schedule_data = pd.DataFrame({
        'id': [1, 2],
        'slug': ['4d-2d-pre-op', '3m-1d-post-op']
    })

    activity_data = pd.DataFrame({
        'id': [1, 2],
        'content_slug': ['pain-survey', 'readiness-survey'],
        'schedule_id': [1, 2]
    })

    patient_journey_data = pd.DataFrame({
        'id': [1, 2],
        'patient_id': [101, 102],
        'invitation_date': [datetime(2023, 1, 1), datetime(2023, 1, 2)],
        'registration_date': [datetime(2023, 1, 3), datetime(2023, 1, 4)],
        'discharge_date': [datetime(2023, 2, 1), datetime(2023, 2, 2)],
        'consent_date': [datetime(2023, 1, 5), datetime(2023, 1, 6)],
        'operation_date': [datetime(2023, 1, 10), datetime(2023, 1, 11)]
    })

    survey_result_data = pd.DataFrame({
        'id': [1, 2],
        'activity_id': [301, 302],
        'patient_journey_id': [1, 2]
    })

    return schedule_data, activity_data, patient_journey_data, survey_result_data

@pytest.fixture
def db_engine():
    # SQLite in-memory database for testing
    return create_engine("sqlite:///:memory:")

def test_parse_schedule_slug_valid():
    slug = "4d-2d-pre-op"
    parser = ScheduleParser()
    start_offset, end_offset, milestone = parser.parse_schedule_slug(slug)
    assert start_offset == -4
    assert end_offset == -2
    assert milestone == 'pre-op'
    
def test_parse_schedule_slug_no_dates():
    slug = "slug"
    parser = ScheduleParser()
    start_offset, end_offset, milestone = parser.parse_schedule_slug(slug)
    assert start_offset is None
    assert end_offset is None
    assert milestone == 'slug'

def test_parse_schedule_slug_empty():
    slug = ""
    parser = ScheduleParser()
    start_offset, end_offset, milestone = parser.parse_schedule_slug(slug)
    assert start_offset is None
    assert end_offset is None
    assert milestone is None

def test_parse_schedule_slug_unable():
    slug = "reg-d0"
    parser = ScheduleParser()
    start_offset, end_offset, milestone = parser.parse_schedule_slug(slug)
    assert start_offset is None
    assert end_offset is None
    assert milestone is None

def test_format_milestone():
    milestone, date = ScheduleParser.format_milestone("pre-op", None, None, None, None, datetime(2023, 1, 10))
    assert milestone == "operation"
    assert date == datetime(2023, 1, 10)

def test_format_milestone_no_date():
    milestone, date = ScheduleParser.format_milestone("appt", None, None, None, None, None)
    assert milestone == "appointment"
    assert date is None

def test_format_milestone_unknown():
    milestone, date = ScheduleParser.format_milestone("fdsjghbsdfg", None, None, None, None, None)
    assert milestone == "fdsjghbsdfg"
    assert date is None

def test_transform_new_data_only(sample_data, db_engine):
    schedule_data, activity_data, patient_journey_data, survey_result_data = sample_data

    pipeline = DataPipeline(db_engine)

    # Simulate an existing table with schema but no matching data
    existing_data = pd.DataFrame({
        'patient_id': [101],
        'patient_journey_id': [1],
        'activity_id': [301],
        'schedule_id': [1]
    })
    # Insert into the database
    existing_data.to_sql('patient_journey_schedule_window', db_engine, index=False, if_exists='replace')

    # Ensure the table is correctly set up
    db_existing_data = pd.read_sql("SELECT * FROM patient_journey_schedule_window", db_engine)
    assert not db_existing_data.empty

    # Transform new data
    transformed_data = pipeline.transform(schedule_data, activity_data, patient_journey_data, survey_result_data)

    # Verify that only new data is returned
    assert len(transformed_data) == 1  # Only the second row should be included
    assert transformed_data.iloc[0]['patient_id'] == 102
    assert transformed_data.iloc[0]['activity_id'] == 302



def test_transform_no_new_data(sample_data, db_engine):
    schedule_data, activity_data, patient_journey_data, survey_result_data = sample_data

    pipeline = DataPipeline(db_engine)

    # Simulate all data already existing in the database
    existing_data = pd.DataFrame({
        'patient_id': [101, 102],
        'patient_journey_id': [1, 2],
        'activity_id': [301, 302],
        'schedule_id': [1, 2]
    })
    existing_data.to_sql('patient_journey_schedule_window', db_engine, index=False, if_exists='replace')

    transformed_data = pipeline.transform(schedule_data, activity_data, patient_journey_data, survey_result_data)

    # Ensure no new data to process
    assert transformed_data.empty

def test_load(sample_data, db_engine):
    schedule_data, activity_data, patient_journey_data, survey_result_data = sample_data

    pipeline = DataPipeline(db_engine)
    transformed_data = pipeline.transform(schedule_data, activity_data, patient_journey_data, survey_result_data)

    pipeline.load(transformed_data)

    # Verify data is loaded into the database
    loaded_data = pd.read_sql("SELECT * FROM patient_journey_schedule_window", db_engine)
    assert len(loaded_data) == len(transformed_data)
    assert set(loaded_data.columns) == set(transformed_data.columns)
