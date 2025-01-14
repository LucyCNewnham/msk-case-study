import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from io import StringIO
from application.patient_journey_pipeline import ScheduleParser, DataPipeline

@pytest.fixture
def sample_data():
    patient_data = pd.DataFrame({
        'patient_id': [1, 2],
        'patient_journey_id': [101, 102]
    })

    schedule_data = pd.DataFrame({
        'schedule_id': [201, 202],
        'schedule_slug': ['4d-2d-pre-op', '3m-1d-post-op']
    })

    activity_data = pd.DataFrame({
        'activity_id': [301, 302],
        'activity_content_slug': ['pain-survey', 'readiness-survey'],
        'schedule_id': [201, 202],
        'patient_id': [1, 2]
    })

    milestone_data = pd.DataFrame({
        'patient_journey_id': [101, 102],
        'milestone_date': [datetime(2023, 5, 10), datetime(2023, 6, 15)]
    })

    return patient_data, schedule_data, activity_data, milestone_data

@pytest.fixture
def db_engine():
    # This is a db for testing
    engine = create_engine("sqlite:///:memory:")
    return engine

def test_parse_schedule_slug_valid():
    # Test valid schedule_slug parsing
    slug = "4d-2d-pre-op"
    start_offset, end_offset, milestone = ScheduleParser.parse_schedule_slug(slug)
    assert start_offset == -4
    assert end_offset == -2
    assert milestone == "pre-op"

def test_parse_schedule_slug_not_valid():
    # Test invalid schedule_slug parsing
    slug = "invalid-slug"
    start_offset, end_offset, milestone = ScheduleParser.parse_schedule_slug(slug)
    assert start_offset is None
    assert end_offset is None
    assert milestone is None

def test_transform(sample_data, db_engine):
    patient_data, schedule_data, activity_data, milestone_data = sample_data

    pipeline = DataPipeline(patient_data, schedule_data, activity_data, milestone_data, db_engine)
    transformed_data = pipeline.transform()

    # Check the structure of the transformed data
    assert set(transformed_data.columns) == {
        'patient_id',
        'patient_journey_id',
        'activity_id',
        'activity_content_slug',
        'schedule_id',
        'schedule_slug',
        'schedule_start_offset_days',
        'schedule_end_offset_days',
        'schedule_milestone_slug',
        'milestone_date'
    }

    # Check the contents of the transformed data
    assert len(transformed_data) == 2
    assert transformed_data.iloc[0]['schedule_start_offset_days'] == -4
    assert transformed_data.iloc[0]['schedule_end_offset_days'] == -2
    assert transformed_data.iloc[0]['schedule_milestone_slug'] == "pre-op"

def test_load(sample_data, db_engine):
    patient_data, schedule_data, activity_data, milestone_data = sample_data

    pipeline = DataPipeline(patient_data, schedule_data, activity_data, milestone_data, db_engine)
    transformed_data = pipeline.transform()
    
    # Attempt to load the transformed data
    try:
        pipeline.load(transformed_data)
        # Verify data in the database
        with db_engine.connect() as conn:
            result = conn.execute("SELECT * FROM patient_journey_schedule_window")
            rows = result.fetchall()
            assert len(rows) == 2
    except OperationalError:
        pytest.fail("Database operation failed.")
