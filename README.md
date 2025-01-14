# msk-case-study
Code test for interview with MSK.ai

## What the code does

### Scheduling
The requirements requested that the code be able to be run on a schedule. There are several ways to do this, and my personal preference based on previous experience would be to create this tool as a FastAPI App, with some subscribers listening to events in the DB. This is not feasible for this task, so I have used the Python package `schedule` and `time.sleep` to prevent busy waiting.



## How to use
1. **Install requirements**

Pip install requirements.txt, and requirements_test.txt if you plan on using the unit tests.

2. **Edit the DATABASE_URL in the .env.** 

The code will not work unless you have access to the dataset. The db_url is correct for my local version of the database.

3. **Run main.py** 

This will trigger the code to run now, and also set off the repeated running every hour. 

If you want to test whether this will run again on schedule, without waiting an hour, please edit line 45 in main.py 

from: 
`schedule.every(1).hour.do(run_pipeline, db_url=db_url)`

to: 
`schedule.every(1).minute.do(run_pipeline, db_url=db_url)`

This changes the schedule to be 1 minute. 
