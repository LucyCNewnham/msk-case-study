# msk-case-study
Code test for interview with MSK.ai

## What the code does

## How to use
1. **Install requirements**

Pip install requirements.txt, and requirements-test.txt if you plan on using the unit tests.

2. **Edit the DATABASE_URL in the .env.** 

The code will not work unless you have access to the dataset. The db_url is correct for my local version of the database.

2. **Run main.py using the following command:** 

This will trigger the code to run now, and also set off the repeated running every hour. 

If you want to test whether this will run again on schedule, without waiting an hour, please edit line (TODO: add this in) and change it to be 1 minute. 
