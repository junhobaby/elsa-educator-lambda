import os
import boto3
import arrow
import datetime
import json
from sqlalchemy import create_engine, text
from loguru import logger


# ENVIRONMENT VARIABLES - AWS CREDENTIALS
ACCESS_KEY = os.environ.get('ACCESS_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')
REGION_NAME = os.environ.get('REGION_NAME')

# ENVIRONMENT VARIABLES - RDS(Postgres) CREDENTIALS
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_USER = os.environ.get('DB_USER')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT', 5432)  # sets default value to 5432
DB_NAME = os.environ.get('DB_NAME', 'postgres')  # sets default value to `postgres`


def extract_rows(query_date: datetime) -> list:
    """
    Extract raw data from Dynamo DB
    """

    # generate dynamo db resource
    dynamodb = boto3.resource(region_name=REGION_NAME,
                              service_name='dynamodb',
                              aws_access_key_id=ACCESS_KEY,
                              aws_secret_access_key=SECRET_KEY)

    # create connection to DynamoDB Table
    registration_table = dynamodb.Table('elsa-registrations-resources-prod-registrations-2019')

    # extract data from DynamoDB Table
    # TODO: filter records from lastUpdatedAt yesterday
    response = registration_table.scan()
    educators_raw = response['Items']
    return educators_raw


def load_rows(rows: list):
    """
    Loads the rows into RDS
    """

    # create SQLAlchemy engine
    engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    # count rows inserted in table
    rows_inserted_count = 0

    # create RDS connection
    with engine.connect() as conn:

        # iterate through items in list
        for row in rows:

            # create SQL query
            # O'Brian -> O''Brian
            sql_query = f"""insert into raw_educator_history(raw_data) values('{json.dumps(row).replace("'", "''")}')"""

            # execute SQL query
            conn.execute(text(sql_query))
            rows_inserted_count += 1
            logger.success(f'Inserted row successfully! Row count: {rows_inserted_count}')

    return rows_inserted_count


def lambda_handler(event, context):
    """
    Extract data from DynamoDB and save updated rows into RDS
    """

    # date to filter from DynamoDB
    execution_date_event = event.get('executionDate', datetime.datetime.now())  # defaults to today's date
    query_date = arrow.get(execution_date_event).date()
    rows_to_insert = extract_rows(query_date)  # extracts rows from Dynamo DB
    n_rows = load_rows(rows_to_insert)  # loads rows to RDS
    print({
        'executionDate': f'{query_date}',
        'message': f'rows inserted: {n_rows:,}'
    })
    return {
        'executionDate': f'{query_date}',
        'message': f'rows inserted: {n_rows:,},',
        'debug': f'{event.keys()}'
    }
