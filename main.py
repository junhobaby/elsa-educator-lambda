from lambda_function import lambda_handler
from datetime import datetime

if __name__ == '__main__':
    date_today = datetime.now()
    lambda_handler(
        event={'executionDate': date_today.date()}
    )
