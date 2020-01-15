import json
import ast
import time
import datetime

from src.processes.get_radiation import get_radiation_data
from src.processes.preprocess import combine_hourly_radiation
from src.processes.estimate import run_estimation
from src.processes.write_to_s3 import write_to_s3


def lambda_handler(event, context):
    # print(event['query_id'])
    # received_data = {'data': 'first test'}
    # received_data = ast.literal_eval(event['body'])

    start_date = datetime.datetime(2018, 7, 1)
    end_date = datetime.datetime(2019, 7, 1)
    lat = -10.4280875
    lng = 113.5748624
    df_dni, df_ghi = get_radiation_data(lat, lng, start_date, end_date)
    complete_df = combine_hourly_radiation(df_dni, df_ghi)
    result_df = run_estimation(complete_df, 0.005, 'Tracking')
    write_to_s3(result_df, 'colin-query-test', '10', 'abc-test@gmail.com', '123cwwc-2vvee2')

    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


if __name__ == "__main__":
    lambda_handler(1, 2)


