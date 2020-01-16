import json
import ast
import time
import datetime
import pandas as pd

from solar_radiation_to_generation.processes.get_radiation import get_radiation_data
from solar_radiation_to_generation.processes.preprocess import combine_hourly_radiation
from solar_radiation_to_generation.processes.estimate import run_estimation
from solar_radiation_to_generation.processes.write_to_s3 import write_to_s3


def lambda_handler(event, context):
    # print(event['query_id'])
    # received_data = {'data': 'first test'}
    # received_data = ast.literal_eval(event['body'])
    time1 = time.time()
    start_date = datetime.datetime.strptime(event['start_date'], '%Y-%m-%d')
    end_date = datetime.datetime.strptime(event['end_date'], '%Y-%m-%d')
    estimation_start_date = start_date if (end_date - start_date).days <= 365 * 3 else end_date - datetime.timedelta(
        days=365 * 3)

    lat = event['lat']
    lng = event['lng']

    # df_dni, df_ghi = get_radiation_data(lat, lng, start_date, end_date)
    # complete_df = combine_hourly_radiation(df_dni, df_ghi)
    # complete_df.to_csv('df.csv',index=False)
    complete_df = pd.read_csv('df.csv', parse_dates=['TimeStamp'])

    df_for_estimation = complete_df[complete_df['TimeStamp'] > estimation_start_date].copy()
    result_df = run_estimation(df_for_estimation, 0.005, 'Tracking')

    result_df.update(complete_df, join='left')
    result_df = complete_df.merge(result_df, how='outer', on=['TimeStamp'])

    write_to_s3(result_df, event['bucket'], event['team_id'], event['email'], event['query_id'])
    print(time.time() - time1)
    return {
        'statusCode': 200,
        'body': json.dumps('success')
    }


if __name__ == "__main__":
    lambda_handler({'query_id': 'fe3fcb94-bbda-4e97-ac4d-e5d6fcb2bb05',
                    'start_date': '2015-7-1',
                    'end_date': '2019-7-1',
                    'lat': -37.8255667,
                    'lng': 144.9719736,
                    'bucket': 'colin-query-test',
                    'team_id': '10',
                    'email': 'abc-test@gmail.com',
                    }, 2)


