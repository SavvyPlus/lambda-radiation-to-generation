import json
# import ast
import time
import datetime
import pandas as pd

from solar_radiation_to_generation.processes.get_radiation import get_radiation_data
from solar_radiation_to_generation.processes.preprocess import combine_hourly_radiation
from solar_radiation_to_generation.processes.estimate import run_estimation
from solar_radiation_to_generation.processes.write_to_s3 import write_to_s3
from solar_radiation_to_generation.processes.postprocess import scale_for_capacity
from solar_radiation_to_generation.processes.group_data import group_data


def lambda_handler(event, context):
    # received_data = {'data': 'first test'}
    # received_data = ast.literal_eval(event['body'])

    # print(event['query_id'])

    time1 = time.time()
    start_date = datetime.datetime.strptime(event['start_date'], '%Y-%m-%d')
    end_date = datetime.datetime.strptime(event['end_date'], '%Y-%m-%d')
    estimation_start_date = start_date if (end_date - start_date).days <= 365 * 3 else end_date - datetime.timedelta(
        days=365 * 3)

    lat = event['lat']
    lng = event['lng']

    generation = True if event['generation'] == 1 else False

    df_dni, df_ghi = get_radiation_data(lat, lng, start_date, end_date)
    # df_dni = pd.read_csv('dni.csv', parse_dates=['TimeStamp'])
    # df_ghi = pd.read_csv('ghi.csv', parse_dates=['TimeStamp'])
    complete_df = combine_hourly_radiation(df_dni, df_ghi)

    # complete_df.to_csv('df.csv', index=False)
    # complete_df = pd.read_csv('dni.csv', parse_dates=['TimeStamp'])
    # df_for_estimation = complete_df[complete_df['TimeStamp'] > estimation_start_date].copy()
    # df_for_estimation = pd.read_csv('df_for_estimation.csv', parse_dates=['TimeStamp'])
    result_df = run_estimation(complete_df, event['capacity'][0], 'Non-tracking', generation, estimation_start_date)

    # result_df = result_df.drop(columns=['DNI', 'GHI'])
    # result_df = complete_df.merge(result_df, how='left', on=['TimeStamp'])
    grouped_df = group_data(result_df, event['resolution'], generation)
    if generation:
        grouped_df = scale_for_capacity(grouped_df, event['capacity'], event['capacity_unit'])

    grouped_df.to_csv('group.csv', index=False)
    # write_to_s3(grouped_df, event['bucket'], event['team_id'], event['email'], event['query_id'], event['resolution'])
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
                    'resolution': 'weekly',
                    'generation': 1,
                    'capacity': [5, 10],
                    'capacity_unit': 'KWh'}, 2)


