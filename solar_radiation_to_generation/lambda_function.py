import json
# import ast
import time
import math
import datetime
import pandas as pd

from solar_radiation_to_generation.processes.get_radiation import get_radiation_data
from solar_radiation_to_generation.processes.preprocess import combine_hourly_radiation
from solar_radiation_to_generation.processes.estimate import run_estimation
from solar_radiation_to_generation.processes.write_to_s3 import write_to_s3
from solar_radiation_to_generation.processes.postprocess import scale_for_capacity
from solar_radiation_to_generation.processes.group_data import group_data


def get_half_hourly_data(df):
    """

    :param df:
    :type df:
    """
    half_hour = df[['TimeStamp', 'DNI', 'GHI']].copy()
    half_hour['TimeStamp'] = \
        half_hour['TimeStamp'].map(lambda x: x + datetime.timedelta(hours=0.5))
    for i in range(0, len(half_hour) - 1):
        half_hour.iloc[i, 1] = round(
            (half_hour.iloc[i]['DNI'] + half_hour.iloc[i + 1]['DNI']) / 2, 2)
        half_hour.iloc[i, 2] = round(
            (half_hour.iloc[i]['GHI'] + half_hour.iloc[i + 1]['GHI']) / 2, 2)
    # print(half_hour)
    # half_hour['hour_diff'] = half_hour['TimeStamp'].map(lambda x: abs(x.hour - 12))
    return half_hour


def lambda_handler(event, context):
    # received_data = {'data': 'first test'}
    # received_data = ast.literal_eval(event['body'])

    # print(event['query_id'])

    # time1 = time.time()
    # start_date = datetime.datetime.strptime(event['start_date'], '%Y-%m-%d')
    # end_date = datetime.datetime.strptime(event['end_date'], '%Y-%m-%d')
    # estimation_start_date = start_date if (end_date - start_date).days <= 365 * 3 else end_date - datetime.timedelta(
    #     days=365 * 3)

    # start_date = datetime.datetime.strptime('2017-01-01', '%Y-%m-%d')
    # end_date = datetime.datetime.strptime('2019-07-31', '%Y-%m-%d')
    # name = 'dingo'
    # lat = -28.588743
    # lng = 153.494055
    # print('Starting No.' + context + ': ' + name)
    # df_dni, df_ghi = get_radiation_data(lat, lng, start_date, end_date)
    # complete_df = combine_hourly_radiation(df_dni, df_ghi)
    # complete_df.to_csv('complte.csv')

    # name = event['name']
    # print('Starting No.' + context + ': ' + name)
    # hourly_df = pd.read_csv('sites/{}.csv'.format(name), parse_dates=['TimeStamp'])
    # hourly_df = pd.read_csv('complte.csv', parse_dates=['TimeStamp'])
    # hh_df = get_half_hourly_data(hourly_df)
    # half_hourly_df = hourly_df.merge(hh_df, how='outer', on=['TimeStamp'], suffixes=('','_y'))
    # half_hourly_df = half_hourly_df.sort_values(by=['TimeStamp'])
    # half_hourly_df['DNI'] = half_hourly_df.apply(lambda x: x['DNI'] if not math.isnan(x['DNI']) else x['DNI_y'], axis=1)
    # half_hourly_df['GHI'] = half_hourly_df.apply(lambda x: x['GHI'] if not math.isnan(x['GHI']) else x['GHI_y'], axis=1)
    # half_hourly_df = half_hourly_df.drop(['DNI_y', 'GHI_y'], axis=1)
    # # half_hourly_df.to_csv('halfhourly/' + name + '.csv', index=False)
    # # print('Ending No.' + context + ': ' + name)
    # half_hourly_df.to_csv('halfhourly/dingo.csv', index=False)

    # complete_df.to_csv('sites/' + name + '.csv', index=False)
    # print('Ending No.' + context + ': ' + name)

    # generation = True if event['generation'] == 1 else False

    # df_dni, df_ghi = get_radiation_data(lat, lng, start_date, end_date)
    # complete_df = combine_hourly_radiation(df_dni, df_ghi)

    # complete_df.to_csv('df.csv', index=False)
    # complete_df = pd.read_csv('df.csv', parse_dates=['TimeStamp'])
    # df_for_estimation = complete_df[complete_df['TimeStamp'] > estimation_start_date].copy()
    # df_for_estimation = pd.read_csv('df_for_estimation.csv', parse_dates=['TimeStamp'])
    # result_df = run_estimation(complete_df, event['capacity'][0], 'Tracking', generation, estimation_start_date)

    # result_df = result_df.drop(columns=['DNI', 'GHI'])
    # result_df = complete_df.merge(result_df, how='left', on=['TimeStamp'])
    # grouped_df = group_data(result_df, event['resolution'], generation, estimation_start_date)
    # if generation:
    #     grouped_df = scale_for_capacity(grouped_df, event['capacity'], event['capacity_unit'])
    #
    # grouped_df.to_csv('group.csv', index=False)
    # write_to_s3(grouped_df, event['bucket'], event['team_id'], event['email'], event['query_id'], event['resolution'])
    # print(time.time() - time1)
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('success')
    # }


if __name__ == "__main__":
    # lambda_handler({'query_id': 'fe3fcb94-bbda-4e97-ac4d-e5d6fcb2bb05',
    #                 'start_date': '2015-7-1',
    #                 'end_date': '2019-7-1',
    #                 'lat': -37.8255667,
    #                 'lng': 144.9719736,
    #                 'bucket': 'colin-query-test',
    #                 'team_id': '10',
    #                 'email': 'abc-test@gmail.com',
    #                 'resolution': 'weekly',
    #                 'generation': 1,
    #                 'capacity': [1, 2],
    #                 'capacity_unit': 'KWh'}, 2)

    # sites = [
    #     {'name': 'Florence Street', 'lat': -34.950559, 'lng': 138.6315199},
    #     {'name': 'Linsell Lodge Hostel', 'lat': -34.854044, 'lng': 138.5601117},
    #     {'name': 'Gawler Corps', 'lat': -34.5938013, 'lng': 138.7506146},
    #     {'name': 'Bethesda Aged Care Plus', 'lat': -23.3765787, 'lng': 150.496089},
    #     {'name': 'Nambour Family Stores', 'lat': -26.6290245, 'lng': 152.9596877},
    #     {'name': 'Shaftesbury Court Retirement Village', 'lat': -33.8771099,
    #      'lng': 151.1071456},
    #     {'name': 'Northside Hornsby Gateway Corps', 'lat': -33.702806, 'lng': 151.104954},
    #     {'name': 'Eastlake Corps', 'lat': -33.0219905, 'lng': 151.6655771},
    #     {'name': 'Elizabeth Jenkins Place Aged Care Plus', 'lat': -33.7344586,
    #      'lng': 151.2998209},
    #     {'name': 'Newcastle Worship and Community Centre', 'lat': -32.9204889,
    #      'lng': 151.7452094},
    #     {'name': 'Wollongong Corps', 'lat': -34.4260521, 'lng': 150.8909451},
    #     {'name': 'Family Services Management', 'lat': -37.757305, 'lng': 144.964003},
    #     {'name': 'James Barker House', 'lat': -37.803934, 'lng': 144.8998859},
    #     {'name': 'Abbotsford Salvos Stores', 'lat': -37.805749, 'lng': 144.998192},
    #     {'name': 'Mundy Street', 'lat': -36.7636383, 'lng': 144.2920565},
    #     {'name': 'SalvoConnect', 'lat': -38.1549769, 'lng': 144.3717657},
    #     {'name': 'Victoria Division - Ballarat Office', 'lat': -37.5671694,
    #      'lng': 143.869179334406},
    #     {'name': 'Eva Burrows College', 'lat': -37.8294426, 'lng': 145.2246585},
    #     {'name': 'Ringwood Corps', 'lat': -37.8327474, 'lng': 145.2272779},
    #     {'name': 'Moyne Aged Care', 'lat': -33.170972, 'lng': 148.210149},
    #     {'name': 'Shared Services Group', 'lat': -33.8471781, 'lng': 151.0309248},
    #     {'name': 'Mary Street', 'lat': -33.8812971, 'lng': 151.2101542},
    #     {'name': 'Stafford Corps', 'lat': -27.41511905, 'lng': 153.0127254},
    #     {'name': 'Life Community Church', 'lat': -27.6392755, 'lng': 153.1370244},
    #     {'name': 'Towards Independence', 'lat': -34.9302731, 'lng': 138.5940005},
    # ]
    #
    # i = 0
    # for site in sites:
    #     i += 1
    #     lambda_handler(site, str(i))

    lambda_handler({}, '1')
