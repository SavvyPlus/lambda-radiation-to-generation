import io
import boto3
import pandas as pd
from math import fabs, radians, cos, sin, asin, sqrt


def get_radiation_data(lat, lng, start_date, end_date):
    """
    Get the both dni and ghi solar data frames.
    Note that the longest period cannot be longer than 3 years.

    :param lat: The latitude of searched location
    :type lat: float
    :param lng: The longitude of searched location
    :type lng: float
    :param start_date: The start date of radiation data
    :type start_date: Datetime
    :param end_date: The end date of radiation data
    :type end_date: Datetime
    """
    lat_list679 = [float(x) for x in load_data('latitude.csv', 'latitude')]
    lng_list839 = [float(x) for x in load_data('longitude.csv', 'longitude')]
    nearest = get_closest_point((lat, lng), lat_list679, lng_list839)

    result = get_radiation(nearest, 'dni', start_date, end_date)
    df_dni = pd.DataFrame(result, columns=['TimeStamp', 'radiation'])
    df_dni['TimeStamp'] = pd.to_datetime(df_dni['TimeStamp'], format="%Y-%m-%d %H:%M")

    result = get_radiation(nearest, 'ghi', start_date, end_date)
    df_ghi = pd.DataFrame(result, columns=['TimeStamp', 'radiation'])
    df_ghi['TimeStamp'] = pd.to_datetime(df_ghi['TimeStamp'], format="%Y-%m-%d %H:%M")

    return df_dni, df_ghi


def get_radiation(coordinates, r_type, start_date, end_date):
    """
    Query athena and get the solar data in a specific period.

    :param coordinates: closest solar station coordiates
    :type coordinates: Tuple[float, float]
    :param r_type: radiation type
    :type r_type: str
    :param start_date: start date of the query
    :type start_date: Datetime
    :param end_date: end date of the query
    :type end_date: Datetime
    """
    lat = coordinates[0]
    lng = coordinates[1]
    print(lat)
    print(lng)
    start_year = start_date.year
    end_year = end_date.year
    client = boto3.client('athena', region_name='ap-southeast-2')
    query_string = f'''SELECT date, radiation
                        FROM solar_radiation_hill.lat_partition_v2
                        WHERE (latitude = '{str(format(lat, '.7f'))}'
                        AND longitude = '{str(format(lng, '.7f'))}' )
                        AND cast(year as integer) >= {start_year}
                        AND cast(year as integer) <= {end_year}
                        AND radiationtype='{r_type}'
                        ORDER BY date'''
    # print(query_string)
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': 'solar_data'
        },
        ResultConfiguration={
            'OutputLocation': 's3://solar-radiation/solar_test'
        }
    )['QueryExecutionId']
    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        # print(query_status)
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))

    print('finished query')
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )
    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])
    for datum in data_list[1:]:
        results.append([x['VarCharValue'] for x in datum])
    return results


def get_closest_point(coordinate_lat_lng, lat_list, lng_list):
    """
    Find the closest Solar point of the input address in the radius of 4km

    :param coordinate_lat_lng: the searched location coordinate
    :type coordinate_lat_lng: Tuple(float, float)
    :param lat_list: list of latitudes of solar stations
    :type lat_list: List
    :param lng_list: list of longitudes of solar stations
    :type lng_list: List
    """
    r = 4
    point = []  # [(lat, lng, distance),(...),...]

    threshold_lat = r / 100  # 110km/degree
    threshold_lng = r / 75  # 80-105km/degree for AU

    for lng in lng_list:
        if fabs(coordinate_lat_lng[1] - lng) < threshold_lng:
            for lat in lat_list:
                if fabs(coordinate_lat_lng[0] - lat) < threshold_lat:
                    dist = haversine(coordinate_lat_lng[0], coordinate_lat_lng[1], lat, lng)
                    if dist < r:
                        point.append((lat, lng, dist))

    point.sort(key=lambda x: x[2])
    nearest_lat = point[0][0]
    nearest_lng = point[0][1]
    # print([(point[0][0], point[0][1])])
    return nearest_lat, nearest_lng


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    :param lat1: The start date of radiation data
    :type lat1: float
    :param lon1: The end date of radiation data
    :type lon1: float
    :param lat2: The start date of radiation data
    :type lat2: float
    :param lon2: The end date of radiation data
    :type lon2: float
    """
    # convert dec to rad
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # Haversine formula
    d_lon = lon2 - lon1
    d_lat = lat2 - lat1
    a = sin(d_lat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(d_lon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Average Earth radius /km
    return c * r  # * 1000


def load_data(filename, header):
    """
    Load csv file and return a data frame header

    :param filename: the csv file path
    :type filename: str
    :param header: the data frame header
    :type header: str
    """
    client = boto3.client('s3')
    file_obj = client.get_object(
        Bucket='solar-radiation',
        Key='solar-radiation-to-generation/' + filename
    )
    df = pd.read_csv(io.BytesIO(file_obj['Body'].read()), header=0, float_precision='round_trip')
    return df[header]

