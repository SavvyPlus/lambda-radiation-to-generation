import pandas as pd
import datetime
import math

from solar_radiation_to_generation.processes.TrackingModel import tracking
from solar_radiation_to_generation.processes.NonTrackingModel import non_tracking as nontracking

TRACKING_CAPACITY = 56
NON_TRACKING_CAPACITY = 102


def run_estimation(radiation_df, capacity, r_type):
    """
    Run the solar data estimation based on the input radiation data

    :param radiation_df: solar radiation dataframe
    :type radiation_df: pd.DataFrame
    :param capacity: the searched location
    :type capacity: float
    :param r_type: tracking or non-tracking
    :type r_type: str
    """
    maximum_cap = capacity
    tracking_type = r_type

    scalar = maximum_cap / TRACKING_CAPACITY if tracking_type == 'Tracking' else maximum_cap / NON_TRACKING_CAPACITY
    # load irradiance csv
    dt = load_irrad_csv(radiation_df)
    # print(len(dt), 66457)    # 66457
    # fill missing data
    dt = dt.reset_index()
    start_yr = dt.iloc[0]['year']

    end_yr = dt.iloc[len(dt)-1]['year']
    # print(start_yr, end_yr)
    calc_missing(dt, 'GHI')
    calc_missing(dt, 'DNI')
    model = tracking if tracking_type == 'Tracking' else nontracking
    # load prediction csv
    ghi_predict = model.load_predict_csv('GHI')
    dni_predict = model.load_predict_csv('DNI')

    # compute prediction based on each row
    compute_predict(dt, ghi_predict, 'GHI')
    compute_predict(dt, dni_predict, 'DNI')
    # print(len(dt), 66457)  # 66457

    half_hour = dt
    # filter the time with no sunshine
    # dt = dt[~dt.GHI_filled.isin([0]) | ~dt.DNI_filled.isin([0])]
    # print(len(dt), 31503)    # 31503

    # compute the time difference in days between each day to the day with longest daylight in that year
    dt = dt.drop(['GHI', 'DNI'], axis=1)
    dt = dt.dropna()   # 31177
    get_time_diff(dt, start_yr, end_yr)
    # print(dt)

    # Remove for 0
    dt['Act_GHI_pred'] = dt.apply(lambda x: 0 if x['GHI_filled'] == 0 and x['DNI_filled'] == 0 else x['GHI_filled'],
                                  axis=1)
    dt['Act_DNI_pred'] = dt.apply(lambda x: 0 if x['GHI_filled'] == 0 and x['DNI_filled'] == 0 else x['DNI_filled'],
                                  axis=1)


    # calculate the lags and leads for the Actual_hh_predicted
    dt['Act_GHI_pred_lag'] = dt['Act_GHI_pred'].shift(1)
    # print(dt['Act_GHI_pred_lag'])
    dt['Act_GHI_pred_lead'] = dt['Act_GHI_pred'].shift(-1)
    # print(dt['Act_GHI_pred_lead'])
    dt['Act_DNI_pred_lag'] = dt['Act_DNI_pred'].shift(1)
    # print(dt['Act_DNI_pred_lag'])
    dt['Act_DNI_pred_lead'] = dt['Act_DNI_pred'].shift(-1)
    # print(dt['Act_DNI_pred_lead'])

    # print(len(dt), 31177)
    # remove first line
    dt = dt.iloc[1:]
    # print(len(dt), 31175)  # 31175

    #  get the winter month
    dt_winter = model.calc_tracking_winter(dt, scalar, half_hour=False)
    dt_winter['prediction_final'] = dt_winter.apply(lambda x: 0 if x['GHI_filled']==0 and x['DNI_filled']==0
                                                    else x['prediction_final'], axis=1)
    # print(dt_winter)

    dt_summer = model.calc_tracking_summer(dt, scalar, half_hour=False)
    dt_summer['prediction_final'] = dt_summer.apply(lambda x: 0 if x['GHI_filled']==0 and x['DNI_filled']==0
                                                    else x['prediction_final'], axis=1)
    # print(dt_summer)

    dt_all = pd.concat([dt_winter, dt_summer])
    # print(dt_all)

    # compute half hourly data
    hh = half_hourly_data(half_hour)
    compute_predict(hh, ghi_predict, 'GHI')
    compute_predict(hh, dni_predict, 'DNI')
    # print(hh)

    # hh = hh[hh['GHI_filled'] != 0]
    # hh = hh[hh['DNI_filled'] != 0]
    # hh = hh.dropna()
    get_time_diff(hh, start_yr, end_yr)

    hh['Act_GHI_pred'] = hh.apply(lambda x: 0 if x['GHI_filled'] == 0 and x['DNI_filled'] == 0 else x['GHI_filled'],
                                  axis=1)
    hh['Act_DNI_pred'] = hh.apply(lambda x: 0 if x['GHI_filled'] == 0 and x['DNI_filled'] == 0 else x['DNI_filled'],
                                  axis=1)
    # calculate the lags and leads for the Actual_hh_predicted
    hh['Act_GHI_pred_lag'] = hh['Act_GHI_pred'].shift(1)
    # print(dt['Act_GHI_pred_lag'])
    hh['Act_GHI_pred_lead'] = hh['Act_GHI_pred'].shift(-1)
    # print(dt['Act_GHI_pred_lead'])
    hh['Act_DNI_pred_lag'] = hh['Act_DNI_pred'].shift(1)
    # print(dt['Act_DNI_pred_lag'])
    hh['Act_DNI_pred_lead'] = hh['Act_DNI_pred'].shift(-1)
    # print(dt['Act_DNI_pred_lead'])
    hh = hh.iloc[1:]

    hh_winter = model.calc_tracking_winter(hh, scalar, half_hour=True)
    hh_winter['prediction_final'] = hh_winter.apply(lambda x: 0 if x['GHI_filled']==0 and x['DNI_filled']==0
                                                    else x['prediction_final'], axis=1)
    hh_summer = model.calc_tracking_summer(hh, scalar, half_hour=True)
    hh_winter['prediction_final'] = hh_winter.apply(lambda x: 0 if x['GHI_filled']==0 and x['DNI_filled']==0
                                                    else x['prediction_final'], axis=1)
    dt_hh = pd.concat([hh_winter, hh_summer])
    # print(dt_hh)

    dt = (pd.concat([dt_all, dt_hh], sort=False))
    dt = dt[['TimeStamp', 'DNI_filled', 'GHI_filled', 'predictions_final']].copy()
    dt['predictions_final'] = dt['predictions_final'] * 1000
    dt = dt.rename(columns={'DNI_filled': 'DNI', 'GHI_filled': 'GHI', 'predictions_final': 'Estimate generation(kW)'})
    dt = dt.sort_values(by=['TimeStamp'])
    # # print(dt)
    # start_time = (dt.iloc[0])['TimeStamp'].date()
    # # print(start_time)
    # end_time = ((dt.iloc[len(dt)-1])['TimeStamp'] + pd.Timedelta(days=1)).date()
    # # print(end_time)
    # dt_datetime_series = pd.date_range(start=start_time, end=end_time, freq='30min')
    # # print(dt_datetime_series)
    # dt_final = pd.DataFrame(dt_datetime_series, columns=['TimeStamp'], index=dt_datetime_series)
    # dt.reset_index(drop=True, inplace=True)
    # dt_final = dt_final.merge(dt, how='outer', on=['TimeStamp'])
    dt_final = dt.fillna(value=0)
    # print(dt_final)
    return dt_final


def load_irrad_csv(radiation_df):
    """

    :param radiation_df: solar radiation dataframe
    :type radiation_df: pd.DataFrame
    """
    dataframe = radiation_df
    # dataframe['TimeStamp'] = pd.to_datetime(dataframe['TimeStamp'], format="%d/%m/%Y %H:%M")
    # print(type(dataframe['TimeStamp']))
    dataframe['month'] = dataframe['TimeStamp'].dt.month
    dataframe['year'] = dataframe['TimeStamp'].dt.year
    dataframe['day'] = dataframe['TimeStamp'].dt.day
    dataframe['hour_diff'] = dataframe['TimeStamp'].map(lambda x: abs(x.hour - 12))
    dataframe['GHI'] = dataframe.apply(lambda x: 0 if not x['GHI'] and x['hour_diff'] >= 7
                                       else x['GHI'], axis=1)
    dataframe['DNI'] = dataframe.apply(lambda x: 0 if not x['DNI'] and x['hour_diff'] >= 7
                                       else x['DNI'], axis=1)
    return dataframe


def calc_missing(df, r_type):
    """

    :param df:
    :type df: pd.DataFrame
    :param r_type:
    :type r_type:
    """
    column_name = r_type + '_filled'
    # df.apply(lambda x: print(x) if math.isnan(x[r_type]) else 1 + 1, axis=1)
    df[r_type] = pd.to_numeric(df[r_type], errors='coerce')
    df[r_type] = df.apply(lambda x: 0 if math.isnan(x[r_type]) and x['hour_diff'] >=7 else x[r_type], axis=1)
    df[column_name] = pd.Series([None]*len(df[r_type]), index=df.index)
    df[column_name] = df[r_type].map(lambda x: x if (x >= 0 or not math.isnan(x)) else None)
    # df.apply(lambda x: print(x) if math.isnan(x[column_name]) else 1+1, axis=1)
    for i in df.index:
        if math.isnan(df.loc[i, column_name]) or df.loc[i, column_name] == -999:
            # if i == 0:
            #     df.loc[i, column_name] = 0
            #     continue
            start = df.loc[i-1, column_name]
            end = 0
            avg = 0
            count = 0
            for j in range(i + 1, len(df.index)):
                if (not math.isnan(df.loc[j, column_name])) and (df.loc[j, column_name] != -999):
                    end = df.loc[j, column_name]
                    count = j - i + 1
                    avg = (end - start) / count
                    break
            for k in range(1, count):
                df.loc[i+k-1, column_name] = round(start + k*avg, 0)


def compute_predict(df, predict, r_type):
    """

    :param df:
    :type df: pd.DataFrame
    :param predict:
    :type predict:
    :param r_type:
    :type r_type:
    """
    column_name = 'Act_' + r_type + '_pred'
    df[column_name] = df[r_type + '_filled'].map(lambda x: from_rad_to_gen(x, predict))


def from_rad_to_gen(rad, predict):
    """

    :param rad:
    :type rad:
    :param predict:
    :type predict:
    """
    # TODO hard coded limit of prediction upper bound Need to change in future
    if 0 <= rad < 1117:
        return predict.Predictions[rad]
    elif rad >= 1117:
        return predict.Predictions[1117]
    else:
        return None


def get_time_diff(df, start_yr, end_yr):
    """

    :param df:
    :type df:
    :param start_yr:
    :type start_yr:
    :param end_yr:
    :type end_yr:
    """
    mid_yr_list = []
    for i in range(start_yr, end_yr+1):
        mid_yr_list.append(datetime.datetime(i, 6, 21))
    # print(mid_yr_list)
    df['time_diff'] = df['TimeStamp'].map(lambda x: find_min_day_diff(x, mid_yr_list))


def find_min_day_diff(datetime1, datetime_list):
    """

    :param datetime1:
    :type datetime1:
    :param datetime_list:
    :type datetime_list:
    """
    return min(abs((datetime1 - date).days + (datetime1 - date).seconds/86400) for date in datetime_list)


def half_hourly_data(df):
    """
    
    :param df:
    :type df:
    """
    half_hour = df[['TimeStamp', 'month', 'day', 'year', 'DNI_filled', 'GHI_filled']].copy()
    half_hour['TimeStamp'] = half_hour['TimeStamp'].map(lambda x: x + datetime.timedelta(hours=0.5))
    for i in range(0, len(half_hour)-1):
        half_hour.loc[i, 'DNI_filled'] = round((half_hour.loc[i, 'DNI_filled'] + half_hour.loc[i + 1, 'DNI_filled']) / 2, 0)
        half_hour.loc[i, 'GHI_filled'] = round((half_hour.loc[i, 'GHI_filled'] + half_hour.loc[i + 1, 'GHI_filled']) / 2, 0)
    # print(half_hour)
    half_hour['hour_diff'] = half_hour['TimeStamp'].map(lambda x: abs(x.hour - 12))
    return half_hour