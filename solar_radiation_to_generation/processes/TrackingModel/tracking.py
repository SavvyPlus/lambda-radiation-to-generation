import io
import boto3
import pandas as pd


WINTER = [4, 5, 6, 7, 8, 9]
SUMMER = [1, 2, 3, 10, 11, 12]
TRACKING_CAPACITY = 56


class TrackingWinterHour:
    GHI_filled = 0.016
    DNI_filled = 0.032
    Act_DNI_pred = -0.6
    Act_GHI_pred = -0.22
    time_diff = 0.025
    hour_diff = 1.852
    Act_DNI_pred_lag = 0.387
    Act_DNI_pred_lead = 0.015
    Act_GHI_pred_lag = 0.659
    Act_GHI_pred_lead = -0.121


class TrackingWinterHalfHour:
    GHI_filled = 0.015
    DNI_filled = 0.041
    Act_DNI_pred = -0.746
    Act_GHI_pred = -0.050
    time_diff = 0.017
    hour_diff = 2.282
    Act_DNI_pred_lag = 0.218
    Act_DNI_pred_lead = 0.0023
    Act_GHI_pred_lag = 0.854
    Act_GHI_pred_lead = -0.300


class TrackingSummerHour:
    GHI_filled = -0.01055
    DNI_filled = 0.01722
    Act_DNI_pred = 0.17360
    Act_GHI_pred = 0.45057
    time_diff = 0.00095
    hour_diff = -1.08214
    Act_DNI_pred_lag = 0.09870
    Act_DNI_pred_lead = 0.27077
    Act_GHI_pred_lag = -0.11496
    Act_GHI_pred_lead = 0.01503


class TrackingSummerHalfHour:
    GHI_filled = -0.01798
    DNI_filled = 0.03344
    Act_DNI_pred = -0.12275
    Act_GHI_pred = 0.75242
    time_diff = 0.00032
    hour_diff = -0.97115
    Act_DNI_pred_lag = -0.02836
    Act_DNI_pred_lead = 0.20878
    Act_GHI_pred_lag = -0.08259
    Act_GHI_pred_lead = 0.01489


def calc_pred_generation(row, model_data):
    result = row['GHI_filled'] * model_data.GHI_filled\
             + row['DNI_filled'] * model_data.DNI_filled\
             + row['Act_DNI_pred'] * model_data.Act_DNI_pred\
             + row['Act_GHI_pred'] * model_data.Act_GHI_pred\
             + row['time_diff'] * model_data.time_diff\
             + row['hour_diff'] * model_data.hour_diff\
             + row['Act_DNI_pred_lag'] * model_data.Act_DNI_pred_lag\
             + row['Act_DNI_pred_lead'] * model_data.Act_DNI_pred_lead\
             + row['Act_GHI_pred_lag'] * model_data.Act_GHI_pred_lag\
             + row['Act_GHI_pred_lead'] * model_data.Act_GHI_pred_lead
    return result


def load_predict_csv(r_type):
    client = boto3.client('s3')
    if r_type == 'GHI':
        file_obj = client.get_object(
            Bucket='solar-radiation',
            Key='solar-radiation-to-generation/TrackingModel/predictions_GHI_5minv2.csv'
        )
    else:
        file_obj = client.get_object(
            Bucket='solar-radiation',
            Key='solar-radiation-to-generation/TrackingModel/predictions_DNI_5min_final.csv'
        )
    dataframe = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
    dataframe.rename(columns={'Irr_Num_seq': 'Irradiance'}, inplace=True)
    return dataframe


def calc_tracking_winter(df, scaler, half_hour):
    df_winter = df[df.month.isin(WINTER)]
    # print(len(df_winter), 13590)  # 13590
    df_winter = df_winter.fillna(0)

    max_generation = TRACKING_CAPACITY * scaler
    model = TrackingWinterHalfHour() if half_hour else TrackingWinterHour()

    # print(df_winter)
    df_winter['predictions'] = df_winter.apply(lambda x: calc_pred_generation(x, model), axis=1)

    # print(df_winter)
    # df_winter['predictions'] = df_winter['predictions'].apply(lambda x: round(x, 2))
    # print(df_winter)

    # apply smoothing function
    df_winter['predictions_final'] = df_winter['predictions'].map(
        lambda x: -0.0003 * x * x + x * 0.6444 - 0.296 if x < 15 else x)
    df_winter['predictions_final'] = df_winter['predictions_final'].map(lambda x: 0 if x < 0 else x)

    # scale the output
    df_winter['predictions_final'] = df_winter['predictions_final'].map(lambda x: x * scaler)

    df_winter['predictions_final'] = df_winter['predictions_final'].map(lambda x: max_generation
                                                                        if x > max_generation else x)
    # df_winter['predictions_final'] = df_winter.apply(lambda x: 0 if x['hour_diff'] > 5.5 else x['predictions_final'],
    #                                                  axis=1)
    df_winter = df_winter.dropna()
    return df_winter


def calc_tracking_summer(df, scalar, half_hour):
    df_summer = df[df.month.isin(SUMMER)]
    # print(len(df_summer), 13590)  # 13590
    df_summer = df_summer.fillna(0)
    max_generation = TRACKING_CAPACITY * scalar
    model = TrackingSummerHalfHour() if half_hour else TrackingSummerHour()

    # print(df_summer)
    df_summer['predictions'] = df_summer.apply(lambda x: calc_pred_generation(x, model), axis=1)

    # print(df_summer)
    # df_summer['predictions'] = df_summer['predictions'].apply(lambda x: round(x, 2))
    # print(df_summer)

    # apply smoothing function
    df_summer['predictions_final'] = df_summer['predictions'].map(
        lambda x: 0.0181*x*x+x*0.4049-0.1984 if x < 30 else x)
    df_summer['predictions_final'] = df_summer['predictions_final'].map(lambda x: 0 if x < 0 else x)

    # scale the output
    df_summer['predictions_final'] = df_summer['predictions_final'].map(lambda x: x * scalar)

    df_summer['predictions_final'] = df_summer['predictions_final'].map(lambda x: max_generation
                                                                        if x > max_generation else x)
    # df_summer['predictions_final'] = df_summer.apply(lambda x: 0 if x['hour_diff'] > 7 else x['predictions_final'],
    #                                                  axis=1)
    df_summer = df_summer.dropna()
    return df_summer
