import io
import boto3
import pandas as pd


WINTER = [4, 5, 6, 7, 8, 9]
SUMMER = [1, 2, 3, 10, 11, 12]
NON_TRACKING_CAPACITY = 102


class NonTrackingWinterHour:
    GHI_filled = 0.0091
    DNI_filled = 0.0017
    Act_DNI_pred = 0.1628
    Act_GHI_pred = 0.3489
    time_diff = -0.1787
    hour_diff = 2.8331
    Act_DNI_pred_lag = 0.0613
    Act_DNI_pred_lead = 0.0715
    Act_GHI_pred_lag = 0.4776
    Act_GHI_pred_lead = 0.0476


class NonTrackingWinterHalfHour:
    GHI_filled = 0.0246
    DNI_filled = -0.0014
    Act_DNI_pred = -0.0010
    Act_GHI_pred = 0.6992
    time_diff = -0.1584
    hour_diff = 2.1836
    Act_DNI_pred_lag = 0.3056
    Act_DNI_pred_lead = 0.1249
    Act_GHI_pred_lag = 0.1833
    Act_GHI_pred_lead = -0.2361


class NonTrackingSummerHour:
    GHI_filled = 0.0308
    DNI_filled = -0.0324
    Act_DNI_pred = 0.3208
    Act_GHI_pred = 0.1696
    time_diff = -0.0630
    hour_diff = 0.4934
    Act_DNI_pred_lag = 0.0276
    Act_DNI_pred_lead = 0.0144
    Act_GHI_pred_lag = 0.0332
    Act_GHI_pred_lead = 0.5097


class NonTrackingSummerHalfHour:
    GHI_filled = 0.0575
    DNI_filled = -0.0335
    Act_DNI_pred = 0.1640
    Act_GHI_pred = 0.3098
    time_diff = -0.0977
    hour_diff = 1.6184
    Act_DNI_pred_lag = 0.0603
    Act_DNI_pred_lead = 0.1220
    Act_GHI_pred_lag = -0.1215
    Act_GHI_pred_lead = 0.3350


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
            Key='solar-radiation-to-generation/NonTrackingModel/combined_GHI_median_function_final.csv'
        )
    else:
        file_obj = client.get_object(
            Bucket='solar-radiation',
            Key='solar-radiation-to-generation/NonTrackingModel/predictions_DNI.csv'
        )
    dataframe = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
    dataframe.rename(columns={'Irr_Num_seq': 'Irradiance'}, inplace=True)
    return dataframe


def calc_tracking_winter(df, scalar, half_hour):
    df_winter = df[df.month.isin(WINTER)]
    # print(len(df_winter), 13590)  # 13590
    df_winter = df_winter.dropna()

    max_generation = NON_TRACKING_CAPACITY * scalar
    model = NonTrackingWinterHalfHour() if half_hour else NonTrackingWinterHour()

    # print(df_winter)
    df_winter['predictions'] = df_winter.apply(lambda x: calc_pred_generation(x, model), axis=1)

    # print(df_winter)
    # df_winter['predictions'] = df_winter['predictions'].apply(lambda x: round(x, 2))
    # print(df_winter)

    # apply smoothing function
    df_winter['predictions_final'] = df_winter['predictions'].map(
        lambda x: 0.0181*x*x+0.1714*x+0.119 if x < 30 else x)
    df_winter['predictions_final'] = df_winter['predictions_final'].map(lambda x: 0 if x < 0 else x)

    # scale the output
    df_winter['predictions_final'] = df_winter['predictions_final'].map(lambda x:x * scalar)

    df_winter['predictions_final'] = df_winter['predictions_final'].map(lambda x: max_generation
                                                                        if x > max_generation else x)
    # df_winter['predictions_final'] = df_winter.apply(lambda x: 0 if x['hour_diff'] > 5 else x['predictions_final'],
    #                                                  axis=1)
    df_winter = df_winter.dropna()
    return df_winter


def calc_tracking_summer(df, scalar, half_hour):
    df_summer = df[df.month.isin(SUMMER)]
    # print(len(df_summer), 13590)  # 13590
    df_summer = df_summer.dropna()
    max_generation = NON_TRACKING_CAPACITY * scalar
    # print('max: ', max_generation)
    model = NonTrackingSummerHalfHour() if half_hour else NonTrackingSummerHour()

    # print(df_summer)
    df_summer['predictions'] = df_summer.apply(lambda x: calc_pred_generation(x, model), axis=1)

    # print(df_summer)
    # df_summer['predictions'] = df_summer['predictions'].apply(lambda x: round(x, 2))
    # print(df_summer)

    # apply smoothing function
    df_summer['predictions_final'] = df_summer['predictions'].map(
        lambda x: -0.028*x*x+6.4048*x-259.3 if x > 85 else x)
    df_summer['predictions_final'] = df_summer['predictions_final'].map(lambda x: 0 if x < 0 else x)

    # scale the output
    df_summer['predictions_final'] = df_summer['predictions_final'].map(lambda x:x * scalar)

    df_summer['predictions_final'] = df_summer['predictions_final'].map(lambda x: max_generation
                                                                        if x > max_generation else x)
    # df_summer['predictions_final'] = df_summer.apply(lambda x: 0 if x['hour_diff'] > 7 else x['predictions_final'],
    #                                                  axis=1)
    df_summer = df_summer.dropna()
    return df_summer