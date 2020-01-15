import pandas as pd


def combine_hourly_radiation(df_dni, df_ghi):
    """
    Combine dni and ghi data frames in to one.

    :param df_dni: the dni data frame
    :type df_dni: pd.DataFrame
    :param df_ghi: the ghi data frame
    :type df_ghi: pd.DataFrame
    """

    # df_dni = pd.read_csv(dni_name, header=None)
    combined_df = complete_time_series(df_dni)
    combined_df = combined_df.rename(columns={'Radiation': 'DNI'})  # rename the column
    # print(combined_df)

    df_ghi = complete_time_series(df_ghi)
    combined_df['GHI'] = df_ghi['Radiation']  # add a column to the DataFrame
    combined_df['TimeStamp'] = combined_df.index
    # print(combined_df)
    # reorder the columns
    cols = combined_df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    combined_df = combined_df[cols]
    # convert Timestamp to datetime
    combined_df['TimeStamp'] = combined_df['TimeStamp'].dt.strftime('%d/%m/%Y %H:%M')
    combined_df.fillna(0)

    print('finished pre process')
    return combined_df


def complete_time_series(df):
    """
    Transfer the raw date in the data frame into timestamp.

    :param df: a data frame that has raw date column
    :type df: pd.DataFrame
    """
    # read-in time series and data
    df_raw = df
    df_raw.columns = ['TimeStamp', 'Radiation']
    df_raw['TimeStamp'] = pd.to_datetime(df_raw['TimeStamp'])  # convert the TimeStamp column from string to datetime
    df_raw = df_raw.drop_duplicates(subset='TimeStamp', keep='first')  # remove the duplicates due to the DST
    df_raw = df_raw.set_index('TimeStamp')  # use TimeStamp column as the index
    # print(df_raw)
    df_filled = df_raw.reindex(pd.date_range(df_raw.index[0], df_raw.index[-1], freq='H'), fill_value="")
    # print(df_raw)
    return df_filled
