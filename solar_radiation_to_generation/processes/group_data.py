import pandas as pd


def group_data(complete_df, resolution, generation, start_date):
    """
    Group the raw solar radiation data based on the input resolution.
    In the output dataframe, all generation will be converted to numeric
    type, and the empty slot will be parsed to 'N/A'

    :param complete_df: the raw solar radiation dataframe.
    :type complete_df: pd.Dataframe
    :param resolution: the input resolution.
    :type resolution: str
    :param generation: whether it includes generation
    :type generation: bool
    """
    complete_df['TimeStamp'] = pd.to_datetime(complete_df['TimeStamp'])
    complete_df['Year'] = complete_df['TimeStamp'].map(lambda x: x.year)
    complete_df['Month'] = complete_df['TimeStamp'].map(lambda x: x.month)
    complete_df['Day'] = complete_df['TimeStamp'].map(lambda x: x.day)
    complete_df['Hour'] = complete_df['TimeStamp'].map(lambda x: x.hour)
    complete_df['WeekNo'] = complete_df['TimeStamp'].map(lambda x: x.weekofyear)
    complete_df['DNI'] = pd.to_numeric(complete_df['DNI'], errors='coerce')
    complete_df['GHI'] = pd.to_numeric(complete_df['GHI'], errors='coerce')
    # if generation:
    #     result_df['Estimate generation(kW)'] = pd.to_numeric(result_df['Estimate generation(kW)'], errors='coerce')

    if resolution == 'halfhourly':
        generation_column = complete_df['Estimate generation']
        grouped_df = complete_df.drop(columns=['WeekNo', 'DNI', 'GHI', 'Hour', 'Estimate generation'])
        grouped_df['Estimate generation'] = generation_column
        grouped_df = grouped_df.sort_values(by=['TimeStamp'])
    else:
        # if generation:
        #     complete_df['Estimate generation'] = complete_df['Estimate generation']/2
        if resolution == 'hourly':
            grouped_df = complete_df.groupby(by=['Year', 'Month', 'Day', 'Hour']).sum().reset_index()
            grouped_df = grouped_df.drop(columns=['Year', 'Day', 'Month', 'WeekNo', 'Hour'])
        elif resolution == 'daily':
            grouped_df = complete_df.groupby(by=['Year', 'Month', 'Day']).sum().reset_index()
            grouped_df = grouped_df.drop(columns=['WeekNo', 'Hour'])
            grouped_df['Estimate generation'] = grouped_df.apply(lambda x: 'N/A' if x['Year']<start_date.year
                                                    or (x['Year']==start_date.year and x['Month']<start_date.month)
                                                    or (x['Year']==start_date.year and x['Month']==start_date.month and x['Day'] < start_date.day)
                                                    else x['Estimate generation'], axis=1)
        elif resolution == 'weekly':
            grouped_df = complete_df.groupby(by=['Year', 'WeekNo']).sum().reset_index()
            grouped_df['Estimate generation'] = grouped_df.apply(lambda x: 'N/A' if x['Year']<start_date.year
                                                    or (x['Year']==start_date.year and x['WeekNo']<pd.Timestamp(start_date).weekofyear)
                                                    else x['Estimate generation'], axis=1)
            grouped_df = grouped_df.drop(columns=['Day', 'Month', 'Hour'])
        elif resolution == 'monthly':
            grouped_df = complete_df.groupby(by=['Year', 'Month']).sum().reset_index()
            grouped_df = grouped_df.drop(columns=['Day', 'WeekNo', 'Hour'])
            grouped_df['Estimate generation'] = grouped_df.apply(lambda x: 'N/A' if x['Year']<start_date.year
                                                    or (x['Year']==start_date.year and x['Month']<start_date.month)
                                                    else x['Estimate generation'], axis=1)
        else:
            grouped_df = complete_df.groupby(by=['Year']).sum().reset_index()
            grouped_df = grouped_df.drop(columns=['Month', 'Day', 'WeekNo', 'Hour'])
            grouped_df['Estimate generation'] = grouped_df.apply(lambda x: 'N/A' if x['Year']<start_date.year
                                                    else x['Estimate generation'], axis=1)

    grouped_df = grouped_df.fillna('N/A')
    # Reverse the whole data order so users won't see 'N/A' generation at the beginning
    grouped_df = grouped_df.iloc[::-1]
    return grouped_df
