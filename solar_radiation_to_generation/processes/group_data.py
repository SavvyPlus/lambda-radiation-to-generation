import pandas as pd


def group_data(result_df, resolution, generation):
    """
    Group the raw solar radiation data based on the input resolution.
    In the output dataframe, all generation will be converted to numeric
    type, and the empty slot will be parsed to 'N/A'

    :param result_df: the raw solar radiation dataframe.
    :type result_df: pd.Dataframe
    :param resolution: the input resolution.
    :type resolution: str
    :param generation: whether it includes generation
    :type generation: bool
    """
    result_df['TimeStamp'] = pd.to_datetime(result_df['TimeStamp'])
    result_df['Year'] = result_df['TimeStamp'].map(lambda x: x.year)
    result_df['Month'] = result_df['TimeStamp'].map(lambda x: x.month)
    result_df['Day'] = result_df['TimeStamp'].map(lambda x: x.day)
    result_df['WeekNo'] = result_df['TimeStamp'].map(lambda x: x.weekofyear)
    result_df['DNI'] = pd.to_numeric(result_df['DNI'], errors='coerce')
    result_df['GHI'] = pd.to_numeric(result_df['GHI'], errors='coerce')
    if generation:
        result_df['Estimate generation(kW)'] = pd.to_numeric(result_df['Estimate generation(kW)'], errors='coerce')

    if resolution == 'hourly':
        grouped_df = result_df.drop(columns=['Year', 'Day', 'Month', 'WeekNo'])
    elif resolution == 'daily':
        grouped_df = result_df.groupby(by=['Year', 'Month', 'Day']).mean().reset_index()
        grouped_df = grouped_df.drop(columns=['WeekNo'])
    elif resolution == 'weekly':
        grouped_df = result_df.groupby(by=['Year', 'WeekNo']).mean().reset_index()
        grouped_df = grouped_df.drop(columns=['Day', 'Month'])
    elif resolution == 'monthly':
        grouped_df = result_df.groupby(by=['Year', 'Month']).mean().reset_index()
        grouped_df = grouped_df.drop(columns=['Day', 'WeekNo'])
    else:
        grouped_df = result_df.groupby(by=['Year']).mean().reset_index()
        grouped_df = grouped_df.drop(columns=['Month', 'Day', 'WeekNo'])

    grouped_df = grouped_df.fillna('N/A')
    # Reverse the whole data order so users won't see 'N/A' generation at the beginning
    grouped_df = grouped_df.iloc[::-1]
    return grouped_df
