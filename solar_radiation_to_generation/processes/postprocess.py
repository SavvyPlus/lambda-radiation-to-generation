import pandas as pd


def scale_for_capacity(result_df, capacity_list, capacity_unit):
    generation_column_header = 'Estimate generation'
    generation_column = result_df[generation_column_header]
    result_df = result_df.drop(columns=[generation_column_header])
    # scaler = 1 if capacity_unit=='MWh' else 1000
    for capacity in capacity_list:
        ratio = capacity/capacity_list[0]
        this_header = f'{generation_column_header} of Capacity {capacity} {capacity_unit}'
        # for i in range(0,len(generation_column)):
        #     result_df.iloc[i][this_header] = generation_column[i] * ratio if generation_column[i] != 'N/A' else 'N/A'
        result_df[this_header] = generation_column.apply(lambda x: x * ratio if x != 'N/A' else 'N/A')
    return result_df
