import pandas as pd


def scale_for_capacity(result_df, capacity_list, capacity_unit):
    generation_column_header = result_df.columns[-1]
    generation_column = result_df[generation_column_header]
    result_df = result_df.drop(columns=[generation_column_header])
    # scaler = 1 if capacity_unit=='MWh' else 1000
    for capacity in capacity_list:
        ratio = capacity/capacity_list[0]
        this_header = f'{generation_column_header} of Capacity {capacity} {capacity_unit}'
        result_df[this_header] = generation_column * ratio
    return result_df
