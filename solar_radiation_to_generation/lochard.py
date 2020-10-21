import pandas as pd


def pickle_to_df(pickle_data):
    new_dict = {'VIC1': [], 'NSW1': [], 'SA1': [], 'QLD1': [], 'TAS1': []}
    for k, v in pickle_data.items():
        one_day_data_state = {'VIC1': [], 'NSW1': [], 'SA1': [], 'QLD1': [], 'TAS1': []}
        for hh, data in v.items():
            for state in states:
                one_day_data_state[state].append(data[state])
        for state in states:
            new_dict[state] += one_day_data_state[state]
    df_nem_prices = pd.DataFrame.from_dict(new_dict)
    # start_date = list(pickle_data.items())[0][0]
    #
    # start_date_datetime = datetime.datetime(start_date.year, start_date.month, start_date.day, 0, 0)
    #
    # df_nem_prices['Half Hour Starting'] = pd.date_range(start_date_datetime, periods=len(df_nem_prices), freq='30min')
    # # df_nem_prices['FY'] = df_nem_prices['Half Hour Starting'].apply(get_financial_year)
    # df_nem_prices_qtrly_avg = df_nem_prices.groupby(df_nem_prices['Half Hour Starting'].dt.to_period('Q')).mean()
    # df_nem_prices_yearly_avg = df_nem_prices.groupby(df_nem_prices['Half Hour Starting'].dt.to_period('Y')).mean()
    # # df_nem_prices_fiscal_avg = df_nem_prices.groupby(['FY']).mean()
    # price_stats_results = { # 'CalYear': df_nem_prices_yearly_avg,
    #                        'Qtr': df_nem_prices_qtrly_avg
    #                         }
    # end = time.time()
    # print(end - start)
    return df_nem_prices


def transform_df_to_db_structure(df_dict, run_id, run_comb, sim_id):
    df_final = pd.DataFrame()
    for period_type, avg_df in df_dict.items():
        for state in states:
            df = pd.DataFrame()
            df['SpotPrice'] = avg_df[state]
            df['RunID'] = run_id
            df['SimID'] = sim_id
            df['RunComb'] = run_comb
            df['State'] = state[:-1]
            df['PeriodType'] = period_type
            df['Date'] = avg_df.index.start_time.values
            df[period_type] = list(map(lambda x: str(x), list(avg_df.index)))
            df_final.append(df)
    return df_final.to_parquet('./'+run_comb+'.parquet.gzip', compression='gzip')




def contribution_margin(df, strategy):
    if strategy == 'Merchant Only':
        return list(df['NonFirmingContributionMargin'])
    elif strategy == 'Wind Offtake Only':
        return list(df['PPACFD'])
    elif strategy == 'Merchant + Wind Offtake':
        return df['NonFirmingContributionMargin'] + df['PPACFD']
    else:
        return list(df['ContributionMargin'])

def ebit(df, strategy):
    if strategy == 'Merchant Only':
        return df['ContributionMargin'] - df['FixedOM']
    elif strategy == 'Wind Offtake Only':
        return list(df['ContributionMargin'])
    else:
        return df['ContributionMargin'] - df['FixedOM']

def contribution_margin_dis(df, metric, base_metric):
    df = df.copy()
    df[metric] = list(df[base_metric])
    for scenario in df['Scenario'].unique():
        for firm_tect in df['FirmingTechnology'].unique():
            for tech in df['Technology'].unique():
                df_year_list = []
                for period in df['Period'].unique():
                    df_year_list.append(df[(df['Scenario'] == scenario) &
                                          (df['FirmingTechnology'] == firm_tect) &
                                          (df['Technology'] == tech) &
                                          (df['Period'] == period)])

                for last_yr_df, cur_yr_df in zip(df_year_list[:-1], df_year_list[1:]):
                    for (index_1, row_1), (index_2, row_2) in zip(last_yr_df.iterrows(), cur_yr_df.iterrows()):
                        df.loc[index_2, metric] = cur_yr_df.loc[index_2, metric] = (
                              row_2[base_metric] - row_1[base_metric]) / (1 +row_2['DiscountRate']) + row_1[metric]

    return df[metric]


def capital_adjustment_dis(df, strategy):
    # df = df.copy()
    if strategy == 'Wind Offtake Only':
        return 0
    else:
        return (df['CapitalExpenditure'] - df['TerminalValue']) / (1 + df['DiscountRate'])


def net_present_value(df):
    # df = df.copy()
    return df['EBITDiscounted'] - df['CapitalAdjustmentDiscounted']

def adjusted_EBIT(df):
    df = df.copy()
    for scenario in df['Scenario'].unique():
        for firm_tect in df['FirmingTechnology'].unique():
            for tech in df['Technology'].unique():
                df_year_list = []
                for period in df['Period'].unique():
                    df_year_list.append(df[(df['Scenario'] == scenario) &
                                          (df['FirmingTechnology'] == firm_tect) &
                                          (df['Technology'] == tech) &
                                          (df['Period'] == period)])

                for df_year in df_year_list:
                    df_year = df_year[df_year['Percentile'] != 'avg']
                    for (head_index, head_row), (tail_index, tail_row) in zip(df_year.iterrows(), df_year.iloc[::-1].iterrows()):
                        df.loc[head_index, 'AdjustedEBIT'] = head_row['EBIT'] + head_row['PPACFD'] + tail_row['MWSoldCFD']
    return list(df['AdjustedEBIT'])

'''Original method to calculate financial strategy dataframe (NOT USED)'''
def generate_full_financial_chart_for_strategy(df):
    strategy_table = pd.DataFrame()
    df['ContributionMargin'] = df['PPACFD'] + df['MWSoldCFD'] + df['NonFirmingContributionMargin']
    df['EBIT'] = df['ContributionMargin'] - df['FixedOM']
    # df['NetPresentValue'] = df['EBITDiscounted'] - df['CapitalExpenditure'] + df['TerminalValue']
    for strategy in ['Merchant Only','Wind Offtake Only', 'Merchant + Wind Offtake', 'Merchant + Wind Offtake + Selling Firm']:
        table = df.copy()

        table['ContributionMargin'] = contribution_margin(table, strategy)
        table['ContributionMarginDiscounted'] = contribution_margin_dis(table, 'ContributionMarginDiscounted', 'ContributionMargin')
        table['EBIT'] = ebit(table, strategy)

        table['EBITDiscounted'] = contribution_margin_dis(table, 'EBITDiscounted', 'EBIT')
        table['CapitalAdjustmentDiscounted'] = capital_adjustment_dis(table, strategy)

        table['NetPresentValue'] = net_present_value(table)
        table['Strategy'] = strategy
        strategy_table = strategy_table.append(table)
    return strategy_table

def shortent_scenario(series):
    scen = series.iloc[0]
    if 'All' in scen:
        return 'All'
    elif 'Base' in scen:
        return 'Base Case'
    elif 'Marinus' in scen and 'Snowy' in scen and 'Yallourn' in scen:
        return 'ML+Snwy+YPS'
    elif 'Marinus' in scen and 'Portland' in scen and 'Snowy' in scen:
        return 'ML+Prtlnd+Snwy'
    elif 'Marinus' in scen and 'Portland' in scen and 'Yallourn' in scen:
        return 'ML+Prtlnd+YPS'
    elif 'Yallourn' in scen and 'Portland' in scen and  'Snowy' in scen:
        return 'YPS+Prtld+Snwy'
    elif 'Marinus' in scen and 'Yallourn' in scen:
        return 'ML+YPS'
    elif 'Marinus' in scen and 'Snowy' in scen:
        return 'ML+Snwy'
    elif 'Marinus' in scen and 'Portland' in scen:
        return 'ML+Prtld'
    elif 'Yallourn' in scen and 'Snowy' in scen:
        return 'YPS+Snwy'
    elif 'Yallourn' in scen and 'Portland' in scen:
        return 'YPS+Prtld'
    elif 'Portland' in scen and 'Snowy' in scen:
        return 'Prtlnd+Snwy'
    elif 'Marinus' in scen:
        return 'ML 2028'
    elif 'Yallourn' in scen:
        return 'YPS 2027'
    elif 'Snowy' in scen:
        return 'Snwy 2027'
    elif 'Portland' in scen:
        return 'Prtlnd 2023'
    else:
        return None


def spot_transform(dir, type):
    # Spot price
    import os
    scenario_id_dict = {}
    # final_table = pd.DataFrame()
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            if file.split('.')[-1] != 'csv':
                continue
            path = os.path.join(subdir, file)

            table = pd.read_csv(path)

            table = table.rename(columns={'Run No.': 'RunID','Description':'RunComb', 'Sim No.':'SimID','Value':type}).drop(
                columns=['Variable'])
            try:
                table = table.drop(columns=['Percentile Location'])
            except:
                print('No percentile location')
            scenario_id_dict[file.split('.')[0]] = table.iloc[0]['RunID']
            table['State'] = 'VIC'
            table['PeriodType'] = 'CalYearly'
            table['RunComb'] = shortent_scenario(table['RunComb'])
            table['CalYear'] = table['Period'].map(lambda x: int(x[-2:]) + 2000)
            table['Date'] = table['CalYear'].map(lambda x: pd.Timestamp(x, 1, 1))
            table['FinYear'] = None
            table['Qtr'] = None
            table = table.drop(columns=['Period'])
            # final_table = final_table.append(table)
            table.to_csv('./{}/{}/{}_{}'.format('Results', type, subdir.split('/')[-1],file), index=False)
    # final_table.to_csv('./{}/{}/{}'.format('Results', type, type+'.csv'), index=False)

    with open('./run_ids_forward.txt', 'w+') as file:
        print(scenario_id_dict, file=file)

'''Get FinancialWalk data from excel'''
def get_strategy_walk(dir):
    import os
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            if file.split('.')[-1] != 'xlsx':
                continue
            path = os.path.join(subdir, file)
            df = pd.read_excel(path, sheet_name='FinancialWalk', header=3, usecols='B:H')
            df['Scenario'] = shortent_scenario(df['Scenario'])
            print(df)
            df.to_csv(f'{dir}/{file.split(".")[0].split(" ")[2]}.csv',index=False)


'''Get Financial strategy summary data from excel and merge into one df'''
def transform_financial_summary_excel(dir, chart='analysis'):
    import os
    full_df = None
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            if file.split('.')[-1] != 'xlsx':
                continue
            path = os.path.join(subdir, file)
            if chart == 'analysis':
                df = pd.read_excel(path, sheet_name='Summary', header=3, usecols='B:I')
                print(df)
                df = df.rename(columns={'Firming Technology': 'FirmingTechnology',
                                        'PPA CFD': 'PPACFD',
                                        'MW Sold CFD': 'MWSoldCFD',
                                        'Non-Firming Contribution Margin': 'NonFirmingContributionMargin'})
            elif chart == 'strategy':
                df = pd.read_excel(path, sheet_name='Financials', header=3, usecols='B:I')
                print(df)
                df = df.rename(columns={'Firming Technology': 'FirmingTechnology',
                                        'PPA CFD': 'PPACFD',
                                        'Correlated MW Sold CFD': 'MWSoldCFD',
                                        'Non-Firming Contribution Margin': 'NonFirmingContributionMargin'})

            df['Scenario'] = shortent_scenario(df['Scenario'])
            if full_df is None:
                full_df = df
            else:
                full_df = full_df.append(df)
    full_df.to_csv(f'{dir}/full.csv',index=False)
    return f'{dir}/full.csv'

'''Combine strategy walk df for each scen into one whole'''
def combine_strategy_walk(dir):
    import os
    full_df = pd.DataFrame(columns=['Scenario','FirmingTechnology','Percentile','Period','Technology','Strategy',
                                    'Metric','Value'])
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            if file.split('.')[-1] != 'csv':
                continue
            path = os.path.join(subdir, file)
            df = pd.read_csv(path)
            print(df)

            df['Value'] = pd.to_numeric(df['Value'])

            df = df.rename(columns={'Firming Technology': 'FirmingTechnology'})
            for index, row in df.iterrows():
                if 'Sold Swap' in row['Variable']:
                    if 'Merchant' in row['Variable']:
                        df.loc[index, 'Strategy'] = '5.Merchant with PPA and Sold Swap'
                    else:
                        df.loc[index, 'Strategy'] = '4.Sold Swap'
                elif 'PPA' in row['Variable']:
                    if 'Merchant' in row['Variable']:
                        df.loc[index, 'Strategy'] = '3.Merchant with PPA'
                    else:
                        df.loc[index, 'Strategy'] = '2.PPA'
                else:
                    df.loc[index, 'Strategy'] = '1.Merchant'


                if 'Discounted' in row['Variable']:
                    df.loc[index, 'Metric'] = 'EBIT Discounted'
                elif 'NPV' in row['Variable']:
                    if 'MWh' in row['Variable']:
                        df.loc[index, 'Metric'] = 'NPV ($/Mwh)'
                    else:
                        df.loc[index, 'Metric'] = 'NPV'
                else:
                    df.loc[index, 'Metric'] = 'ROI'
                    df.loc[index, 'Value'] = df.loc[index, 'Value'] * 100
            df = df.drop(columns=['Variable'])
            new_df = []
            combo_list = []
            for _,row in df.iterrows():
                if (row['Scenario'],row['FirmingTechnology'], row['Percentile'], row['Period'], row['Technology'], row['Strategy'],
                    row['Metric']) not in combo_list:
                    combo_list.append((row['Scenario'],row['FirmingTechnology'], row['Percentile'], row['Period'], row['Technology'], row['Strategy'],
                                        row['Metric']))
                    new_df.append(dict(row))
            full_df = full_df.append(pd.DataFrame.from_dict(new_df))
    full_df = full_df[['Scenario','Technology','Period','FirmingTechnology','Percentile','Strategy','Metric','Value']]
    full_df = full_df.reindex()
    full_df.to_csv(f'{dir}/strategy_walk_full.csv')

'''Complete financial analysis table'''
def merge_static_tables(df, static0, static1):
    table = df
    table_0 = static0
    table_1 = static1
    # # # table2 = table.copy()
    for index, row in table_1.iterrows():
        sub_df = table[(table['FirmingTechnology'] == row['FirmingTechnology']) & (table['Period']==row['Year'])]
        for sub_index, sub_row in sub_df.iterrows():
            table.loc[sub_index, 'PPAEnergy'] = row['PPAEnergy']
            table.loc[sub_index, 'PPACapacityMW'] = row['PPACapacityMW']
            table.loc[sub_index, 'MWSoldQuantity'] = row['MWSoldQuantity']

    for index, row in table_0.iterrows():
        sub_df = table[(table['Technology'] == row['Technology']) & (table['Period']==row['Period'])]
        for sub_index, sub_row in sub_df.iterrows():
            for col, var in dict(row).items():
                if col not in ['Period', 'Technology']:
                    table.loc[sub_index, col] = row[col]

    return table


def transform_scenario(table):
    table['EBIT'] = table['PPACFD'] +table['MWSoldCFD'] +\
                              table['NonFirmingContributionMargin'] - table['FixedOM']
    table['EBITDiscounted'] = table['EBIT']
    table['PPACFDAnnual'] = table['PPACFD']
    table['MWSoldCFDAnnual'] = table['MWSoldCFD']
    for scenario in table['Scenario'].unique():
        for firm_tect in table['FirmingTechnology'].unique():
            for tech in table['Technology'].unique():
                df_year_list = []
                for period in table['Period'].unique():
                    df_year_list.append(table[(table['Scenario'] == scenario) &
                                              (table['FirmingTechnology'] == firm_tect) &
                                              (table['Technology'] == tech) &
                                              (table['Period'] == period)])
                # TODO financial annual data
                for last_yr_df, cur_yr_df in zip(df_year_list[:-1], df_year_list[1:]):
                    for (index_1, row_1), (index_2, row_2) in zip(last_yr_df.iterrows(), cur_yr_df.iterrows()):
                        table.loc[index_2, 'EBITDiscounted'] = cur_yr_df.loc[index_2, 'EBITDiscounted'] = (row_2['EBIT'] - row_1['EBIT']) / (1 + row_2['DiscountRate']) + \
                            row_1['EBITDiscounted']
                        table.loc[index_2, 'PPACFDAnnual'] = cur_yr_df.loc[index_2, 'PPACFDAnnual'] = row_2['PPACFD'] - row_1['PPACFD']
                        table.loc[index_2, 'MWSoldCFDAnnual'] = cur_yr_df.loc[index_2, 'MWSoldCFDAnnual']  = row_2['MWSoldCFD'] - row_1['MWSoldCFD']
    return table

'''Transpose strategy walk table from row to key-value rows'''
def flattening(table):
    column_dicts = {
        'Scenario': [],
        'FirmingTechnology': [],
        'Percentile': [],
        'Period': [],
        'Technology': [],
        'Strategy': [],
        'Metric': [],
        'Value': []
    }
    for scenario in table['Scenario'].unique():
        for firm_tect in table['FirmingTechnology'].unique():
            for tech in table['Technology'].unique():
                df_year_list = []
                for period in table['Period'].unique():
                    df_year_list.append(table[(table['Scenario'] == scenario) &
                                              (table['FirmingTechnology'] == firm_tect) &
                                              (table['Technology'] == tech) &
                                              (table['Period'] == period)])

                for df_year in df_year_list:
                    for  _, row in df_year.iterrows():
                        for column in ["EBIT","EBITDiscounted","ContributionMargin","NetPresentValue",
                                           "ContributionMarginDiscounted","CapitalAdjustmentDiscounted"]:
                            # column_dicts['RunID'].append(row['RunID'])
                            column_dicts['Scenario'].append(row['Scenario'])
                            column_dicts['FirmingTechnology'].append(row['FirmingTechnology'])
                            column_dicts['Percentile'].append(row['Percentile'])
                            column_dicts['Period'].append(row['Period'])
                            column_dicts['Technology'].append(row['Technology'])
                            column_dicts['Strategy'].append(row['Strategy'])
                            column_dicts['Metric'].append(column)
                            column_dicts['Value'].append(row[column])
    return pd.DataFrame.from_dict(column_dicts)

'''To transpose financial analysis table from row to key value pair rows (NOT USED)'''
def flatenning_financial_table(table):
    transposed_table = pd.DataFrame(columns=['RunID', 'Scenario', 'FirmingTechnology', 'Percentile', 'Period', 'Technology',
                                             'Variable', 'Value'])
    column_dicts = {
        'RunID': [],
        'Scenario': [],
        'FirmingTechnology': [],
        'Percentile': [],
        'Period': [],
        'Technology': [],
        'Metric': [],
        'Value': []
    }
    for scenario in table['Scenario'].unique():
        for firm_tect in table['FirmingTechnology'].unique():
            for tech in table['Technology'].unique():
                df_year_list = []
                for period in table['Period'].unique():
                    df_year_list.append(table[(table['Scenario'] == scenario) &
                                              (table['FirmingTechnology'] == firm_tect) &
                                              (table['Technology'] == tech) &
                                              (table['Period'] == period)])

                for df_year in df_year_list:
                    for  _, row in df_year.iterrows():
                        for column in ['PPACFD','MWSoldCFD','NonFirmingContributionMargin','PPAEnergy','PPACapacityMW','MWSoldQuantity',
                                       'FixedOM','CapitalExpenditure','TerminalValue','DiscountRate','Hours','CumulativeHours',
                                       'CapacityMW','EBITDiscounted','PPACFDAnnual','MWSoldCFDAnnual','ContributionMargin','EBIT',
                                       'NetPresentValue']:
                            column_dicts['RunID'].append(row['RunID'])
                            column_dicts['Scenario'].append(row['Scenario'])
                            column_dicts['FirmingTechnology'].append(row['FirmingTechnology'])
                            column_dicts['Percentile'].append(row['Percentile'])
                            column_dicts['Period'].append(row['Period'])
                            column_dicts['Technology'].append(row['Technology'])
                            column_dicts['Metric'].append(column)
                            column_dicts['Value'].append(row[column])

    transposed_table = pd.Dataframe.from_dict(column_dicts)

'''Do this before financial analysis to make the PPACFD, MWSoldCFD and NFCM become cumaltive for each year'''
def cumulative_CFD(table):
    for scenario in table['Scenario'].unique():
        for firm_tect in table['FirmingTechnology'].unique():
            for tech in table['Technology'].unique():
                df_year_list = []
                for period in table['Period'].unique():
                    df_year_list.append(table[(table['Scenario'] == scenario) &
                                              (table['FirmingTechnology'] == firm_tect) &
                                              (table['Technology'] == tech) &
                                              (table['Period'] == period)])
                # TODO financial annual data
                for last_yr_df, cur_yr_df in zip(df_year_list[:-1], df_year_list[1:]):
                    for (index_1, row_1), (index_2, row_2) in zip(last_yr_df.iterrows(), cur_yr_df.iterrows()):
                        table.loc[index_2, 'PPACFD'] = cur_yr_df.loc[index_2, 'PPACFD'] = row_2['PPACFD'] + row_1['PPACFD']
                        table.loc[index_2, 'MWSoldCFD'] = cur_yr_df.loc[index_2, 'MWSoldCFD'] = row_2['MWSoldCFD'] + row_1['MWSoldCFD']
                        table.loc[index_2, 'NonFirmingContributionMargin'] = cur_yr_df.loc[index_2, 'NonFirmingContributionMargin'] = \
                            row_2['NonFirmingContributionMargin'] + row_1['NonFirmingContributionMargin']
    return table


def read_financials_tab(dir):
    import os
    column_dicts = {
        'Scenario': [],
        'FirmingTechnology': [],
        'Percentile': [],
        'Period': [],
        'Technology': [],
        'Strategy': [],
        'Metric': [],
        'Value': [],
    }
    for subdir, dirs, files in os.walk(dir):
        for file in files:
            if file.split('.')[-1] != 'xlsx':
                continue
            path = os.path.join(subdir, file)
            df = pd.read_excel(path, sheet_name='Financials', header=3, usecols='B:AI')
            df = df.rename(columns={'Firming Technology': 'FirmingTechnology', 'PPA CFD': 'PPACFD',
                                    'Correlated MW Sold CFD': 'MWSoldCFD',
                                    'Non-Firming Contribution Margin': 'NonFirmingContributionMargin'}).fillna(0)
            df['Scenario'] = shortent_scenario(df['Scenario'])
            for index, row in df.iterrows():
                for column in ['Merchant EBIT', 'Merchant EBIT Discounted', 'Merchant NPV','Merchant IRR',
                               'PPA EBIT', 'PPA EBIT Discounted', 'PPA NPV', 'PPA ROI',
                               'Merchant with PPA EBIT', 'Merchant with PPA EBIT Discounted', 'Merchant with PPA NPV', 'Merchant with PPA ROI',
                               'Sold Swap EBIT', 'Sold Swap Discounted', 'Sold Swap NPV', 'Sold Swap ROI',
                               'Merchant with PPA and Sold Swap EBIT', 'Merchant with PPA and Sold Swap EBIT Discounted',
                               'Merchant with PPA and Sold Swap NPV', 'Merchant with PPA and Sold Swap ROI']:
                    column_dicts['Scenario'].append(row['Scenario'])
                    column_dicts['FirmingTechnology'].append(row['FirmingTechnology'])
                    column_dicts['Percentile'].append(row['Percentile'])
                    column_dicts['Period'].append(row['Period'])
                    column_dicts['Technology'].append(row['Technology'])
                    if 'Sold' in column:
                        if 'Merchant' in column:
                            column_dicts['Strategy'].append('Merchant + Wind Offtake + Selling Firm')
                        else:
                            column_dicts['Strategy'].append('Selling Firm Only')
                    elif 'PPA' in column:
                        if 'Merchant' in column:
                            column_dicts['Strategy'].append('Merchant + Wind Offtake')
                        else:
                            column_dicts['Strategy'].append('Wind Offtake Only')
                    else:
                        column_dicts['Strategy'].append('Merchant Only')
                    if 'Discounted' in column:
                        column_dicts['Metric'].append('EBIT Disounted')
                        column_dicts['Value'].append(row[column])
                    elif 'ROI' in column:
                        column_dicts['Metric'].append('ROI')
                        column_dicts['Value'].append(row[column] * 100)
                    elif 'NPV' in column:
                        column_dicts['Metric'].append('NPV')
                        column_dicts['Value'].append(row[column])
                    elif 'EBIT' in column:
                        column_dicts['Metric'].append('EBIT')
                        column_dicts['Value'].append(row[column])
                    else:
                        column_dicts['Metric'].append('IRR')
                        column_dicts['Value'].append(row[column])
    return pd.DataFrame.from_dict(column_dicts)


if __name__ == '__main__':

    ''' Lochard update on Aug '''
    new_root = './Lochard-new'
    df = read_financials_tab(new_root)
    df.to_csv('financial_strategy.csv', index=False)

    # new_out = './Results-new'
    #
    # static_0 = './static_0.csv'
    # static_1 = './static_1.csv'
    # static0 = pd.read_csv(static_0)
    # static1 = pd.read_csv(static_1)
    #
    # # TODO extract summary data first
    # name = transform_financial_summary_excel(new_root, chart='strategy')
    #
    # # df = pd.read_csv(new_root+ '/full.csv')
    # df = pd.read_csv(name)
    # # TODO make CFD columns cumlative for years
    # # df = cumulative_CFD(df)
    #
    # # TODO generate full financial analysis
    # table = merge_static_tables(df, static0, static1)
    # df = transform_scenario(table)
    # # df.to_csv('financial_full.csv', index=False)
    #
    # # financial strategy
    # df = generate_full_financial_chart_for_strategy(df)
    # df = flattening(df)
    # df.to_csv('financial_strategy.csv', index=False)
    #
    # # TODO extract strategy data from excel
    # # get_strategy_walk(new_root)
    # # combine_strategy_walk(new_root)





