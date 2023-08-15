import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime

import kis_api

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import korea_data_settings as kds


COLUMNS_CHART_DATA = ['date', 'open', 'high', 'low', 'close', 'volume']

COLUMNS_TRAINING_DATA_FROM_CHART = [
    'date',
    'open_lastclose_ratio',
    'high_close_ratio',
    'low_close_ratio',
    'close_lastclose_ratio',
    'volume_lastvolume_ratio',
    'close_ma5_ratio',
    'volume_ma5_ratio',
    'close_ma10_ratio',
    'volume_ma10_ratio',
    'close_ma20_ratio',
    'volume_ma20_ratio',
    'close_ma40_ratio',
    'volume_ma40_ratio',
    'close_ma60_ratio',
    'volume_ma60_ratio',
    'close_ma80_ratio',
    'volume_ma80_ratio',
    'close_ma100_ratio',
    'volume_ma100_ratio',
]


def preprocess(data):
    windows = [5, 10, 20, 40, 60, 80, 100]
    for window in windows:
        data[f'close_ma{window}'] = data['close'].rolling(window).mean()
        data[f'volume_ma{window}'] = data['volume'].rolling(window).mean()

        data[f'volume_ma{window}'] = data[f'volume_ma{window}']\
            .replace(to_replace=0, method='ffill')\
            .replace(to_replace=0, method='bfill')

        data[f'close_ma{window}_ratio'] = \
            (data['close'] - data[f'close_ma{window}']) / data[f'close_ma{window}']
        data[f'volume_ma{window}_ratio'] = \
            (data['volume'] - data[f'volume_ma{window}']) / data[f'volume_ma{window}']

    data['open_lastclose_ratio'] = np.zeros(len(data))
    data.loc[1:, 'open_lastclose_ratio'] = \
        (data['open'][1:].values - data['close'][:-1].values) / data['close'][:-1].values
    data['high_close_ratio'] = (data['high'].values - data['close'].values) / data['close'].values
    data['low_close_ratio'] = (data['low'].values - data['close'].values) / data['close'].values
    data['close_lastclose_ratio'] = np.zeros(len(data))
    data.loc[1:, 'close_lastclose_ratio'] = \
        (data['close'][1:].values - data['close'][:-1].values) / data['close'][:-1].values
    data['volume_lastvolume_ratio'] = np.zeros(len(data))

    try:
        data.loc[1:, 'volume_lastvolume_ratio'] = (
            (data['volume'][1:].values - data['volume'][:-1].values)
            / data['volume'][:-1].replace(to_replace=0, method='ffill')
            .replace(to_replace=0, method='bfill').values)
    except ZeroDivisionError:
        data['volume_lastvolume_ratio'] = 0

    return data


def load_data_from_chart(code):
    from_101 = kds.subtract_korea_stock_date(kds.latest_korea_stock_date(), 100)

    from_101_200 = kis_api.get_chart_price(code, period=100, end_date=from_101)
    if from_101_200 is None:
        return None

    from_today_100 = kis_api.get_chart_price(code, period=100, end_date=kds.latest_korea_stock_date())
    if from_today_100 is None:
        return None


    from_today_200 = pd.concat([from_today_100, from_101_200], ignore_index=True)
    from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
    from_today_200 = from_today_200.sort_values(by='date', ascending=True)
    from_today_200 = from_today_200.reset_index(drop=True)

    chart_data = from_today_200[COLUMNS_CHART_DATA]
    chart_data['date'] = pd.to_datetime(chart_data['date'], format='%Y%m%d')
    chart_data = chart_data.sort_values(by='date', ascending=True)
    chart_data = chart_data.set_index('date')

    from_today_200 = preprocess(from_today_200)

    pd.options.mode.chained_assignment = None
    training_data = from_today_200[COLUMNS_TRAINING_DATA_FROM_CHART]
    training_data['date'] = pd.to_datetime(training_data['date'], format='%Y%m%d')
    training_data = training_data.sort_values(by='date', ascending=True)
    training_data = training_data.set_index('date')
    training_data = training_data.dropna()

    df_stockfeatures = chart_data.merge(training_data, left_index=True, right_index=True, how='inner')

    update_date = datetime.today().strftime('%Y%m%d')

    if not os.path.exists(f'./../data/stock/{update_date}/'):
        os.makedirs(f'./../data/stock/{update_date}/')
        df_stockfeatures.to_csv(f'./../data/stock/{update_date}/{code}.csv')

    else:
        df_stockfeatures.to_csv(f'./../data/stock/{update_date}/{code}.csv')
