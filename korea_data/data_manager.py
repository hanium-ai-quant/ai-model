import pandas as pd
import numpy as np

COLUMNS_CHART_DATA = ['date', 'open', 'high', 'low', 'close', 'volume']

COLUMNS_TRAINING_DATA_FROM_CHART = [
    'open_lastclose_ratio', 'high_close_ratio', 'low_close_ratio',
    'close_lastclose_ratio', 'volume_lastvolume_ratio',
    'close_ma5_ratio', 'volume_ma5_ratio',
    'close_ma10_ratio', 'volume_ma10_ratio',
    'close_ma20_ratio', 'volume_ma20_ratio',
    'close_ma60_ratio', 'volume_ma60_ratio',
    'close_ma120_ratio', 'volume_ma120_ratio',
]


def preprocess(data):
    windows = [5, 10, 20, 60, 120]
    for window in windows:
        data[f'close_ma{window}'] = data['close'].rolling(window).mean()
        data[f'volume_ma{window}'] = data['volume'].rolling(window).mean()
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
    data.loc[1:, 'volume_lastvolume_ratio'] = (
            (data['volume'][1:].values - data['volume'][:-1].values)
            / data['volume'][:-1].replace(to_replace=0, method='ffill')\
            .replace(to_replace=0, method='bfill').values)

    return data