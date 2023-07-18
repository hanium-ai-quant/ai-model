import pandas as pd
import numpy as np
from tqdm import tqdm
import kis_api as kis
import korea_data_settings as ks
from datetime import datetime
COLUMNS_CHART_DATA = ['date', 'open', 'high', 'low', 'close', 'volume']

COLUMNS_TRAINING_DATA_FROM_CHART = [
    'open_lastclose_ratio', 'high_close_ratio', 'low_close_ratio',
    'close_lastclose_ratio', 'volume_lastvolume_ratio',
    'close_ma5_ratio', 'volume_ma5_ratio',
    'close_ma10_ratio', 'volume_ma10_ratio',
    'close_ma20_ratio', 'volume_ma20_ratio',
    'close_ma40_ratio', 'volume_ma40_ratio',
    'close_ma60_ratio', 'volume_ma60_ratio',
    'close_ma80_ratio', 'volume_ma80_ratio',
    'close_ma100_ratio', 'volume_ma100_ratio',
]


def preprocess(data):
    windows = [5, 10, 20, 40, 60, 80, 100]
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

def load_data_from_chart(code):

    from_101 = ks.subtract_korea_stock_date(ks.latest_korea_stock_date(), 100)

    from_101_200 = kis.get_chart_price(code, period=100, end_date=from_101)
    from_today_100 = kis.get_chart_price(code, period=100, end_date=ks.latest_korea_stock_date())

    from_today_200 = pd.concat([from_today_100, from_101_200],ignore_index=True)
    from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
    from_today_200 = from_today_200.sort_values(by='date', ascending=True)
    from_today_200 = from_today_200.reset_index(drop=True)

    from_today_200 = preprocess(from_today_200)

    chart_data = from_today_200[COLUMNS_CHART_DATA]
    training_data = from_today_200[COLUMNS_TRAINING_DATA_FROM_CHART]
    training_data = training_data.dropna()
    training_data = training_data.reset_index(drop=True)
    update_date = datetime.today().strftime('%Y%m%d')
    training_data.to_csv(f'stockfeatures_{code}_{update_date}.csv')


load_data_from_chart('005930')