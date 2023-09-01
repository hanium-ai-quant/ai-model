import scipy
import numpy as np
import pandas as pd
from datetime import datetime
import os


def label_manager(code):
    stocks = os.listdir('./../data/stock')
    dates = [str(stock) for stock in stocks]

    path = f'./../data/stock/{max(dates)}'
    file_path = os.path.join(path, f'{code}.csv')
    df_feature = pd.read_csv(file_path, index_col=False, encoding='utf-8')
    peaks = []
    peaks.extend(scipy.signal.find_peaks(df_feature['close'], distance=5, width=10)[0])
    peaks.extend(scipy.signal.find_peaks(-df_feature['close'], distance=5, width=10)[0])
    if len(df_feature) - 1 not in peaks:
        peaks.append(len(df_feature) - 1)

    df_feature.loc[:, 'peak_date'] = ''
    df_feature.loc[:, 'peak_close'] = np.nan
    df_feature.loc[:, 'peak_diffratio'] = np.nan
    df_feature.loc[:, 'interpeak_mdd'] = np.nan
    df_feature.loc[:, 'interpeak_trans_price_exp'] = np.nan

    _last_date = ''
    for peak in peaks:
        _date = df_feature.iloc[peak]['date']
        _close = df_feature.iloc[peak]['close']
        mask = (df_feature['date'] >= _last_date) & (df_feature['date'] <= _date)
        _last_date = _date
        if len(df_feature[mask]) > 0:
            df_feature.loc[mask, 'peak_date'] = _date
            df_feature.loc[mask, 'peak_close'] = _close
            _x = np.array(df_feature.loc[mask, 'close'])
            lower = np.argmax(np.maximum.accumulate(_x) - _x)
            upper = np.argmax(_x[:lower + 1])
            df_feature.loc[mask, 'interpeak_mdd'] = (_x[lower] - _x[upper]) / _x[upper]
            df_feature.loc[mask, 'interpeak_trans_price_exp'] = df_feature.loc[mask, 'trans_price_exp'].mean()
    df_feature.loc[:, 'peak_diffratio'] = (df_feature.loc[:, 'peak_close'] - df_feature.loc[:, 'close']) / df_feature.loc[:, 'close']
    df_feature = df_feature.drop(['peak_date', 'peak_close'], axis=1)
    file_path = os.path.join(path, f'{code}_labeled.csv')
<<<<<<< HEAD
    df_feature.to_csv(file_path, index=False, encoding='utf-8')
=======
    df_feature.to_csv(file_path, index=False, encoding='utf-8')

>>>>>>> origin/kimdaehwan
