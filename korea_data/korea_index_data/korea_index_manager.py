import pandas as pd
import numpy as np
from tqdm import tqdm
import fsc_api as fsc
import korea_data.korea_data_settings as ks
from datetime import datetime


class kospi:
#코스피 데이터를 정제한다
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):

        from_today_200 = fsc.get_kospi_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        spi = kospi()
        from_today_200 = spi.preprocess(from_today_200)

        chart_data = from_today_200[kospi.COLUMNS_CHART_DATA]
        training_data = from_today_200[kospi.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_kospi_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_kospi_{update_date}.csv')

class kosdaq:
#코스닥 데이터를 정제한다.
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):

        from_today_200 = fsc.get_kosdaq_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)

        daq = kosdaq()
        from_today_200 = daq.preprocess(from_today_200)

        chart_data = from_today_200[kosdaq.COLUMNS_CHART_DATA]
        training_data = from_today_200[kosdaq.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_kosdaq_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_kosdaq_{update_date}.csv')



class kospi_200:
#코스피 200 데이터를 정제한다.
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):

        from_today_200 = fsc.get_kospi_200_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        kospi200 = kospi_200()
        from_today_200 = kospi200.preprocess(from_today_200)

        chart_data = from_today_200[kospi_200.COLUMNS_CHART_DATA]
        training_data = from_today_200[kospi_200.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_kospi_200_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_kospi_200_{update_date}.csv')

class krx_300:
#KRX(한국거래소)300 데이터를 정제한다.
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):

        from_today_200 = fsc.get_krx_300_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        krx300 = krx_300()
        from_today_200 = krx300.preprocess(from_today_200)

        chart_data = from_today_200[krx_300.COLUMNS_CHART_DATA]
        training_data = from_today_200[krx_300.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_krx_300_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_krx_300_{update_date}.csv')

class bond_k10y_future:
#국채10년물 선물 데이터를 정제한다.
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):
        from_today_200 = fsc.get_bond_k10y_future_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        bondk10yfuture = bond_k10y_future()
        from_today_200 = bondk10yfuture.preprocess(from_today_200)

        chart_data = from_today_200[bond_k10y_future.COLUMNS_CHART_DATA]
        training_data = from_today_200[bond_k10y_future.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_bond_k10y_future_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_bond_k10y_future_{update_date}.csv')


class krx_bond:
    COLUMNS_CHART_DATA = ['date','총수익지수 종가','순가격지수 종가','제로재투자지수 종가','콜재투자지수 종가','시장가격지수 종가']

    COLUMNS_TRAINNG_DATA_FROM_CHART = [
    'date',
    'totBnfIdxClpr_last_ratio',
    'nPrcIdxClpr_last_ratio',
    'zrRinvIdxClpr_last_ratio',
    'clRinvIdxClpr_last_ratio',
    'mrktPrcIdxClpr_last_ratio',
    'totBnfIdxClpr_ma5_ratio',
    'nPrcIdxClpr_ma5_ratio',
    'zrRinvIdxClpr_ma5_ratio',
    'clRinvIdxClpr_ma5_ratio',
    'mrktPrcIdxClpr_ma5_ratio',
    'totBnfIdxClpr_ma10_ratio',
    'nPrcIdxClpr_ma10_ratio',
    'zrRinvIdxClpr_ma10_ratio',
    'clRinvIdxClpr_ma10_ratio',
    'mrktPrcIdxClpr_ma10_ratio',
    'totBnfIdxClpr_ma20_ratio',
    'nPrcIdxClpr_ma20_ratio',
    'zrRinvIdxClpr_ma20_ratio',
    'clRinvIdxClpr_ma20_ratio',
    'mrktPrcIdxClpr_ma20_ratio',
    'totBnfIdxClpr_ma40_ratio',
    'nPrcIdxClpr_ma40_ratio',
    'zrRinvIdxClpr_ma40_ratio',
    'clRinvIdxClpr_ma40_ratio',
    'mrktPrcIdxClpr_ma40_ratio',
    'totBnfIdxClpr_ma60_ratio',
    'nPrcIdxClpr_ma60_ratio',
    'zrRinvIdxClpr_ma60_ratio',
    'clRinvIdxClpr_ma60_ratio',
    'mrktPrcIdxClpr_ma60_ratio',
    'totBnfIdxClpr_ma80_ratio',
    'nPrcIdxClpr_ma80_ratio',
    'zrRinvIdxClpr_ma80_ratio',
    'clRinvIdxClpr_ma80_ratio',
    'mrktPrcIdxClpr_ma80_ratio',
    'totBnfIdxClpr_ma100_ratio',
    'nPrcIdxClpr_ma100_ratio',
    'zrRinvIdxClpr_ma100_ratio',
    'clRinvIdxClpr_ma100_ratio',
    'mrktPrcIdxClpr_ma100_ratio',
    ]

    def preprocess(self, data):
        windows = [5, 10, 20, 40, 60, 80, 100]
        for window in windows:
            data[f'totBnfIdxClpr_ma{window}'] = data['totBnfIdxClpr'].rolling(window).mean()
            data[f'nPrcIdxClpr_ma{window}'] = data['nPrcIdxClpr'].rolling(window).mean()
            data[f'zrRinvIdxClpr_ma{window}'] = data['zrRinvIdxClpr'].rolling(window).mean()
            data[f'clRinvIdxClpr_ma{window}'] = data['clRinvIdxClpr'].rolling(window).mean()
            data[f'mrktPrcIdxClpr_ma{window}'] = data['mrktPrcIdxClpr'].rolling(window).mean()

            data[f'totBnfIdxClpr_ma{window}_ratio'] = \
                (data['totBnfIdxClpr'] - data[f'totBnfIdxClpr_ma{window}']) / data[f'totBnfIdxClpr_ma{window}']
            data[f'nPrcIdxClpr_ma{window}_ratio'] = \
                (data['nPrcIdxClpr'] - data[f'nPrcIdxClpr_ma{window}']) / data[f'nPrcIdxClpr_ma{window}']
            data[f'zrRinvIdxClpr_ma{window}_ratio'] = \
                (data['zrRinvIdxClpr'] - data[f'zrRinvIdxClpr_ma{window}']) / data[f'zrRinvIdxClpr_ma{window}']
            data[f'clRinvIdxClpr_ma{window}_ratio'] = \
                (data['clRinvIdxClpr'] - data[f'clRinvIdxClpr_ma{window}']) / data[f'clRinvIdxClpr_ma{window}']
            data[f'mrktPrcIdxClpr_ma{window}_ratio'] = \
                (data['mrktPrcIdxClpr'] - data[f'mrktPrcIdxClpr_ma{window}']) / data[f'mrktPrcIdxClpr_ma{window}']

        data['totBnfIdxClpr_last_ratio'] = np.zeros(len(data))
        data.loc[1:, 'totBnfIdxClpr_last_ratio'] = \
            (data['totBnfIdxClpr'][1:].values - data['totBnfIdxClpr'][:-1].values) / data['totBnfIdxClpr'][:-1].values

        data['nPrcIdxClpr_last_ratio'] = np.zeros(len(data))
        data.loc[1:, 'nPrcIdxClpr_last_ratio'] = \
            (data['nPrcIdxClpr'][1:].values - data['nPrcIdxClpr'][:-1].values) / data['nPrcIdxClpr'][:-1].values

        data['zrRinvIdxClpr_last_ratio'] = np.zeros(len(data))
        data.loc[1:, 'zrRinvIdxClpr_last_ratio'] = \
            (data['zrRinvIdxClpr'][1:].values - data['zrRinvIdxClpr'][:-1].values) / data['zrRinvIdxClpr'][:-1].values

        data['clRinvIdxClpr_last_ratio'] = np.zeros(len(data))
        data.loc[1:, 'clRinvIdxClpr_last_ratio'] = \
            (data['clRinvIdxClpr'][1:].values - data['clRinvIdxClpr'][:-1].values) / data['clRinvIdxClpr'][:-1].values

        data['mrktPrcIdxClpr_last_ratio'] = np.zeros(len(data))
        data.loc[1:, 'mrktPrcIdxClpr_last_ratio'] = \
            (data['mrktPrcIdxClpr'][1:].values - data['mrktPrcIdxClpr'][:-1].values) / data['mrktPrcIdxClpr'][:-1].values

        return data

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):
        from_today_200 = fsc.get_krx_bond_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        bondk10yfuture = bond_k10y_future()
        from_today_200 = bondk10yfuture.preprocess(from_today_200)

        chart_data = from_today_200[bond_k10y_future.COLUMNS_CHART_DATA]
        training_data = from_today_200[bond_k10y_future.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_bond_k10y_future_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_bond_k10y_future_{update_date}.csv')


class bond_k10y_future:
#국채10년물 선물 데이터를 정제한다.
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):

        from_today_200 = fsc.get_bond_k10y_future_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        bondk10yfuture = bond_k10y_future()
        from_today_200 = bondk10yfuture.preprocess(from_today_200)

        chart_data = from_today_200[bond_k10y_future.COLUMNS_CHART_DATA]
        training_data = from_today_200[bond_k10y_future.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_bond_k10y_future_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_bond_k10y_future_{update_date}.csv')

class bond_k3_10y_future:
#국채10년물 선물 데이터를 정제한다.
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


    def preprocess(self, data):
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

    def load_data_from_chart(self, period=200, end_date=ks.latest_korea_stock_date_nextday()):

        from_today_200 = fsc.get_bond_future_k10y_index(period=period, end_date=end_date)
        from_today_200['date'] = pd.to_datetime(from_today_200['date'], format='%Y%m%d')
        from_today_200 = from_today_200.sort_values(by='date', ascending=True)
        from_today_200 = from_today_200.reset_index(drop=True)
        bondk10yfuture = bond_k10y_future()
        from_today_200 = bondk10yfuture.preprocess(from_today_200)

        chart_data = from_today_200[bond_k10y_future.COLUMNS_CHART_DATA]
        training_data = from_today_200[bond_k10y_future.COLUMNS_TRAINING_DATA_FROM_CHART]
        training_data = training_data.dropna()
        training_data = training_data.reset_index(drop=True)
        update_date = datetime.today().strftime('%Y%m%d')
        chart_data.to_csv(f'marketdata_bond_k10y_future_{update_date}.csv')
        training_data.to_csv(f'marketfeatures_bond_k10y_future_{update_date}.csv')

spi = kospi()
daq = kosdaq()
kospi200 = kospi_200()
krx300 = krx_300()
bondk10yfuture = bond_k10y_future()



