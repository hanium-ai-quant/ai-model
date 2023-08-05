import sys
from tqdm import tqdm
import pandas as pd
from datetime import datetime
import yaml
import os

sys.path.append('./korea_data/korea_stock_data/')
sys.path.append('./korea_data/korea_index_data/')

from korea_data.korea_stock_data import wics
from korea_data.korea_stock_data import korea_stock_manager as ksm
from korea_data.korea_index_data import korea_index_manager as kim

COLUMNS_TRAINING_DATA = [
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
    'kospi_open_lastclose_ratio',
    'kospi_high_close_ratio',
    'kospi_low_close_ratio',
    'kospi_close_lastclose_ratio',
    'kospi_volume_lastvolume_ratio',
    'kospi_close_ma5_ratio',
    'kospi_volume_ma5_ratio',
    'kospi_close_ma10_ratio',
    'kospi_volume_ma10_ratio',
    'kospi_close_ma20_ratio',
    'kospi_volume_ma20_ratio',
    'kospi_close_ma40_ratio',
    'kospi_volume_ma40_ratio',
    'kospi_close_ma60_ratio',
    'kospi_volume_ma60_ratio',
    'kospi_close_ma80_ratio',
    'kospi_volume_ma80_ratio',
    'kospi_close_ma100_ratio',
    'kospi_volume_ma100_ratio',
    'kosdaq_open_lastclose_ratio',
    'kosdaq_high_close_ratio',
    'kosdaq_low_close_ratio',
    'kosdaq_close_lastclose_ratio',
    'kosdaq_volume_lastvolume_ratio',
    'kosdaq_close_ma5_ratio',
    'kosdaq_volume_ma5_ratio',
    'kosdaq_close_ma10_ratio',
    'kosdaq_volume_ma10_ratio',
    'kosdaq_close_ma20_ratio',
    'kosdaq_volume_ma20_ratio',
    'kosdaq_close_ma40_ratio',
    'kosdaq_volume_ma40_ratio',
    'kosdaq_close_ma60_ratio',
    'kosdaq_volume_ma60_ratio',
    'kosdaq_close_ma80_ratio',
    'kosdaq_volume_ma80_ratio',
    'kosdaq_close_ma100_ratio',
    'kosdaq_volume_ma100_ratio',
    'kospi200_open_lastclose_ratio',
    'kospi200_high_close_ratio',
    'kospi200_low_close_ratio',
    'kospi200_close_lastclose_ratio',
    'kospi200_volume_lastvolume_ratio',
    'kospi200_close_ma5_ratio',
    'kospi200_volume_ma5_ratio',
    'kospi200_close_ma10_ratio',
    'kospi200_volume_ma10_ratio',
    'kospi200_close_ma20_ratio',
    'kospi200_volume_ma20_ratio',
    'kospi200_close_ma40_ratio',
    'kospi200_volume_ma40_ratio',
    'kospi200_close_ma60_ratio',
    'kospi200_volume_ma60_ratio',
    'kospi200_close_ma80_ratio',
    'kospi200_volume_ma80_ratio',
    'kospi200_close_ma100_ratio',
    'kospi200_volume_ma100_ratio',
    'krx300_open_lastclose_ratio',
    'krx300_high_close_ratio',
    'krx300_low_close_ratio',
    'krx300_close_lastclose_ratio',
    'krx300_volume_lastvolume_ratio',
    'krx300_close_ma5_ratio',
    'krx300_volume_ma5_ratio',
    'krx300_close_ma10_ratio',
    'krx300_volume_ma10_ratio',
    'krx300_close_ma20_ratio',
    'krx300_volume_ma20_ratio',
    'krx300_close_ma40_ratio',
    'krx300_volume_ma40_ratio',
    'krx300_close_ma60_ratio',
    'krx300_volume_ma60_ratio',
    'krx300_close_ma80_ratio',
    'krx300_volume_ma80_ratio',
    'krx300_close_ma100_ratio',
    'krx300_volume_ma100_ratio',
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
    'bondk10yfuture_open_lastclose_ratio',
    'bondk10yfuture_high_close_ratio',
    'bondk10yfuture_low_close_ratio',
    'bondk10yfuture_close_lastclose_ratio',
    'bondk10yfuture_volume_lastvolume_ratio',
    'bondk10yfuture_close_ma5_ratio',
    'bondk10yfuture_volume_ma5_ratio',
    'bondk10yfuture_close_ma10_ratio',
    'bondk10yfuture_volume_ma10_ratio',
    'bondk10yfuture_close_ma20_ratio',
    'bondk10yfuture_volume_ma20_ratio',
    'bondk10yfuture_close_ma40_ratio',
    'bondk10yfuture_volume_ma40_ratio',
    'bondk10yfuture_close_ma60_ratio',
    'bondk10yfuture_volume_ma60_ratio',
    'bondk10yfuture_close_ma80_ratio',
    'bondk10yfuture_volume_ma80_ratio',
    'bondk10yfuture_close_ma100_ratio',
    'bondk10yfuture_volume_ma100_ratio',
    'bondk310yfuture_open_lastclose_ratio',
    'bondk310yfuture_high_close_ratio',
    'bondk310yfuture_low_close_ratio',
    'bondk310yfuture_close_lastclose_ratio',
    'bondk310yfuture_volume_lastvolume_ratio',
    'bondk310yfuture_close_ma5_ratio',
    'bondk310yfuture_volume_ma5_ratio',
    'bondk310yfuture_close_ma10_ratio',
    'bondk310yfuture_volume_ma10_ratio',
    'bondk310yfuture_close_ma20_ratio',
    'bondk310yfuture_volume_ma20_ratio',
    'bondk310yfuture_close_ma40_ratio',
    'bondk310yfuture_volume_ma40_ratio',
    'bondk310yfuture_close_ma60_ratio',
    'bondk310yfuture_volume_ma60_ratio',
    'bondk310yfuture_close_ma80_ratio',
    'bondk310yfuture_volume_ma80_ratio',
    'bondk310yfuture_close_ma100_ratio',
    'bondk310yfuture_volume_ma100_ratio',
]


def load_code_from_sector(sector_code):
    wics.wics_to_db()
    code_list = wics.get_code_from_sector(sector_code)
    return code_list


def load_data(code):
    update_date = datetime.now().strftime('%Y%m%d')

    ksm.load_data_from_chart(code)

    kim.load_data_from_index()
    market_dir = f'./../data/market/{update_date}'
    market_files = [f for f in os.listdir(market_dir) if f.endswith('.csv')]
    df_marketfeatures = None

    for market_file in tqdm(market_files):
        df_marketfeature = pd.read_csv(os.path.join(market_dir, market_file))
        if df_marketfeatures is None:
            df_marketfeatures = df_marketfeature
        else:
            df_marketfeatures = pd.merge(df_marketfeatures, df_marketfeature, on='date', how='left',suffixes=('', '_dup'))
    df_marketfeatures.to_csv(f'./../data/market/{update_date}/marketfeatures.csv')

def load_data_from_file(code):
    update_date = datetime.now().strftime('%Y%m%d')
    df_stockfeatures = pd.read_csv(f'./../data/stock/{update_date}/{code}.csv')
    df_marketfeatures = pd.read_csv(f'./../data/market/{update_date}/marketfeatures.csv')
    df = pd.merge(df_stockfeatures, df_marketfeatures, on='date', how='left',suffixes=('', '_dup'))
    df = df.reset_index(drop=True)

    training_data = df[COLUMNS_TRAINING_DATA].values
    return training_data

load_data('005930')
print(load_data_from_file('005930'))

