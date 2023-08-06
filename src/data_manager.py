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
    'krxbond_totBnfIdxClpr_last_ratio',
    'krxbond_nPrcIdxClpr_last_ratio',
    'krxbond_zrRinvIdxClpr_last_ratio',
    'krxbond_clRinvIdxClpr_last_ratio',
    'krxbond_mrktPrcIdxClpr_last_ratio',
    'krxbond_totBnfIdxClpr_ma5_ratio',
    'krxbond_nPrcIdxClpr_ma5_ratio',
    'krxbond_zrRinvIdxClpr_ma5_ratio',
    'krxbond_clRinvIdxClpr_ma5_ratio',
    'krxbond_mrktPrcIdxClpr_ma5_ratio',
    'krxbond_totBnfIdxClpr_ma10_ratio',
    'krxbond_nPrcIdxClpr_ma10_ratio',
    'krxbond_zrRinvIdxClpr_ma10_ratio',
    'krxbond_clRinvIdxClpr_ma10_ratio',
    'krxbond_mrktPrcIdxClpr_ma10_ratio',
    'krxbond_totBnfIdxClpr_ma20_ratio',
    'krxbond_nPrcIdxClpr_ma20_ratio',
    'krxbond_zrRinvIdxClpr_ma20_ratio',
    'krxbond_clRinvIdxClpr_ma20_ratio',
    'krxbond_mrktPrcIdxClpr_ma20_ratio',
    'krxbond_totBnfIdxClpr_ma40_ratio',
    'krxbond_nPrcIdxClpr_ma40_ratio',
    'krxbond_zrRinvIdxClpr_ma40_ratio',
    'krxbond_clRinvIdxClpr_ma40_ratio',
    'krxbond_mrktPrcIdxClpr_ma40_ratio',
    'krxbond_totBnfIdxClpr_ma60_ratio',
    'krxbond_nPrcIdxClpr_ma60_ratio',
    'krxbond_zrRinvIdxClpr_ma60_ratio',
    'krxbond_clRinvIdxClpr_ma60_ratio',
    'krxbond_mrktPrcIdxClpr_ma60_ratio',
    'krxbond_totBnfIdxClpr_ma80_ratio',
    'krxbond_nPrcIdxClpr_ma80_ratio',
    'krxbond_zrRinvIdxClpr_ma80_ratio',
    'krxbond_clRinvIdxClpr_ma80_ratio',
    'krxbond_mrktPrcIdxClpr_ma80_ratio',
    'krxbond_totBnfIdxClpr_ma100_ratio',
    'krxbond_nPrcIdxClpr_ma100_ratio',
    'krxbond_zrRinvIdxClpr_ma100_ratio',
    'krxbond_clRinvIdxClpr_ma100_ratio',
    'krxbond_mrktPrcIdxClpr_ma100_ratio',
    'bondk10yfuture_open_lastclose_ratio',
    'bondk10yfuture_high_close_ratio',
    'bondk10yfuture_low_close_ratio',
    'bondk10yfuture_close_lastclose_ratio',
    'bondk10yfuture_close_ma5_ratio',
    'bondk10yfuture_close_ma10_ratio',
    'bondk10yfuture_close_ma20_ratio',
    'bondk10yfuture_close_ma40_ratio',
    'bondk10yfuture_close_ma60_ratio',
    'bondk10yfuture_close_ma80_ratio',
    'bondk10yfuture_close_ma100_ratio',
    'bondk310yfuture_open_lastclose_ratio',
    'bondk310yfuture_high_close_ratio',
    'bondk310yfuture_low_close_ratio',
    'bondk310yfuture_close_lastclose_ratio',
    'bondk310yfuture_close_ma5_ratio',
    'bondk310yfuture_close_ma10_ratio',
    'bondk310yfuture_close_ma20_ratio',
    'bondk310yfuture_close_ma40_ratio',
    'bondk310yfuture_close_ma60_ratio',
    'bondk310yfuture_close_ma80_ratio',
    'bondk310yfuture_close_ma100_ratio',
]


def load_code_from_sector(sector_code):
    wics.wics_to_db()
    code_list = wics.get_code_from_sector(sector_code)
    return code_list


def load_data(code):
    update_date = datetime.now().strftime('%Y%m%d')

    ksm.load_data_from_chart(code)
    market_dir = f'./../data/market/{update_date}'

    kim.load_data_from_index()

    market_files = [f for f in os.listdir(market_dir) if f.endswith('.csv')]
    df_marketfeatures = None

    for market_file in tqdm(market_files):
        df_marketfeature = pd.read_csv(os.path.join(market_dir, market_file))
        prefix = os.path.splitext(market_file)[0]
        df_marketfeature.columns = [prefix + '_' + col if col != 'date' else col for col in df_marketfeature.columns]

        if df_marketfeatures is None:
            df_marketfeatures = df_marketfeature
        else:
            df_marketfeatures = pd.merge(df_marketfeatures, df_marketfeature, on='date', how='left')

    return df_marketfeatures


def load_data_from_file(code):
    update_date = datetime.now().strftime('%Y%m%d')
    df_stockfeatures = pd.read_csv(f'./../data/stock/{update_date}/{code}.csv')

    df_marketfeatures = load_data(code)

    df = pd.merge(df_stockfeatures, df_marketfeatures, on='date', how='left')
    df = df.reset_index(drop=True)
    df.to_csv(f'./../data/{update_date}_{code}.csv')
    training_data = df[COLUMNS_TRAINING_DATA].values
    return training_data


def data_manager_from_code(code):
    functions = [load_data, load_data_from_file]
    for function in tqdm(functions):
        function(code)


def data_manager_from_sector(sector_code):
    code_list = load_code_from_sector(sector_code)
    for code in tqdm(code_list):
        data_manager_from_code(code)

data_manager_from_sector('G2010')
