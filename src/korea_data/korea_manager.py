from datetime import datetime
from korea_stock_data.wics import *
from tqdm import tqdm
from korea_stock_data import korea_stock_manager as ksm

my_sector_code = 'G2010'
code_list = get_code_from_sector(my_sector_code)
update_date = datetime.now().strftime('%Y%m%d')


def load_stock_data(code):
    stock_data = pd.read_csv(f'./../../data/stock/{update_date}/{code}.csv')
    return stock_data

for code in tqdm(code_list):
    ksm.load_data_from_chart(code)
    load_stock_data(code)
