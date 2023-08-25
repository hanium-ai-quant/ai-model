import os
import multiprocessing
from datetime import datetime

from ai import process


# Assuming you have a way to get the most recent date saved in files
def most_recent_date(sector_code):
    # Get a list of all models
    models = os.listdir('./models')

    # Extract dates from the filenames and get the max date
    dates = [int(model.split('_')[1]) for model in models if model.endswith('_a3c_lstm_policy.mdl') and model.startswith(sector_code)]
    return max(dates) if dates else None


if __name__ == '__main__':

    # Use a Process Pool to run each sector in parallel
    sector_list = [
        'G2020',
    ]
    mode = 'update'

    if mode == 'test' or mode == 'update':

        for sector in sector_list:
            most_recent_date = most_recent_date(sector)
            old_name_policy = f'./models/{sector}_{most_recent_date}_a3c_lstm_policy.mdl'
            new_name_policy = f"./models/{sector}_{datetime.today().strftime('%Y%m%d')}_a3c_lstm_policy.mdl"
            os.rename(old_name_policy, new_name_policy)

            old_name_value = f'./models/{sector}_{most_recent_date}_a3c_lstm_value.mdl'
            new_name_value = f"./models/{sector}_{datetime.today().strftime('%Y%m%d')}_a3c_lstm_value.mdl"
            os.rename(old_name_value, new_name_value)

    # Assuming you have the process function defined somewhere above or in another module
    args_list = [(sector, mode) for sector in sector_list]
    pool = multiprocessing.Pool(processes=3)
    pool.map(process, args_list)
    pool.close()
    pool.join()
