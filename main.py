from ai import process
import multiprocessing

if __name__ == '__main__':
    # Use a Process Pool to run each sector in parallel
    sector_list = [
        'G1510',
        'G2010',
        'G2020',
    ]
    mode = 'train'
    args_list = [(sector, mode) for sector in sector_list]
    pool = multiprocessing.Pool(processes=3)
    pool.map(process, args_list)
    pool.close()
    pool.join()
