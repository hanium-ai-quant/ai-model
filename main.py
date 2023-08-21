from ai import process
import multiprocessing

if __name__ == '__main__':
    # Use a Process Pool to run each sector in parallel
    sector_list = [
        'G1010',
        'G1510',
        'G2010',
        'G2020',
        'G2030',
        'G2510',
        'G2520',
        'G2530',
        'G2550',
        'G2560',
        'G3010',
        'G3020',
        'G3030',
        'G3510',
        'G3520',
        'G4010',
        'G4020',
        'G4030',
        'G4040',
        'G4050',
        'G4510',
        'G4520',
        'G4530',
        'G4540',
        'G5010',
        'G5020',
        'G5510'
        ]
    pool = multiprocessing.Pool(processes=4)
    pool.map(process, sector_list)
    pool.close()
    pool.join()
