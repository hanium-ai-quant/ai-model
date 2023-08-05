import time
import requests as rq
import pandas as pd
from tqdm import tqdm
import pymysql
import src.korea_data.korea_data_settings as ks


def wics_to_db():
    sector_code_name = [
        '에너지',
        '소재',
        '자본재',
        '상업 서비스와 공급품',
        '운송',
        '자동차와 부품',
        '내구 소비재와 의류',
        '호텔, 레스토랑, 레저 등',
        '소매(유통)',
        '교육 서비스',
        '식품과 기본 식료품 소매',
        '식품, 음료, 담배',
        '가정 용품과 개인 용품',
        '건강 관리 장비와 서비스',
        '제약과 생물 공학',
        '은행',
        '증권',
        '다각화된 금융',
        '보험',
        '부동산',
        '소프트웨어와 IT 서비스',
        '기술 하드웨어, 용품 및 기타',
        '반도체와 반도체 장비',
        '전자와 전기 제품',
        '디스플레이',
        '전기통신 서비스'
        '미디어와 엔터테인먼트',
        '유틸리티'
    ]
    sector_code = [
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
        'G5510',
    ]

    sector_code_dict = {}
    for i in range(len(sector_code)):
        sector_code_dict[sector_code[i]] = sector_code_name[i]

    data_sector = []
    biz_day = ks.latest_korea_stock_date()
    for i in tqdm(sector_code):
        url = f'''http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&dt={biz_day}&sec_cd={i}'''
        data = rq.get(url).json()
        data_pd = pd.json_normalize(data['list'])

        data_sector.append(data_pd)

        time.sleep(0.2)

    sector = pd.concat(data_sector, axis=0)
    sector = sector[['IDX_CD', 'CMP_CD', 'CMP_KOR']]
    sector['SEC_NM_KOR'] = sector['IDX_CD'].map(sector_code_dict)
    sector['기준일'] = biz_day
    sector['기준일'] = pd.to_datetime(sector['기준일'])

    con = pymysql.connect(user='root',
                          passwd='kkljjh',
                          host='localhost',
                          db='korea_data',
                          charset='utf8')

    cursor = con.cursor()

    query = f"""
        insert into sector (IDX_CD, CMP_CD, CMP_KOR, SEC_NM_KOR, 기준일)
        values (%s,%s,%s,%s,%s) as new
        on duplicate key update
        IDX_CD = new.IDX_CD, CMP_KOR = new.CMP_KOR, SEC_NM_KOR = new.SEC_NM_KOR
    """

    args = sector.values.tolist()
    cursor.executemany(query, args)
    con.commit()

    con.close()


# Define the specific IDX_CD value
def get_code_from_sector(my_sector_code):
    conn = pymysql.connect(user='root',
                           passwd='kkljjh',
                           host='localhost',
                           db='korea_data',
                           charset='utf8')
    cur = conn.cursor()
    cur.execute("SELECT MAX(기준일) FROM sector")
    latest_date = cur.fetchone()[0]

    # Modify the query to select where the date is the latest date
    query = f"SELECT IDX_CD, CMP_CD, CMP_KOR FROM sector WHERE 기준일 = '{latest_date}'"

    cur.execute(query)
    sector = cur.fetchall()
    sector = pd.DataFrame(sector, columns=['IDX_CD', 'CMP_CD', 'CMP_KOR'])
    conn.close()

    specific_idx_cd = my_sector_code  # 'G2010' for '자본재'
    # Use boolean indexing to filter the DataFrame and then get the 'CMP_CD' column
    cmp_cd_list = sector[sector['IDX_CD'] == specific_idx_cd]['CMP_CD'].tolist()
    return cmp_cd_list


