# Description: 한국 주식 시장 지수 데이터를 가져오는 모듈
import requests
import yaml
import src.korea_data.korea_data_settings as ks
import pandas as pd

with open('config_fsc.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
URL_BASE = _cfg['URL_BASE']
SERVICE_KEY = _cfg['SERVICE_KEY']


def get_kospi_index(period, end_date):
    PATH = "getStockMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "코스피",

    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        mkp = float(res.json()['response']['body']['items']['item'][i]['mkp'])  # 시가
        hipr = float(res.json()['response']['body']['items']['item'][i]['hipr'])  # 고가
        lopr = float(res.json()['response']['body']['items']['item'][i]['lopr'])  # 저가
        clpr = float(res.json()['response']['body']['items']['item'][i]['clpr'])  # 종가
        trqu = int(res.json()['response']['body']['items']['item'][i]['trqu'])  # 거래량
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'open': [mkp],
                'high': [hipr],
                'low': [lopr],
                'close': [clpr],
                'volume': [trqu],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df


def get_kosdaq_index(period, end_date):
    PATH = "getStockMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "코스닥",

    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        mkp = float(res.json()['response']['body']['items']['item'][i]['mkp'])  # 시가
        hipr = float(res.json()['response']['body']['items']['item'][i]['hipr'])  # 고가
        lopr = float(res.json()['response']['body']['items']['item'][i]['lopr'])  # 저가
        clpr = float(res.json()['response']['body']['items']['item'][i]['clpr'])  # 종가
        trqu = int(res.json()['response']['body']['items']['item'][i]['trqu'])  # 거래량
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'open': [mkp],
                'high': [hipr],
                'low': [lopr],
                'close': [clpr],
                'volume': [trqu],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df


def get_kospi_200_index(period, end_date):
    PATH = "getStockMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "코스피 200",

    }
    res = requests.get(URL, headers=headers, params=params)

    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        mkp = float(res.json()['response']['body']['items']['item'][i]['mkp'])  # 시가
        hipr = float(res.json()['response']['body']['items']['item'][i]['hipr'])  # 고가
        lopr = float(res.json()['response']['body']['items']['item'][i]['lopr'])  # 저가
        clpr = float(res.json()['response']['body']['items']['item'][i]['clpr'])  # 종가
        trqu = int(res.json()['response']['body']['items']['item'][i]['trqu'])  # 거래량
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'open': [mkp],
                'high': [hipr],
                'low': [lopr],
                'close': [clpr],
                'volume': [trqu],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df


def get_krx_300_index(period, end_date):
    PATH = "getStockMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "KRX 300",

    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        mkp = float(res.json()['response']['body']['items']['item'][i]['mkp'])  # 시가
        hipr = float(res.json()['response']['body']['items']['item'][i]['hipr'])  # 고가
        lopr = float(res.json()['response']['body']['items']['item'][i]['lopr'])  # 저가
        clpr = float(res.json()['response']['body']['items']['item'][i]['clpr'])  # 종가
        trqu = int(res.json()['response']['body']['items']['item'][i]['trqu'])  # 거래량
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'open': [mkp],
                'high': [hipr],
                'low': [lopr],
                'close': [clpr],
                'volume': [trqu],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df


def get_krx_bond_index(period, end_date):
    PATH = "getBondMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "KRX 채권지수",
    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(
        columns=['date', 'totBnfIdxClpr', 'nPrcIdxClpr', 'zrRinvIdxClp', 'clRinvIdxClpr', 'mrktPrcIdxClpr '])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        totBnfIdxClpr = float(res.json()['response']['body']['items']['item'][i]['totBnfIdxClpr'])  # 총수익지수 종가
        nPrcIdxClpr = float(res.json()['response']['body']['items']['item'][i]['nPrcIdxClpr'])  # 순가격지수 종가
        zrRinvIdxClpr = float(res.json()['response']['body']['items']['item'][i]['zrRinvIdxClpr'])  # 제로재투자지수 종가
        clRinvIdxClpr = float(res.json()['response']['body']['items']['item'][i]['clRinvIdxClpr'])  # 콜재투자지수 종가
        mrktPrcIdxClpr = float(res.json()['response']['body']['items']['item'][i]['mrktPrcIdxClpr'])  # 시장가격지수 종가
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'totBnfIdxClpr': [totBnfIdxClpr],
                'nPrcIdxClpr': [nPrcIdxClpr],
                'zrRinvIdxClpr': [zrRinvIdxClpr],
                'clRinvIdxClpr': [clRinvIdxClpr],
                'mrktPrcIdxClpr': [mrktPrcIdxClpr],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df


def get_bond_k10y_future_index(period, end_date):
    PATH = "getDerivationProductMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "10년국채선물지수",
    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close'])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        mkp = float(res.json()['response']['body']['items']['item'][i]['mkp'])  # 시가
        hipr = float(res.json()['response']['body']['items']['item'][i]['hipr'])  # 고가
        lopr = float(res.json()['response']['body']['items']['item'][i]['lopr'])  # 저가
        clpr = float(res.json()['response']['body']['items']['item'][i]['clpr'])  # 종가
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'open': [mkp],
                'high': [hipr],
                'low': [lopr],
                'close': [clpr],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df


def get_bond_k3_10y_future_index(period, end_date):
    PATH = "getDerivationProductMarketIndex"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
    params = {
        "serviceKey": SERVICE_KEY,
        "resultType": "json",
        "numOfRows": period,
        "pageNo": "1",
        "beginBasDt": ks.subtract_korea_stock_date(end_date, period),
        "endBasDt": end_date,
        "idxNm": "국채 3-10년 선물지수",
    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close'])

    for i in range(0, len(res.json()['response']['body']['items']['item'])):
        bas_Dt = res.json()['response']['body']['items']['item'][i]['basDt']  # 날짜
        mkp = float(res.json()['response']['body']['items']['item'][i]['mkp'])  # 시가
        hipr = float(res.json()['response']['body']['items']['item'][i]['hipr'])  # 고가
        lopr = float(res.json()['response']['body']['items']['item'][i]['lopr'])  # 저가
        clpr = float(res.json()['response']['body']['items']['item'][i]['clpr'])  # 종가
        if df.loc[df['date'] == bas_Dt].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [bas_Dt],
                'open': [mkp],
                'high': [hipr],
                'low': [lopr],
                'close': [clpr],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df
