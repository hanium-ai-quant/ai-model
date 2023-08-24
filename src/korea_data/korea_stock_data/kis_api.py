import sys
import os
import time

import pandas as pd
import json
import requests
import yaml

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append('./')

import korea_data_settings as kds

with open('config_kis.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
    APP_KEY = _cfg['APP_KEY']
    APP_SECRET = _cfg['APP_SECRET']
    ACCESS_TOKEN = ""
    CANO = _cfg['CANO']
    ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
    URL_BASE = _cfg['URL_BASE']


def save_token(access_token, access_token_expired):
    data = {
        'ACCESS_TOKEN': access_token,
        'ACCESS_TOKEN_EXPIRED': access_token_expired
    }
    with open('kis_token.json', 'w') as f:
        json.dump(data, f)


def auth():
    """인증"""
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))

    ACCESS_TOKEN = res.json()["access_token"]
    ACCESS_TOKEN_EXPIRED = res.json()["access_token_token_expired"]

    save_token(ACCESS_TOKEN, ACCESS_TOKEN_EXPIRED)


def hashkey(datas):
    """주식 주문을 위한 암호화"""
    PATH = "uapi/hashkey"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
        'content-Type': 'application/json',
        'appKey': APP_KEY,
        'appSecret': APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey


def get_access_token():
    try:
        with open('kis_token.json', 'r') as f:
            data = json.load(f)
            return data['ACCESS_TOKEN'], data['ACCESS_TOKEN_EXPIRED']
    except FileNotFoundError:
        auth()
        with open('kis_token.json', 'r') as f:
            data = json.load(f)
            return data['ACCESS_TOKEN'], data['ACCESS_TOKEN_EXPIRED']


def get_chart_price(code="005930", period=100, end_date="20201030"):
    ACCESS_TOKEN, ACCESS_TOKEN_EXPIRED = get_access_token()
    if time.strftime('%Y-%m-%d %H:%M:%S') < ACCESS_TOKEN_EXPIRED:
        ACCESS_TOKEN = ACCESS_TOKEN
    else:
        auth()
        ACCESS_TOKEN, ACCESS_TOKEN_EXPIRED = get_access_token()
    PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": "FHKST03010100"}
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": code,
        "fid_input_date_1": kds.subtract_korea_stock_date(end_date, period),
        "fid_input_date_2": end_date,
        "fid_period_div_code": "D",
        "fid_org_adj_prc": "0",
    }
    res = requests.get(URL, headers=headers, params=params)
    json_data = res.json()

    if 'output2' not in json_data:
        return None

    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    expected_keys = ['stck_bsop_date', 'stck_oprc', 'stck_hgpr', 'stck_lwpr', 'stck_clpr', 'acml_vol']

    for i in range(0, len(json_data['output2'])):
        data = json_data['output2'][i]

        # Check if all expected keys exist
        if not all(key in data for key in expected_keys):
            print(f"No data for {i}th date in 'output2' of stock code {code}")
            return None

        stck_bsop_date = data['stck_bsop_date']  # 날짜
        stck_oprc = int(data['stck_oprc'])  # 시가
        stck_hgpr = int(data['stck_hgpr'])  # 고가
        stck_lwpr = int(data['stck_lwpr'])  # 저가
        stck_clpr = int(data['stck_clpr'])  # 종가
        acml_vol = int(data['acml_vol'])  # 거래량

        if df.loc[df['date'] == stck_bsop_date].empty:
            temp_df = pd.DataFrame({
                'date': [stck_bsop_date],
                'open': [stck_oprc],
                'high': [stck_hgpr],
                'low': [stck_lwpr],
                'close': [stck_clpr],
                'volume': [acml_vol],
            })
            df = df._append(temp_df, ignore_index=True)
    return df
