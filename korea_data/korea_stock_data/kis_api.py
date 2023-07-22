from kis_auth import *
import korea_data.korea_data_settings as ks
import pandas as pd

# 데이터에 접근하기 위해 토큰 발급
ACCESS_TOKEN = auth()


# 종목코드를 입력하면 종목의 일별 시세를 반환
def get_chart_price(code="005930", period=100, end_date="20201030"):
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
        "fid_input_date_1": ks.subtract_korea_stock_date(end_date, period),
        "fid_input_date_2": end_date,
        "fid_period_div_code": "D",
        "fid_org_adj_prc": "0",
    }
    res = requests.get(URL, headers=headers, params=params)
    df = pd.DataFrame(columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    for i in range(0, len(res.json()['output2'])):
        stck_bsop_date = res.json()['output2'][i]['stck_bsop_date']  # 날짜
        stck_oprc = int(res.json()['output2'][i]['stck_oprc'])  # 시가
        stck_hgpr = int(res.json()['output2'][i]['stck_hgpr'])  # 고가
        stck_lwpr = int(res.json()['output2'][i]['stck_lwpr'])  # 저가
        stck_clpr = int(res.json()['output2'][i]['stck_clpr'])  # 종가
        acml_vol = int(res.json()['output2'][i]['acml_vol'])  # 거래량
        if df.loc[df['date'] == stck_bsop_date].empty:
            # Create a temporary DataFrame with the current data
            temp_df = pd.DataFrame({
                'date': [stck_bsop_date],
                'open': [stck_oprc],
                'high': [stck_hgpr],
                'low': [stck_lwpr],
                'close': [stck_clpr],
                'volume': [acml_vol],
            })

            # Append the temporary DataFrame to the main DataFrame
            df = df._append(temp_df, ignore_index=True)
    return df
