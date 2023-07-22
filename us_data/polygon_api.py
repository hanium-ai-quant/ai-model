from polygon import RESTClient
from requests
client = RESTClient(api_key="uaj3YzpaSEIPHYmpG8M4W3iFpTFBAcpF")
URL_BASE = "https://api.polygon.io"

#종목코드를 입력하면 종목의 일별 시세를 반환
def daily_open_close(stocksTicker="AAPL", date="2023-07-07") :
    PATH = f"/v1/open-close/{stocksTicker}/{date}"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type": "application/json",
               "authorization": f"Bearer {ACCESS_TOKEN}",
               "appKey": APP_KEY,
               "appSecret": APP_SECRET,
               "tr_id": "FHKST03010100"}
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd": code,
        "fid_input_date_1" : ks.subtract_korea_stock_date(end_date, period),
        "fid_input_date_2" : end_date,
        "fid_period_div_code": "D",
        "fid_org_adj_prc": "0",
    }
    res = requests.get(URL, headers=headers, params=params)


