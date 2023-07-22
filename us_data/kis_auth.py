from polygon import RESTClient
client = RESTClient(api_key="uaj3YzpaSEIPHYmpG8M4W3iFpTFBAcpF")

request = client.get_daily_open_close_agg(
    "AAPL",
    "2023-02-07",
)

print(request)
