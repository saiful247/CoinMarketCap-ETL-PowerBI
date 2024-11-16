import requests
import pandas as pd
import s3fs

# Function to fetch data from CoinMarketCap API and store it in S3


def run_coinmarket_etl():
    # CoinMarketCap API Endpoint
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    # Headers with your API Key
    headers = {
        "Accepts": "application/json",
        # Replace with your actual API key
        "X-CMC_PRO_API_KEY": "Replace with your actual API key"
    }

    # Parameters for the request
    params = {
        "start": "1",       # Start at rank 1
        "limit": "100",     # Number of cryptocurrencies to retrieve
        "convert": "USD"    # Currency for the prices
    }

    # Make the API request
    response = requests.get(url, headers=headers, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        # Convert the 'data' field to a DataFrame
        df = pd.json_normalize(data['data'])

        # Extract specific columns to create the table
        df_filtered = df[[
            'name',               # Cryptocurrency name
            'quote.USD.price',    # Price in USD
            'quote.USD.percent_change_1h',   # 1h % change
            'quote.USD.percent_change_24h',  # 24h % change
            'quote.USD.percent_change_7d',     # 7d % change
            'quote.USD.market_cap',            # Market Cap
            'quote.USD.volume_24h',            # 24h Volume
            'circulating_supply'
        ]]

        # Rename columns for readability
        df_filtered.columns = ['Name', 'Price', '1h %',
                               '24h %', '7d %', 'MarketCap', 'Volume 24h', 'Supply']

        # print(df_filtered)
        df_filtered.to_csv(
            's3://Replace with your s3 bucket name/coinMarketData.csv')
    else:
        print(f"Failed to fetch data: {response.status_code}")
