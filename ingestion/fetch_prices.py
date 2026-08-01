import uuid
import random
import requests
import pandas as pd
import boto3
from datetime import datetime, UTC
from io import BytesIO

S3_BUCKET = 'ausmart-data-lake'
REGION    = 'ap-southeast-2'

s3 = boto3.client('s3', region_name=REGION)

def get_aud_rate():
    """
    Fetch live USD to AUD exchange rate from frankfurter.app
    Free, no API key, data from European Central Bank.
    Falls back to 1.55 if API is unavailable.
    """
    try:
        response = requests.get(
            'https://api.frankfurter.app/latest?from=USD&to=AUD',
            timeout=10
        )
        response.raise_for_status()
        rate = response.json()['rates']['AUD']
        print(f"Live exchange rate: 1 USD = {rate} AUD")
        return rate
    except Exception as e:
        print(f"Could not fetch exchange rate: {e}. Using fallback 1.55")
        return 1.55

def fetch_prices():
    """
    Fetch current prices for all products.
    Simulates real-time price fluctuations — retailers change
    prices multiple times per day based on demand and competition.
    """
    print("Fetching current prices...")

    response = requests.get('https://fakestoreapi.com/products', timeout=15)
    response.raise_for_status()
    items = response.json()

    AUD_RATE = get_aud_rate()

    prices = []
    for item in items:
        base_price = round(item['price'] * AUD_RATE, 2)

        # Simulate price variation — up to 20% discount randomly
        discount_pct = round(random.uniform(0, 20), 2)
        current_price = round(base_price * (1 - discount_pct / 100), 2)

        prices.append({
            'price_id':     str(uuid.uuid4()),
            'product_id':   str(item['id']),
            'price':        current_price,
            'old_price':    base_price,
            'discount_pct': discount_pct,
            'currency':     'AUD',
            'retailer':     'ausmart',
            'fetched_at':   datetime.now(UTC).isoformat()
        })

    print(f"Fetched {len(prices)} price records")
    return prices


def write_to_s3(records: list, prefix: str, filename: str):
    if not records:
        print("No records to write.")
        return

    df = pd.DataFrame(records)
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)

    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}{filename}_{timestamp}.parquet"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )

    print(f"Written {len(records)} records to s3://{S3_BUCKET}/{s3_key}")


if __name__ == '__main__':
    prices = fetch_prices()
    write_to_s3(prices, 'raw/prices/', 'prices')
    print(f"\nDone. {len(prices)} price records written to S3.")