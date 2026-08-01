import uuid
import random
import requests
import pandas as pd
import boto3
from datetime import datetime, timezone
from io import BytesIO

S3_BUCKET = 'ausmart-data-lake'
REGION    = 'ap-southeast-2'

s3 = boto3.client('s3', region_name=REGION)

def get_aud_rate():
    try:
        response = requests.get(
            'https://api.frankfurter.app/latest?from=USD&to=AUD',
            timeout=10,
            headers={'User-Agent': 'AusMart-Pipeline/1.0'}
        )
        response.raise_for_status()
        rate = response.json()['rates']['AUD']
        print(f"Live exchange rate: 1 USD = {rate} AUD")
        return rate
    except Exception as e:
        print(f"Could not fetch rate: {e}. Using 1.55")
        return 1.55

def fetch_prices():
    """
    Fetch Australian products and generate realistic price events.
    Price variations simulate real retail discount patterns.
    """
    print("Fetching prices...")

    headers = {
        'User-Agent': 'AusMart-DataPipeline/1.0 (github.com/AKASH7358/ausmart; student project)'
    }

    url = (
        'https://world.openfoodfacts.org/api/v2/search'
        '?countries_tags=en:australia'
        '&fields=code,product_name,categories_tags'
        '&page_size=100'
        '&page=1'
        '&sort_by=popularity'
    )

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"API error: {e}")
        raise

    AUD_RATE = get_aud_rate()

    category_prices = {
        'beverages':     2.50,
        'snacks':        3.50,
        'dairy':         4.00,
        'breakfast':     5.50,
        'frozen':        6.00,
        'confectionery': 3.00,
    }

    prices = []
    for item in data.get('products', []):
        if not item.get('product_name', '').strip():
            continue

        categories = item.get('categories_tags', [])
        category = 'general'
        for cat in categories:
            clean = cat.replace('en:', '').replace('-', ' ')
            if ':' not in cat:
                category = clean
                break

        base_price = round(
            category_prices.get(category, 4.50) * AUD_RATE / 1.55, 2
        )
        discount_pct = round(random.uniform(0, 25), 2)
        current_price = round(base_price * (1 - discount_pct / 100), 2)

        prices.append({
            'price_id':     str(uuid.uuid4()),
            'product_id':   item.get('code', str(uuid.uuid4())),
            'price':        current_price,
            'old_price':    base_price,
            'discount_pct': discount_pct,
            'currency':     'AUD',
            'retailer':     'ausmart',
            'fetched_at':   datetime.now(timezone.utc).isoformat()
        })

    print(f"Generated {len(prices)} price records")
    return prices


def write_to_s3(records, prefix, filename):
    if not records:
        print("No records to write.")
        return

    df = pd.DataFrame(records)
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)

    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
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
    print(f"Done. {len(prices)} price records written to S3.")