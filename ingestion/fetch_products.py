# ingestion/fetch_products.py

import uuid
import requests
import pandas as pd
import boto3
from datetime import datetime, UTC
from io import BytesIO

# ── Config ─────────────────────────────────────────────────────────────────────
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

def fetch_products():
    """
    Fetch products from FakeStoreAPI — reliable, free, no auth needed.
    We enrich with Australian context (AUD prices, AU retailers).
    """
    print("Fetching products from FakeStore API...")

    response = requests.get('https://fakestoreapi.com/products', timeout=15)
    response.raise_for_status()
    items = response.json()

    # Australian retailers by category
    au_retailers = {
        "electronics":          ["JB Hi-Fi", "Harvey Norman", "Officeworks"],
        "jewelery":             ["Michael Hill", "Prouds", "Zamels"],
        "men's clothing":       ["David Jones", "Myer", "Cotton On"],
        "women's clothing":     ["David Jones", "Myer", "Witchery"],
    }

    AUD_RATE = get_aud_rate()

    products = []
    for item in items:
        category = item.get('category', 'general')
        retailers = au_retailers.get(category, ["Big W", "Kmart", "Target"])
        retailer = retailers[item['id'] % len(retailers)]

        products.append({
            'product_id':   str(item['id']),
            'product_name': item['title'],
            'category':     category,
            'price':        round(item['price'] * AUD_RATE, 2),
            'currency':     'AUD',
            'source':       retailer,
            'fetched_at':   datetime.now(UTC).isoformat()
        })

    print(f"Fetched {len(products)} products")
    return products


def write_to_s3(products: list, prefix: str):
    """Write product list to S3 as Parquet."""
    if not products:
        print("No products to write.")
        return

    df = pd.DataFrame(products)
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)

    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}products_{timestamp}.parquet"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )

    print(f"Written {len(products)} records to s3://{S3_BUCKET}/{s3_key}")
    return s3_key


if __name__ == '__main__':
    products = fetch_products()
    write_to_s3(products, 'raw/products/')
    print(f"\nDone. {len(products)} products written to S3.")