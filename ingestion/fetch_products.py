# ingestion/fetch_products.py

import uuid
import time
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


def get_fallback_products(aud_rate):
    """Real Australian products used as fallback when APIs are down."""
    items = [
        ("Vegemite 380g",                    "spreads",            5.50,  "Woolworths"),
        ("Tim Tams Original 200g",           "biscuits",           4.00,  "Coles"),
        ("Milo 400g",                        "beverages",          6.50,  "Woolworths"),
        ("Weet-Bix 750g",                    "breakfast cereals",  4.50,  "Aldi"),
        ("Arnott's SAO Crackers",            "crackers",           3.50,  "Coles"),
        ("Bundaberg Ginger Beer 4pk",        "beverages",          9.00,  "Dan Murphy's"),
        ("Shapes BBQ 175g",                  "snacks",             3.00,  "Woolworths"),
        ("Uncle Tobys Oats 1kg",             "breakfast cereals",  5.00,  "Coles"),
        ("Sanitarium Up&Go 6pk",             "beverages",          9.50,  "Woolworths"),
        ("Smith's Chips 170g",               "snacks",             4.00,  "7-Eleven"),
        ("Cadbury Dairy Milk 200g",          "confectionery",      5.00,  "Woolworths"),
        ("Twisties 90g",                     "snacks",             2.50,  "Coles"),
        ("Barbecue Shapes 175g",             "snacks",             3.00,  "Woolworths"),
        ("Weeties 500g",                     "breakfast cereals",  4.00,  "Aldi"),
        ("Peters Vanilla Ice Cream 2L",      "frozen",             7.00,  "Coles"),
        ("Zooper Dooper 24pk",               "frozen",             5.00,  "Woolworths"),
        ("McCain Frozen Chips 1kg",          "frozen",             5.50,  "Coles"),
        ("Bega Tasty Cheese 500g",           "dairy",              8.00,  "Woolworths"),
        ("Pauls Full Cream Milk 2L",         "dairy",              3.50,  "Coles"),
        ("Yoplait Strawberry Yoghurt 6pk",   "dairy",              6.00,  "Woolworths"),
    ]

    products = []
    for i, (name, category, price_aud, retailer) in enumerate(items):
        products.append({
            'product_id':   str(i + 1),
            'product_name': name,
            'category':     category,
            'price':        round(price_aud * aud_rate / 1.55, 2),
            'currency':     'AUD',
            'source':       retailer,
            'fetched_at':   datetime.now(timezone.utc).isoformat()
        })

    print(f"Using {len(products)} fallback Australian products")
    return products


def fetch_australian_products():
    """
    Fetch real Australian products from Open Food Facts API.
    Retries 3 times across 2 different endpoints.
    Falls back to hardcoded real Australian products if API is unavailable.
    """
    print("Fetching Australian products from Open Food Facts...")

    headers = {
        'User-Agent': 'AusMart-DataPipeline/1.0 (github.com/AKASH7358/ausmart; student project)'
    }

    urls = [
        (
            'https://world.openfoodfacts.org/api/v2/search'
            '?countries_tags=en:australia'
            '&fields=code,product_name,categories_tags,brands'
            '&page_size=100&page=1&sort_by=popularity'
        ),
        (
            'https://world.openfoodfacts.org/cgi/search.pl'
            '?action=process&tagtype_0=countries&tag_contains_0=contains'
            '&tag_0=australia&fields=code,product_name,categories_tags'
            '&json=1&page_size=100'
        ),
    ]

    au_retailers = {
        'beverages':          'Woolworths',
        'snacks':             'Coles',
        'dairy':              'Woolworths',
        'breakfast cereals':  'Aldi',
        'frozen':             'Coles',
        'confectionery':      'Woolworths',
        'biscuits':           'Coles',
        'crackers':           'Woolworths',
    }

    category_prices = {
        'beverages':          2.50,
        'snacks':             3.50,
        'dairy':              4.00,
        'breakfast cereals':  5.50,
        'frozen':             6.00,
        'confectionery':      3.00,
        'biscuits':           4.00,
        'crackers':           3.50,
    }

    aud_rate = get_aud_rate()
    data = None

    for url in urls:
        for attempt in range(3):
            try:
                print(f"Attempt {attempt + 1} — {url[:60]}...")
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                if data.get('products'):
                    print(f"Got {len(data['products'])} products from API")
                    break
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
        if data and data.get('products'):
            break

    # ── API unavailable — use fallback ────────────────────────────────────────
    if not data or not data.get('products'):
        print("Open Food Facts unavailable after all retries. Using fallback data.")
        return get_fallback_products(aud_rate)

    # ── Parse API response ────────────────────────────────────────────────────
    products = []
    for item in data.get('products', []):
        product_name = item.get('product_name', '').strip()
        if not product_name:
            continue

        # Extract first meaningful category
        categories = item.get('categories_tags', [])
        category = 'general'
        for cat in categories:
            clean = cat.replace('en:', '').replace('-', ' ').strip()
            if clean and ':' not in clean:
                category = clean
                break

        retailer = au_retailers.get(category, 'IGA')
        base_price = category_prices.get(category, 4.50)

        products.append({
            'product_id':   item.get('code', str(uuid.uuid4())),
            'product_name': product_name[:100],
            'category':     category,
            'price':        round(base_price * aud_rate / 1.55, 2),
            'currency':     'AUD',
            'source':       retailer,
            'fetched_at':   datetime.now(timezone.utc).isoformat()
        })

    print(f"Parsed {len(products)} products from API response")
    return products


def write_to_s3(products, prefix):
    if not products:
        print("No products to write.")
        return

    df = pd.DataFrame(products)
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)

    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}products_{timestamp}.parquet"

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    print(f"Written {len(products)} records to s3://{S3_BUCKET}/{s3_key}")


if __name__ == '__main__':
    products = fetch_australian_products()
    write_to_s3(products, 'raw/products/')
    print(f"\nDone. {len(products)} products written to S3.")