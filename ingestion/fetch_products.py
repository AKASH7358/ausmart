import uuid
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

def fetch_australian_products():
    """
    Fetch real Australian products from Open Food Facts.
    Open Food Facts explicitly encourages programmatic access
    and never blocks cloud servers.
    """
    print("Fetching Australian products from Open Food Facts...")

    # Australian retailers mapped by category
    au_retailers = {
        'beverages':      'Woolworths',
        'snacks':         'Coles',
        'dairy':          'Woolworths',
        'breakfast':      'Aldi',
        'frozen':         'Coles',
        'confectionery':  'Woolworths',
        'default':        'IGA'
    }

    headers = {
        'User-Agent': 'AusMart-DataPipeline/1.0 (github.com/AKASH7358/ausmart; student project)'
    }

    # Open Food Facts v2 search API — Australian products
    url = (
        'https://world.openfoodfacts.org/api/v2/search'
        '?countries_tags=en:australia'
        '&fields=code,product_name,categories_tags,nutriments,brands'
        '&page_size=100'
        '&page=1'
        '&sort_by=popularity'
    )

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Open Food Facts API error: {e}")
        raise

    products = []
    AUD_RATE = get_aud_rate()

    for item in data.get('products', []):
        product_name = item.get('product_name', '').strip()
        if not product_name:
            continue

        # Extract category
        categories = item.get('categories_tags', [])
        category = 'general'
        for cat in categories:
            clean = cat.replace('en:', '').replace('-', ' ')
            if clean in au_retailers:
                category = clean
                break
            elif ':' not in cat:
                category = clean
                break

        # Get retailer based on category
        retailer = au_retailers.get(category, au_retailers['default'])

        # Estimate AUD price based on category
        # Open Food Facts doesn't have prices but we can estimate
        # based on real Australian supermarket price ranges
        category_prices = {
            'beverages':     2.50,
            'snacks':        3.50,
            'dairy':         4.00,
            'breakfast':     5.50,
            'frozen':        6.00,
            'confectionery': 3.00,
        }
        base_price = category_prices.get(category, 4.50)

        products.append({
            'product_id':   item.get('code', str(uuid.uuid4())),
            'product_name': product_name[:100],  # truncate long names
            'category':     category,
            'price':        round(base_price * AUD_RATE / 1.55, 2),  # adjust for live rate
            'currency':     'AUD',
            'source':       retailer,
            'fetched_at':   datetime.now(timezone.utc).isoformat()
        })

    print(f"Fetched {len(products)} Australian products")
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
    print(f"Done. {len(products)} products written to S3.")