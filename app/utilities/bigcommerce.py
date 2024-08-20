import requests
from app.config import Config

# BigCommerce API credentials and store details
ACCESS_TOKEN = Config.BIG_COMMERCE_ACCESS_TOKEN
STORE_HASH = Config.BIG_COMMERCE_STORE_HASH
API_BASE = f'https://api.bigcommerce.com/stores/{STORE_HASH}/v3'

# Headers for API requests
headers = {
    'X-Auth-Token': ACCESS_TOKEN,
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

def get_category_names(category_ids):
    """Fetches category names for given category IDs."""
    category_names = []
    for category_id in category_ids:
        category_url = f'{API_BASE}/catalog/categories/{category_id}'
        response = requests.get(category_url, headers=headers)
        if response.status_code == 200:
            category_data = response.json()
            category_names.append(category_data['data']['name'])
    return category_names

def get_brand_name(brand_id):
    """Fetches brand name for a given brand ID."""
    brand_url = f'{API_BASE}/catalog/brands/{brand_id}'
    response = requests.get(brand_url, headers=headers)
    if response.status_code == 200:
        brand_data = response.json()
        return brand_data['data']['name']
    return "N/A"

def get_product_details(product):
    """Extracts and returns relevant details from a product or variant."""
    # Additional details extraction
    brand_name = get_brand_name(product.get('brand_id', 0))
    category_names = get_category_names(product.get('categories', []))

    option_details = []
    for option_value in product.get('option_values', []):
        option_detail = f"{option_value.get('option_display_name')}: {option_value.get('label')}"
        option_details.append(option_detail)

    details = {
        'Product Name': product.get('name'),
        'Brand': brand_name,
        'Categories': ", ".join(category_names),
        'Meta Title': product.get('meta_title'),
        'Meta Description': product.get('meta_description'),
        'Product Description': product.get('description'),
        'SKU': product.get('sku'),
        'Cost Price': product.get('cost_price'),
        'Price': product.get('price'),
        'Custom URL': product.get('custom_url', {}).get('url', ''),
        'Case Weight': product.get('weight'),
        'Dimensions': {
            'Case Width': product.get('width'),
            'Case Height': product.get('height'),
            'Case Depth': product.get('depth')
        },
        'Image URL': product.get('primary_image', {}).get('url_standard', ''),
        'Inventory Level': product.get('inventory_level'),
        'Variant Name': ", ".join(option_details),
        'Date Created': product.get('date_created'),
        'Date Modified': product.get('date_modified')
    }
    return details


def find_product_by_sku(sku):
        product_url = f'{API_BASE}/catalog/products?sku={sku}&include=variants,options'
        response = requests.get(product_url, headers=headers)
        data = response.json()
        products = data.get('data', [])
        if not products:
            return None
        for product in products:
            if product['sku'] == sku:
                return get_product_details(product)
            for variant in product['variants']:
                if variant['sku'] == sku:
                    parent_product_url = f"{API_BASE}/catalog/products/{product['id']}"
                    parent_response = requests.get(parent_product_url, headers=headers)
                    parent_data = parent_response.json()
                    return get_product_details(parent_data['data'])
        return None


