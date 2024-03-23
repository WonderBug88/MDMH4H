import requests

def push_product_to_bigcommerce(product_data):
    api_url = "https://api.bigcommerce.com/stores/{store_hash}/v3/catalog/products"
    headers = {
        "X-Auth-Token": "your_bigcommerce_api_token",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    response = requests.post(api_url, json=product_data, headers=headers)
    return response.json()
