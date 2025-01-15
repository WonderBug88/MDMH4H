import os
import psycopg2
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv(dotenv_path=os.path.join(BASE_DIR, '.env'))


# Example of accessing environment variables
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
}

API_PATH = os.getenv('BIG_COMMERCE_API_PATH')
ACCESS_TOKEN = os.getenv('BIG_COMMERCE_ACCESS_TOKEN')


HEADERS = {
    "X-Auth-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
    "Accept": "application/json"
}


def fetch_products_by_brand(brand_ids):
    """Fetch products with specific brand IDs."""
    brand_filter = ",".join(map(str, brand_ids))
    url = f"{API_PATH}/catalog/products?brand_id={brand_filter}&limit=100"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            print(f"Failed to fetch products. Status Code: {response.status_code}")
            print(response.json())
            return []
    except Exception as e:
        print(f"Error fetching products: {e}")
        return []
    
def fetch_variant_id_by_product_id(product_id, sku):
    """Retrieve the variant ID for a given SKU within a product."""
    url = f"{API_PATH}/catalog/products/{product_id}/variants"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            variants = response.json().get("data", [])
            for variant in variants:
                if variant.get("sku") == sku:
                    return variant.get("id")  # Return the variant ID
            print(f"Variant with SKU {sku} not found for product {product_id}.")
            return None
        else:
            print(f"Failed to fetch variants for product {product_id}. Status Code: {response.status_code}")
            print(response.json())
            return None
    except Exception as e:
        print(f"Error fetching variants for product {product_id}: {e}")
        return None

def fetch_product_id_by_sku(sku):
    """Retrieve the product ID for a given SKU."""
    url = f"{API_PATH}/catalog/products?sku={sku}&include=variants"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                return data[0].get("id")  # Return the product ID
            else:
                print(f"SKU {sku} not found in BigCommerce products.")
                return None
        else:
            print(f"Failed to fetch product ID for SKU {sku}. Status Code: {response.status_code}")
            print(response.json())
            return None
    except Exception as e:
        print(f"Error fetching product ID for SKU {sku}: {e}")
        return None

def fetch_variants_for_product(product_id):
    """Fetch variants for a given product."""
    url = f"{API_PATH}/catalog/products/{product_id}/variants"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            print(f"Failed to fetch variants for product {product_id}. Status Code: {response.status_code}")
            print(response.json())
            return []
    except Exception as e:
        print(f"Error fetching variants for product {product_id}: {e}")
        return []

def check_sku_in_database(sku):
    """Check if a SKU exists in the ganesh.inventory database."""
    query = "SELECT inventory FROM ganesh.inventory WHERE sku = %s ORDER BY timestamp DESC LIMIT 1;"
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, (sku,))
                result = cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        print(f"Error checking SKU in database: {e}")
        return None

def fetch_inventory_from_bigcommerce(sku):
    """Fetch current inventory for a given SKU from BigCommerce."""
    url = f"{API_PATH}/catalog/products?sku={sku}&include=variants"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                # Extract inventory for the first matching variant
                for variant in data[0].get("variants", []):
                    if variant.get("sku") == sku:
                        return variant.get("inventory_level", None)
            else:
                print(f"SKU {sku} not found on BigCommerce.")
                return None
        else:
            print(f"Failed to fetch inventory for SKU {sku}. Status Code: {response.status_code}")
            print(response.json())
            return None
    except Exception as e:
        print(f"Error fetching inventory for SKU {sku}: {e}")
        return None

def adjust_inventory(sku, quantity):
    """Adjust inventory for a given SKU."""
    url = f"{API_PATH}/inventory/adjustments/absolute"
    payload = {
        "reason": "Weekly update.",  # Reason for the adjustment
        "items": [
            {
        "sku": sku,
        "quantity": quantity,
        "location_id": 1,  #Default location ID
            }
        ]
    }
    try:
        # Change the request method to PUT
        response = requests.put(url, headers=HEADERS, data=json.dumps(payload))
        if response.status_code == 200:
            print(f"Inventory for SKU {sku} updated to {quantity}.")
        else:
            print(f"Failed to adjust inventory for SKU {sku}. Status Code: {response.status_code}")
            print(response.json())
    except Exception as e:
        print(f"Error adjusting inventory for SKU {sku}: {e}")
        
def log_inventory_adjustment(sku, quantity, success=True):
    """Log inventory adjustments to a local file or database."""
    with open("inventory_adjustments.log", "a") as log_file:
        status = "SUCCESS" if success else "FAILED"
        log_file.write(f"{sku},{quantity},{status},{datetime.now()}\n")

def big_api_main():
    # Step 1: Fetch products with brand_id 3020 and 3026
    brand_ids_to_update = [3020, 3026]
    products = fetch_products_by_brand(brand_ids_to_update)
    
    if not products:
        print("No products found for the specified brand IDs.")
        return

    # Step 2: Fetch variants and process SKUs
    for product in products:
        product_id = product["id"]
        variants = fetch_variants_for_product(product_id)
        
        for variant in variants:
            sku = variant.get("sku")
            variant_id = variant.get("id")  # Always fetch the variant ID
            purchasing_disabled = variant.get("purchasing_disabled", False)
            
            if not sku:
                continue  # Skip if SKU is missing
            
            if purchasing_disabled:
                print(f"SKU {sku} is purchasing_disabled. Skipping inventory update.")
                continue  # Skip SKUs that are not purchasable
            
            # Step 3: Check SKU in the database
            database_inventory = check_sku_in_database(sku)
            if database_inventory is None:
                print(f"SKU {sku} not found in database. Setting inventory to 1.")
                database_inventory = 1
            
            # Step 4: Adjust inventory
            adjust_inventory(sku, database_inventory)  # Use SKU for updates


if __name__ == "__main__":
    big_api_main()