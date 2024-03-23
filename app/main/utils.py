import os
import json
import logging
from flask import redirect, url_for, flash
import openai
import tempfile
from app.config import BASE_DIR


def load_and_process_products(json_file_path):
    parent_products = {}
    with open(json_file_path, 'r') as file:
        for line in file:
            try:
                product = json.loads(line)
                parent_label = product.get('Parent Product')
                
                # Initialize parent product entry if it doesn't exist
                if parent_label not in parent_products:
                    parent_products[parent_label] = {
                        'child_products': [],
                        'description': None,  # Placeholder for a description
                        'brand': None,  # Placeholder for a brand
                        'sku': '',  # Placeholder for a SKU
                    }
                
                # Optionally, set/update the description if it's meant to be the same for all child products
                parent_products[parent_label]['description'] = product.get('Description', 'No description available')
                
                # Set/update the brand for the parent product
                # Assuming 'Brand' is the key used in your product data
                parent_products[parent_label]['brand'] = product.get('Brand', 'No brand available')
                
                # Append the current product to the child products list
                parent_products[parent_label]['child_products'].append(product)

                # Set the SKU for the parent product
                parent_products[parent_label]['sku'] = product.get('SKU', 'No SKU available')

            except json.JSONDecodeError as e:
                logging.error(f"Error decoding JSON in line: {line}, error: {str(e)}")                

    return parent_products


def load_and_validate_json(json_file_path):
    # Check if the JSON file exists
    if not os.path.exists(json_file_path):
        flash('JSON file does not exist.', 'error')
        return redirect(url_for('index')), None

    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)
    except json.JSONDecodeError:
        flash('Invalid JSON format.', 'error')
        return redirect(url_for('index')), None
    except Exception as e:
        flash(f'An error occurred while processing the JSON file: {str(e)}', 'error')
        return redirect(url_for('index')), None

    # Add additional checks here for data validation as needed

    return None, data  # No error, return data



def extract_product_info(product):
    return {
        'SKU': product.get('SKU', 'N/A'),
        'Type': product.get('Type', 'N/A'),
        'Product Size': product.get('Product Size', 'N/A'),
        'Product Weight': product.get('Product Weight', 'N/A'),
        'Product UOM': product.get('Product UOM', 'N/A'),
        'Colors': product.get('Colors', 'N/A'),
        'Case Qty': product.get('Case Qty', 'N/A'),
        'Case Weight': product.get('Case Weight', 'N/A'),
        'Case Length': product.get('Case Length', 'N/A'),
        'Case Width': product.get('Case Width', 'N/A'),
        'Case Height': product.get('Case Height', 'N/A'),
        'Case Dims UOM': product.get('Case Dims UOM', 'N/A'),
        'Case Weight UOM': product.get('Case Weight UOM', 'N/A'),
    }
def get_product_details(parent_product):
    details_list = []
    for child in parent_product.get('child_products', []):
        product_info = extract_product_info(child)
        
        # Extract logistics information within the loop
        logistics_info = {key: value for key, value in product_info.items() if key.startswith('Case')}
        
        # Combine product and logistics information into a single string
        details = ", ".join([f"{key}: {value}" for key, value in product_info.items()])
        logistics_details = ", ".join([f"{key}: {value}" for key, value in logistics_info.items()])
        
        # Append combined details to the list
        combined_details = f"Product Info: {details}. Logistics Info: {logistics_details}"
        details_list.append(combined_details)
        
    return ' '.join(details_list)



def generate_content(content_type, product_details):
    """
    Generates content based on the specified type and product details.
    
    :param content_type: The type of content to generate ('product_title', 'description', 'meta_title', 'keywords', 'meta_description').
    :param product_details: A dictionary containing details about the product.
    :return: The generated content as a string.
    """
    # Define prompts for each content type
    prompt_templates = {
        'description': "write a SEO friendly product description in HTML. AUDIENCE ; Hotels, Procurement teams and lodging facilities, Narrator: buy at Hotels4humanity, Brand: Ganesh Mills. for the following product: {name}." ,
        'meta_title': "Create an SEO-friendly meta title for the product: {name}.",
        'keywords': "Generate SEO keywords for the product, separate keywords with a comma: {name}.",
        'meta_description': "Create an SEO-friendly meta description for the product: {name}.",
        'product_title': "write a optimized product title: {name}."
    }
    
    # Select the appropriate prompt template
    prompt_template = prompt_templates.get(content_type, "")
    if not prompt_template:
        return "Content type not supported."
    
    # Format the prompt with product details
    prompt = prompt_template.format(**product_details)
    
    try:
        response = openai.Completion.create(
            model="gpt-3.5-turbo-instruct",  # Adjust the model as necessary
            prompt=prompt,
            temperature=0.7,
            max_tokens=500  # Adjust based on the expected length of the content
        )
        generated_text = response['choices'][0]['text'].strip()
        return generated_text
    except Exception as e:
        print(f"Failed to generate {content_type} due to: {e}")
        return None

def safely_write_to_file(target_file_path, data):
    dir_name, file_name = os.path.split(target_file_path)
    with tempfile.NamedTemporaryFile(dir=dir_name, delete=False) as tmp_file:
        tmp_file.write(data.encode('utf-8'))
        temp_file_path = tmp_file.name
    os.rename(temp_file_path, target_file_path)
    
def find_parent_product(json_file_path, parent_product_id):
    with open(json_file_path, 'r') as file:
        for line in file:
            product = json.loads(line)
            if product.get('Parent Product') == parent_product_id:
                return product
    return None


def read_json_file(json_file):
    """Read data from JSON file and store it in a variable"""

    with open(json_file, 'r') as file:
        data = json.load(file)
    return data


def get_compititors_data(compititor):
    """ Get data for a specific compititor.
        compititor: The name of the compititor whose data is to be retrieved
        ash, cathgro, direct etc."""
    
    compititor_json_file = os.path.join(BASE_DIR, f'scrap_data/output/{compititor}.json')
    compititor_data = read_json_file(compititor_json_file)
    return compititor_data

def search_products_by_sku(data, sku):
    """Search for products by SKU in the loaded data"""
    # part_number is the key used in the JSON data for SKU
    matched_products = [product for product in data if product.get('part_number') in sku]
    return matched_products
