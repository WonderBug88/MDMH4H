import os
import json
import openai
import tempfile


def extract_product_info(product):
    return {
        'SKU': product.get('sku', 'N/A'),
        'Type': product.get('type', 'N/A'),
        'Product Size': product.get('size', 'N/A'),
        'Product Weight': product.get('weight', 'N/A'),
        'Product UOM': product.get('Product UOM', 'N/A'),
        'Colors': product.get('color', 'N/A'),
        'Case Qty': product.get('case_qty', 'N/A'),
        'Case Weight': product.get('case_weight', 'N/A'),
        'Case Length': product.get('case_height', 'N/A'),
        'Case Width': product.get('case_width', 'N/A'),
        'Case Height': product.get('case_height', 'N/A'),
        'Case Dims UOM': product.get('Case Dims UOM', 'N/A'),
        'Case Weight UOM': product.get('Case Weight UOM', 'N/A'),
    }


def get_product_details(variants):
    details_list = []
    for child in variants:
        product_info = extract_product_info(child)

        # Extract logistics information within the loop
        logistics_info = {key: value for key,
                          value in product_info.items() if key.startswith('case')}
        # Combine product and logistics information into a single string
        details = ", ".join(
            [f"{key}: {value}" for key, value in product_info.items()])
        logistics_details = ", ".join(
            [f"{key}: {value}" for key, value in logistics_info.items()])

        # Append combined details to the list
        combined_details = f"Product Info: {details}. Logistics Info: {logistics_details}"
        details_list.append(combined_details)

    return ' '.join(details_list)


def generate_content(content_type, product_details, brand="Ganesh Mills"):
    """
    Generates content based on the specified type and product details.

    :param content_type: The type of content to generate ('product_title', 'description', 'meta_title', 'keywords', 'meta_description').
    :param product_details: A dictionary containing details about the product.
    :return: The generated content as a string.
    """
    # Define prompts for each content type
    prompt_templates = {
        'description': "write a SEO friendly product description in HTML. AUDIENCE ; Hotels, Procurement teams and lodging facilities, Narrator: buy at Hotels4humanity, Brand: "+brand+". for the following product: {name}.",
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


def search_products_by_sku(data, sku):
    """Search for products by SKU in the loaded data"""
    # part_number is the key used in the JSON data for SKU
    matched_products = [
        product for product in data if product.get('part_number') in sku]
    return matched_products
