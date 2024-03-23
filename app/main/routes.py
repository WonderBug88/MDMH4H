import os
import logging
import mysql.connector
import subprocess
import json
import sys
import portalocker
from app.config import Config
from db import get_db_connection
from flask import (Blueprint, session, request, render_template,
                   redirect, url_for, flash, send_from_directory, jsonify)
from werkzeug.utils import secure_filename
from app.utilities.helpers import allowed_file
from .utils import (load_and_process_products, get_product_details,
                    generate_content, find_parent_product,
                    search_products_by_sku, get_compititors_data)

main_bp = Blueprint("main", __name__)

# Load Competitor Data at the beginning of the file so don't have to load/read file it every time
competitor_data = get_compititors_data('ahs') # @TODO Change the supplier name from db or from the user input


@main_bp.route('/', methods=['GET', 'POST'])
def index():
    SUPPLIERS = [
        "Choose Supplier", "GANESH MILLS", "WESTPOINT HOSPITALITY", "THOMASTON MILLS",
        "HOSPITALITY 1 SOURCE", "DOWNLITE", "1888 MILLS", "BERKSHIRE HOSPITALITY",
        "HOLLYWOOD BED FRAME", "CSL", "KARTRI", "FORBES", "SICO", "BISSEL", "HAPCO",
        "JS FIBER", "KTX", "PACIFIC COAST", "GLARO", "CONAIR", "ESSENDENT"
    ]
    if request.method == 'POST':
        file = request.files.get('file')
        supplier = request.form.get('supplier')
        # Check if the supplier is not selected or if it's the default choice
        if supplier not in SUPPLIERS or supplier == "Choose Supplier":
            flash('Invalid supplier selected', 'error')
            return redirect(url_for('main.index'))
        # Check for file presence and validation
        if not file or file.filename == '':
            flash('No file selected', 'error')
            return redirect(url_for('index'))
        if not allowed_file(file.filename):
            flash('File type not allowed', 'error')
            return redirect(url_for('main.index'))
        filename = secure_filename(file.filename)
        # filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        filepath = os.path.join(Config.UPLOAD_FOLDER, filename)
        file.save(filepath)
        # Process the file here based on the supplier
        flash('File uploaded successfully', 'success')
        # Redirect or render another page to show the results
        return redirect(url_for('main.product_management', supplier=supplier, filepath=filepath))
    return render_template('index.html', suppliers=SUPPLIERS)


@main_bp.route('/main_index', methods=['GET', 'POST'])
def main_index():
    if request.method == 'POST':
        file = request.files.get('file')
        supplier = request.form.get('supplier')
        if not file or file.filename == '' or supplier not in SUPPLIERS:
            flash('No file selected or invalid supplier', 'error')
            return redirect(url_for('index'))
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(Config.UPLOAD_FOLDER, filename)
            file.save(filepath)
            # Redirect to product management with supplier and filepath parameters
            return redirect(url_for('main_bp.product_management', supplier=supplier, filepath=filepath))
    return render_template('index.html', suppliers=SUPPLIERS)


@main_bp.route('/secure-data', methods=['GET'])
def secure_data():
    json_file_path = os.path.join(
        Config.UPLOAD_FOLDER, 'processed_ganesh_mills_data.json')

    try:
        with open(json_file_path, 'r') as file:
            # Lock the file for exclusive access
            portalocker.lock(file, portalocker.LOCK_EX)

            # Read and load the JSON data
            data = json.load(file)

            # Unlock the file
            portalocker.unlock(file)
    except IOError as e:
        return jsonify({"error": "Unable to access the data file."}), 500
    except json.JSONDecodeError as e:
        return jsonify({"error": "Invalid JSON format."}), 500

    # Return the data as a JSON response
    return jsonify(data)


@main_bp.route('/inventory')
def inventory_view():
    # Placeholder inventory data
    inventory = {
        'inventory': 100,  # Example inventory count
        # Add more fields as needed for your placeholder data
    }

    # Pass the placeholder inventory data to the template
    return render_template('inventory_template.html', inventory=inventory)


@main_bp.route('/view_image/<sku>')
def view_image(sku):
    # Logic to view similar products
    pass


@main_bp.route('/view_similar/<sku>')
def view_similar(sku):
    # Logic to view similar products
    pass


@main_bp.route('/edit_product/<sku>')
def edit_product(sku):
    # Logic to edit product
    pass


@main_bp.route('/toggle_product_visibility/<sku>')
def toggle_product_visibility(sku):
    # Logic to toggle product visibility
    pass


@main_bp.route('/select_supplier', methods=['POST'])
def select_supplier():
    # Capture the supplier from the form submission
    supplier_name = request.form.get('supplier')
    if supplier_name:
        session['supplier'] = supplier_name
        print('Session supplier set:', session['supplier'])  # Debug print
        return redirect(url_for('some_next_page'))
    else:
        # Handle the case where no supplier was selected
        return 'No supplier selected', 400


# Define a route to serve static files
@main_bp.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)


@main_bp.route('/select_product', methods=['POST'])
def select_product():
    parent_product_id = request.form['parent_product_selection'] or 'default_id'
    session['parent_product_id'] = parent_product_id
    # Ensure the ID matches one of the JSON file entries
    return redirect(url_for('logistics'))


@main_bp.route('/product-management', methods=['GET'])
def product_management():
    supplier = session.get('supplier', 'Default Supplier for Testing')
    filepath = request.args.get('filepath', default=None)
    selected_parent_label = request.args.get('parent_product_name')
    print(f"Selected Parent Label: {selected_parent_label}")

    # Load and process the JSON file
    json_file_path = os.path.join(
        Config.UPLOAD_FOLDER, 'processed_ganesh_mills_data.json')
    if not os.path.exists(json_file_path):
        flash('Processed JSON file not found', 'error')
        return redirect(url_for('main.index'))

    parent_products = load_and_process_products(json_file_path)
    if not parent_products:
        flash('No parent products found in the JSON file', 'error')
        return redirect(url_for('main.index'))

    # Initialize content generation variables
    generated_description = generated_meta_title = generated_keywords = generated_product_title = generated_meta_description = 'No information available'

    # Determine the parent product to display
    parent_product = parent_products.get(selected_parent_label)
    if parent_product:
        # Assuming each parent product has an 'id' field
        parent_product_id = parent_product.get('id', 'default_id')
        session['parent_product_id'] = parent_product_id
        print(
            f"Set parent_product_id in session: {session['parent_product_id']}")
    else:
        print('Parent product not found for the given label.')

    if parent_product:
        # Construct product details for content generation
        product_details = {
            'name': selected_parent_label,  # Assuming this is a valid name or identifier
            'description': parent_product.get('description', 'No description available'),
            # Ensure this function returns a meaningful string or default
            'child_details': get_product_details(parent_product)
        }

        # Generate SEO-friendly content
        generated_product_title = generate_content(
            'product_title', product_details)
        generated_description = generate_content(
            'description', product_details)
        generated_meta_description = generate_content(
            'meta_description', product_details)
        generated_meta_title = generate_content('meta_title', product_details)
        generated_keywords = generate_content('keywords', product_details)
    else:
        flash('Parent product not found.', 'error')

    # ETL Script Logic
    etl_scripts_path = Config.ETL_SCRIPTS_PATH
    etl_scripts = {
        "GANESH MILLS": ["module_ganeshmills.py", "module_ganeshmills2.py", "module_ganeshmills3.py"],
    }

    if supplier in etl_scripts and filepath:
        for script_path in etl_scripts[supplier]:
            full_script_path = os.path.join(etl_scripts_path, script_path)
            try:
                result = subprocess.run(
                    [sys.executable, full_script_path, filepath], capture_output=True, text=True, check=True)
                # Ensure the script is executable
                os.chmod(full_script_path, 0o755)
                logging.info(f'Script Output: {result.stdout}')
                if result.returncode != 0:
                    flash(
                        f'An error occurred while processing ETL script: {result.stderr}', 'error')
                    return redirect(url_for('main.index'))
            except subprocess.CalledProcessError as e:
                logging.error(
                    f'ETL script failed with return code {e.returncode}: {e.stderr}')
                flash(f'ETL script failed: {e.stderr}', 'error')
                return redirect(url_for('main.index'))
            except Exception as e:
                logging.error(f'Error executing ETL script: {str(e)}')
                flash(f'Error executing ETL script: {str(e)}', 'error')
                return redirect(url_for('main.index'))
    # Determine the parent product to display, incorporating previous and next navigation
    # Default to first product; could enhance to find index of selected_parent_label
    current_index = 0
    if selected_parent_label and selected_parent_label in parent_products:
        current_parent_name = selected_parent_label
    else:
        parent_product_names = list(parent_products.keys())
        current_index = int(request.args.get('index', 0))
        current_parent_name = parent_product_names[current_index]

        next_index = (current_index + 1) % len(parent_product_names)
        prev_index = (current_index - 1) % len(parent_product_names)

    # Example function to format product details into a descriptive string

    parent_product = parent_products[current_parent_name]

    if parent_product:
        # Details for generating content
        product_details = {
            'name': current_parent_name,
            'description': parent_product.get('description', 'No description available'),
            'child_details': get_product_details(parent_product),
        }

    # Generate SEO-friendly content
        generated_description = generate_content(
            'description', product_details)
        generated_meta_description = generate_content(
            'meta_description', product_details)
        generated_meta_title = generate_content('meta_title', product_details)
        generated_keywords = generate_content('keywords', product_details)
        generated_product_title = generate_content(
            'product_title', product_details)

    else:
        flash('Parent product not found.', 'error')
        generated_description, generated_meta_title, generated_keywords, generated_product_title, generated_meta_description = None, None, None

    # Get competitor data
    # get skus from the parent product child products
    skus_list = [child_product['SKU'] for child_product in parent_product.get('child_products', [])]
    matched_products = search_products_by_sku(
        competitor_data, skus_list)
    return render_template('product_management.html',
                           supplier=session.get(
                               'supplier', 'Default Supplier for Testing'),
                           parent_product=parent_product,
                           parent_products=parent_products,
                           selected_parent_label=selected_parent_label,
                           # Pass generated description to the template
                           generated_description=generated_description,
                           # Pass generated meta title to the template
                           generated_meta_title=generated_meta_title,
                           # Pass generated keywords to the template
                           generated_keywords=generated_keywords,
                           # Pass generated description to the template
                           generated_product_title=generated_product_title,
                           # Pass generated description to the template
                           generated_meta_description=generated_meta_description,
                           next_index=next_index,
                           prev_index=prev_index,
                           current_parent_name=current_parent_name,
                           filepath=filepath,
                           competitor_data=matched_products
                           )


@main_bp.route('/logistics', methods=['GET'])
def logistics():
    if 'parent_product_id' not in session:
        flash('No parent product ID found.', 'error')
        return redirect(url_for('index'))

    parent_product_id = session['parent_product_id']
    logging.debug(
        f"Retrieved parent_product_id from session: {parent_product_id}")

    parent_product_id = session['parent_product_id']
    json_file_path = os.path.join(
        Config.UPLOAD_FOLDER, 'processed_ganesh_mills_data.json')
    logging.debug(f"JSON file path: {json_file_path}")

    parent_product = find_parent_product(json_file_path, parent_product_id)
    if not parent_product:
        flash('Parent product not found in the JSON file', 'error')
        return redirect(url_for('index'))

    # Process parent_product for logistics...
    return render_template('logistics.html', parent_product=parent_product)


@main_bp.route('/updateProductDetails', methods=['POST'])
def update_product_details():
    data = request.json
    sku = data['sku']
    field = data['field']
    newValue = data['newValue']

    # Load the current JSON data
    with open('path_to_your_json_file.json', 'r+') as file:
        products = json.load(file)

        # Update the specified product's detail
        for product in products:
            if product['SKU'] == sku:
                product[field] = newValue
                break

        # Write the changes back to the file
        file.seek(0)  # Reset file pointer to the beginning
        json.dump(products, file, indent=4)
        file.truncate()  # Remove remaining part of the old content

    # Optionally, update MySQL database here or trigger an update

    return jsonify(success=True)


@main_bp.route('/update-database-from-json')
def update_database_from_json():
    # Path to your JSON file (adjust as necessary)
    json_file_path = os.path.join(
        Config.UPLOAD_FOLDER, 'processed_ganesh_mills_data.json')

    # Establish database connection
    connection = get_db_connection()
    cursor = connection.cursor()

    # Try to open and read the JSON file
    try:
        with open(json_file_path, 'r') as file:
            data = json.load(file)  # Assuming data is a list of dictionaries
    except Exception as e:
        flash(f"Error opening JSON file: {e}", 'error')
        return redirect(url_for('index'))

    # Iterate through each item in the JSON file and insert it into the database
    for product in data:
        try:
            # Adjust the SQL query and values according to your database schema
            query = """
            INSERT INTO ganesh_mills
            (sku, brand, category, parent_product, product_size, product_weight, cost_per_each, case_qty,
            cost_per_case, case_length, case_width, case_height, case_dims_uom, case_weight, case_weight_uom,
            bale_carton, type, material_composition, design_feature, color_design, size_design, duplicate_sku,
            special_attribute, Description, Product Weight UOM)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                product.get('SKU'), product.get('Brand'), product.get(
                    'Category'), product.get('Parent Product'),
                product.get('Product Size'), product.get('Product Weight'), product.get(
                    'Cost Per Each'), product.get('Case Qty'),
                product.get('Cost Per Case'), product.get('Case Length'), product.get(
                    'Case Width'), product.get('Case Height'),
                product.get('Case Dims UOM'), product.get(
                    'Case Weight'), product.get('Case Weight UOM'),
                product.get('Bale / Carton'), product.get('Type'), product.get(
                    'Material/Composition'), product.get('Design/Feature'),
                product.get('Color Design'), product.get(
                    'Size Design'), product.get('Duplicate Sku'),
                product.get('Special Attribute'), product.get(
                    'Description'), product.get('Product Weight UOM')
            )
            cursor.execute(query, values)
        except mysql.connector.Error as error:
            flash(f"Failed to insert data into database: {error}", 'error')
            connection.rollback()  # Rollback in case of error
            cursor.close()
            connection.close()
            return redirect(url_for('index'))

    # Commit the changes if all iterations are successful
    connection.commit()

    # Close cursor and connection
    cursor.close()
    connection.close()

    flash('Database successfully updated from JSON file.', 'success')
    return redirect(url_for('index'))


@main_bp.route('/parent-product')
def parent_product():
    """Fetch brand names and categories from the database and render them to the product management page."""
    data = {'brands': [], 'parent_products': [], 'categories': []}
    try:
        connection = get_db_connection()
        cursor = connection.cursor(dictionary=True)

        cursor.execute("SELECT brand_name FROM brand_name")
        data['brands'] = cursor.fetchall()

        cursor.execute("SELECT parent_products_name FROM parent_products")
        data['parent_products'] = cursor.fetchall()

        cursor.execute("SELECT category_name FROM product_categories")
        data['categories'] = cursor.fetchall()
    except mysql.connector.Error as error:
        print(f"Failed to read data from MySQL table: {error}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
    return render_template('product_management.html', data=data)
