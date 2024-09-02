import os
import logging
import mysql.connector
import json
import portalocker
from datetime import datetime, timedelta
from flask import (Blueprint, session, request, render_template,
                   redirect, url_for, flash, send_from_directory, jsonify)
from app.config import Config
from db import get_db_connection
from db.curd import DataRetriever
from db.queries import get_raw_query, get_gsc_query, get_order_history_query
from .utils import find_parent_product, generate_content, get_product_details
from app.utilities.bigcommerce import find_product_by_sku

main_bp = Blueprint("main", __name__)


@main_bp.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        supplier = request.form.get('supplier')
        if not supplier:
            flash('No supplier selected', 'error')
            return redirect(url_for('main.index'))
        return redirect(url_for('main.product_management', supplier=supplier))

    schmas = [
        {
            "id": '1888_mills',
            "name": '1888 MILLS',
            "table": "product"
        },
        {
            "id": 'Thomaston Mills',
            "name": 'Thomaston Mills',
            "table": "products"
        },

        {
            "id": 'berkshire',
            "name": 'Berkshire',
            "table": "products"
        },
        {
            "id": 'downlite_import',
            "name": 'DOWNLITE',
            "table": "producs"
        },
        {
            "id": 'ganesh',
            "name": 'GANESH',
            "table": "products"
        },
        {
            "id": 'bissel',
            "name": 'Bissel',
            "table": "product"
        },
        # {
        #     "id": 'microchill',
        #     "name": 'Microchill',
        #     "table": "products"
        # }
    ]

    return render_template('index.html', brands=schmas)


@main_bp.route('/product-management', methods=['GET'])
def product_management():
    try:
        brand_id = request.args.get('supplier')

        if not brand_id:
            flash('No supplier selected.', 'error')
            return redirect(url_for('main.index'))

        # SQL query to retrieve products and their variants with pagination
        query = get_raw_query(brand_id)

        current_page = request.args.get('page', default=1, type=int)
        # Update page value based on the current page and next and previous buttons
        if request.args.get('next'):
            current_page += 1
        elif request.args.get('prev') and current_page > 1:
            current_page -= 1

        limit = 1
        offset = (current_page - 1) * limit
        brand_products = DataRetriever(schema=brand_id).query(query, limit, offset)
        if not brand_products:
            flash(f'No products found for {brand_id}', 'error')
            return redirect(url_for('main.index'))

        # Correctly format skus_string for the SQL IN clause
        variant_skus = [f"'{variant['sku']}'" for variant in brand_products[0]['variants']]
        skus_string = ', '.join(variant_skus)

        # Updated orders_query
        orders_query = get_order_history_query(skus_string)

        analytics_data_schema = DataRetriever(schema='analytics_data')
        orders_data = analytics_data_schema.query(orders_query)

        # get competitor_data from ash schema products table for variant_skus
        ash_query = f"""SELECT * FROM products WHERE part_number IN ({skus_string})"""
        competitor_data = DataRetriever(schema='ahs').query(ash_query)

        # Initialize content generation variables
        generated_description = generated_meta_title = generated_keywords = generated_product_title = generated_meta_description = 'No information available'

        # # Determine the parent product to display
        parent_product = brand_products[0] if brand_products else {}
        variants = parent_product.get('variants', [])
        parent_product_sku = variants[0]['sku'] if variants else ''
        product_category = variants[0].get('category', '') if variants else '' # As of now, category is not available for every supplier
        product_subcategories = list({variant.get('sub_category', '') for variant in variants if variant.get('sub_category')})
        parent_product_description = variants[0]['description'] if variants else 'No description available'

        if parent_product:
            # Construct product details for content generation
            product_details = {
                'name': parent_product.get('parent_product', 'No name available'),
                'description': parent_product_description,
                # Ensure this function returns a meaningful string or default
                'child_details': get_product_details(variants)
            }

            # Generate SEO-friendly content
            generated_product_title = generate_content(
                'product_title', product_details, brand=brand_id)
            generated_description = generate_content(
                'description', product_details, brand=brand_id)
            generated_meta_description = generate_content(
                'meta_description', product_details, brand=brand_id)
            generated_meta_title = generate_content(
                'meta_title', product_details, brand=brand_id)
            generated_keywords = generate_content(
                'keywords', product_details, brand=brand_id)
        else:
            flash('Parent product not found.', 'error')

        # GET GSC DATA
        gsc_data = []
        gsc_serach_value = ''
        today = datetime.now()
        last_30_days = today - timedelta(days=180)
        gsc_filter_from = last_30_days.strftime('%Y-%m-%d')
        gsc_filter_to = today.strftime('%Y-%m-%d')

        # parent_product_sku = 'HOS100CO0080' # Testing SKU
        # gsc_serach_value = '/downlite-pillows-25-75-goose-down-feather/'
        bc_product = find_product_by_sku(parent_product_sku) # BigCommerce Product
        gsc_serach_value = bc_product.get('Custom URL') if bc_product else None
        if gsc_serach_value:
            gsc_qry = get_gsc_query(gsc_serach_value, gsc_filter_from, gsc_filter_to)
            gsc_data = analytics_data_schema.query(gsc_qry)

        return render_template('product_management.html',
                            supplier=brand_id,
                            supplier_name=brand_id,
                            product=parent_product,
                            product_category=product_category,
                            product_subcategories=product_subcategories,
                            page=current_page,
                            generated_description=generated_description,
                            generated_meta_title=generated_meta_title,
                            generated_keywords=generated_keywords,
                            generated_product_title=generated_product_title,
                            generated_meta_description=generated_meta_description,
                            competitor_data=competitor_data,
                            gsc_custom_url=gsc_serach_value,
                            gsc_data=gsc_data,
                            gsc_filter_from=gsc_filter_from,
                            gsc_filter_to=gsc_filter_to,
                            orders_data=orders_data
                            )
    except Exception as e:
        logging.error(f"Error in product_management: {e}")
        flash('An error occurred while fetching product data.', 'error')
        return jsonify({"error": str(e)})

@main_bp.route('/gsc-data', methods=['GET'])
def gsc_data():
    """Filter GSC table data based on custom URL and date range."""
    search_value = request.args.get('search_value')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not search_value or not start_date or not end_date:
        return jsonify({"error": "Invalid parameters."}), 400
    
    search_value = search_value.replace(" ", "-")
    gsc_qry = get_gsc_query(search_value, start_date, end_date)
    gsc_data = DataRetriever(schema='analytics_data').query(gsc_qry)

    return jsonify(gsc_data)


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
