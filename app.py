import os
from flask import Flask, render_template, request, session, jsonify
import psycopg2
from dotenv import load_dotenv

# Load Environment Variables
env_path = r"C:\Users\juddu\Downloads\PAM\Staging Area\WestPoint\.env"
load_dotenv(env_path)

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT')

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "default_secret_key")

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

########################
# 1) Category Auto-Select Rules
########################
auto_select_rules = {
    "Towels":  ["Type Size", "Size", "Lbs Per Dz", "Case Qty"],
    "Hotel Pillow":  ["Type Size", "Size", "Fill Weight", "Case Qty"],
    "Sheets":  ["Product Type", "Type Size", "Size", "Case Qty"],
    # Add more categories as needed
}

def sort_and_format_options(option_list):
    """
    Reorder and combine the option items so that:
      - "Product Type" always comes first (if present).
      - "Type Size" and "Size" are combined into one entry.
      - The remaining attributes appear in a defined order.
    """
    items = {label: value for label, value in option_list}
    result = []

    # 1. Product Type always first.
    if "Product Type" in items:
        result.append(items["Product Type"])

    # 2. Combine "Type Size" and "Size"
    type_size_combined = ""
    if "Type Size" in items and "Size" in items:
        type_size_combined = f"{items['Type Size']} {items['Size']}"
    elif "Type Size" in items:
        type_size_combined = items["Type Size"]
    elif "Size" in items:
        type_size_combined = items["Size"]
    if type_size_combined:
        result.append(type_size_combined)

    # 3. Add the rest of the attributes in a defined order
    remaining_order = ["Lbs Per Dz", "Fill Weight", "Variable Weight", "Color", "Style", "Case Qty"]
    for label in remaining_order:
        if label in items:
            result.append(items[label])

    # 4. Add any additional attributes not in the predefined set
    used_labels = {
        "Product Type", "Type Size", "Size", "Lbs Per Dz",
        "Fill Weight", "Variable Weight", "Color", "Style", "Case Qty"
    }
    for label, value in option_list:
        if label not in used_labels and value not in result:
            result.append(value)

    return result
@app.route('/', methods=['GET', 'POST'])
def index():
    product_id = 112110
    collection = "Martex Atelier Zen"
    product_type = "Towels"  # for auto-select logic

    flagged_data = []
    grouped_products = []
    mapping_data = []
    uploaded_data = []   # single-SKU (template)
    all_skus_data = []   # multi-SKU (for the modal)
    old_option_mappings = []
    variant_sku = None

    product_name = None
    parent_category = None
    subcategory_name = None
    collection_name = None

    conn = get_db_connection()
    if not conn:
        flagged_data.append("⚠️ Database connection failed.")
        return render_template(
            'index.html',
            product_name=product_name,
            parent_category=parent_category,
            subcategory_name=subcategory_name,
            collection_name=collection_name,
            grouped_products=grouped_products,
            flagged_data=flagged_data,
            mapping_data=mapping_data,
            variant_sku=variant_sku,
            uploaded_data=uploaded_data,
            all_skus_data=all_skus_data,  # pass empty for now
            old_option_mappings=old_option_mappings,
            user_selections={}
        )

    try:
        cur = conn.cursor()
        # ------------------------------------------------------------------
        # (1) Fetch all SKUs for product_id -> store in grouped_products for Parent Product List
        # ------------------------------------------------------------------
        cur.execute("""
            SELECT variant_sku
            FROM onboarding.westpointoptions
            WHERE product_id = %s;
        """, (product_id,))
        all_skus = [row[0] for row in cur.fetchall()]

        if all_skus:
            grouped_products.append({
                "key": f"Product ID: {product_id}",
                "skus": all_skus
            })
            variant_sku = all_skus[0]  # The first SKU (template)
        else:
            flagged_data.append(f"⚠️ No SKUs found for Product ID {product_id}.")
            variant_sku = None
            
        

        # ------------------------------------------------------------------
        # (2) Build mapping_data for the single "template" SKU
        # ------------------------------------------------------------------
        if variant_sku:
            cur.execute("""
                SELECT product_type, type_size, size, lbs_per_dz, fill_weight,
                       variable_weight, color, style, case_qty
                FROM onboarding.westpointoptions
                WHERE variant_sku = %s;
            """, (variant_sku,))
            attribute_values = cur.fetchone()

            expected_attributes = [
                "Product Type", "Type Size", "Size", "Lbs Per Dz",
                "Fill Weight", "Variable Weight", "Color", "Style", "Case Qty"
            ]

            if attribute_values:
                for label, value in zip(expected_attributes, attribute_values):
                    if value not in (None, ""):
                        mapping_data.append({"label": label, "value": value})
            else:
                flagged_data.append(f"⚠️ No attributes found for SKU {variant_sku}.")

        # ------------------------------------------------------------------
        # (3) Automatic Pre-Selection Logic
        # ------------------------------------------------------------------
        default_selections = {}
        relevant_labels = auto_select_rules.get(product_type, [])
        for item in mapping_data:
            lbl = item["label"]
            if lbl in relevant_labels:
                default_selections[lbl] = "option1"
            else:
                default_selections[lbl] = "none"

        # Handle GET vs POST
        if request.method == 'POST':
            # 1) Capture the user-defined Option 1 / Option 2 names
            option1_name = request.form.get("option1_name", "Option 1")
            option2_name = request.form.get("option2_name", "Option 2")
           ## session["option1_name"] = option1_name
            ## session["option2_name"] = option2_name

            # 2) Capture the attribute mappings
            user_selections = {}
            for item in mapping_data:
                label = item["label"]
                form_key = f"mapping_{label}"
                user_selections[label] = request.form.get(form_key, "none")
                
            # 3) If you have custom attributes
            custom_label = request.form.get("custom_attribute_label")  # if you want
            custom_value = request.form.get("custom_attribute_value")  # etc.

            # 4) Store them or apply them
            session["option1_name"] = option1_name
            session["option2_name"] = option2_name
            session["user_selections"] = user_selections

            #session["user_selections"] = user_selections
        #else:
         #   option1_name = session.get("option1_name", "Option 1")
          #  option2_name = session.get("option2_name", "Option 2")
           # user_selections = session.get("user_selections", default_selections)

        # ------------------------------------------------------------------
        # (4) Helper functions: format_value and combine_type_and_size
        # ------------------------------------------------------------------
        def format_value(label, raw_value):
            if label == "Fill Weight":
                return f"{raw_value} oz"
            elif label == "Case Qty":
                return f"Case of {raw_value}"
            else:
                return str(raw_value)

        def combine_type_and_size(option_list):
            type_size_val = None
            size_val = None
            others = []
            for lbl, val in option_list:
                if lbl == "Type Size":
                    type_size_val = val
                elif lbl == "Size":
                    size_val = val
                else:
                    others.append(val)

            combined = []
            if type_size_val or size_val:
                if type_size_val and size_val:
                    combined.append(f"{type_size_val} {size_val}")
                elif type_size_val:
                    combined.append(type_size_val)
                else:
                    combined.append(size_val)
            combined.extend(others)
            return " | ".join(combined)

        expected_attrs = [
            "Product Type", "Type Size", "Size", "Lbs Per Dz",
            "Fill Weight", "Variable Weight", "Color", "Style", "Case Qty"
        ]

        # ------------------------------------------------------------------
        # (5) Build uploaded_data for single (template) SKU
        # ------------------------------------------------------------------
        if variant_sku:
            cur.execute("""
                SELECT product_type, type_size, size, lbs_per_dz, fill_weight,
                       variable_weight, color, style, case_qty
                FROM onboarding.westpointoptions
                WHERE variant_sku = %s;
            """, (variant_sku,))
            row = cur.fetchone()
            if row:
                # Create a dict of attributes for the single SKU
                sku_attr_dict = {}
                for lbl, val in zip(expected_attrs, row):
                    if val not in (None, ""):
                        sku_attr_dict[lbl] = val

                option1_list = []
                option2_list = []

                for lbl, val in sku_attr_dict.items():
                    selection = user_selections.get(lbl, "none")
                    fmt_val = format_value(lbl, val)
                    if selection == "option1":
                        option1_list.append((lbl, fmt_val))
                    elif selection == "option2":
                        option2_list.append((lbl, fmt_val))

                option1_display = combine_type_and_size(option1_list)
                option2_display = combine_type_and_size(option2_list)

                # Option 1 row
                uploaded_data.append({
                    "sku": variant_sku,
                    "label": option1_name,
                    "option_value_display": option1_display or "No Display Name Chosen"
                })
                # Option 2 row (only if something selected)
                if option2_display.strip():
                    uploaded_data.append({
                        "sku": variant_sku,
                        "label": option2_name,
                        "option_value_display": option2_display
                    })

        # ------------------------------------------------------------------
        # (6) Build all_skus_data for multi-SKU listing in the modal
        # ------------------------------------------------------------------
        all_skus_data = []
        if all_skus:
            for sku in all_skus:
                # fetch attributes
                cur.execute("""
                    SELECT product_type, type_size, size, lbs_per_dz, fill_weight,
                           variable_weight, color, style, case_qty
                    FROM onboarding.westpointoptions
                    WHERE variant_sku = %s;
                """, (sku,))
                row = cur.fetchone()
                if not row:
                    continue

                sku_attr_dict = {}
                for lbl, val in zip(expected_attrs, row):
                    if val not in (None, ""):
                        sku_attr_dict[lbl] = val

                # Apply user mappings
                opt1_list = []
                opt2_list = []
                for lbl, val in sku_attr_dict.items():
                    selection = user_selections.get(lbl, "none")
                    val_fmt = format_value(lbl, val)
                    if selection == "option1":
                        opt1_list.append((lbl, val_fmt))
                    elif selection == "option2":
                        opt2_list.append((lbl, val_fmt))

                opt1_disp = combine_type_and_size(opt1_list)
                opt2_disp = combine_type_and_size(opt2_list)

                # Option 1 row for this SKU
                all_skus_data.append({
                    "sku": sku,
                    "label": option1_name,
                    "option_value_display": opt1_disp or "No Display Name Chosen"
                })
                # Option 2 row if any
                if opt2_disp.strip():
                    all_skus_data.append({
                        "sku": sku,
                        "label": option2_name,
                        "option_value_display": opt2_disp
                    })

        # ------------------------------------------------------------------
        # (7) old_option_mappings
        # ------------------------------------------------------------------
        old_option_mappings = []
        if variant_sku:
            cur.execute("""
                SELECT
                    sku,
                    (option_values->0->>'option_id')          AS option_id,
                    (option_values->0->>'option_display_name') AS option_name,
                    (option_values->0->>'label')              AS option_value
                FROM h4h_import2.variants
                WHERE product_id = %s
                  AND sku = %s
                ORDER BY id ASC
                LIMIT 1;
            """, (product_id, variant_sku))
            old_rows = cur.fetchall()
            for r in old_rows:
                old_option_mappings.append({
                    "sku": r[0],
                    "option_id": r[1],
                    "option_name": r[2],
                    "option_value": r[3]
                })

        # ------------------------------------------------------------------
        # (8) fetch product name & categories
        # ------------------------------------------------------------------
        cur.execute("""
            SELECT name, categories
            FROM h4h_import2.products
            WHERE id = %s
            LIMIT 1;
        """, (product_id,))
        prod_row = cur.fetchone()
        if prod_row:
            product_name = prod_row[0] or "Unknown Product"
            categories_array = prod_row[1] if prod_row[1] else []
        else:
            product_name = "Unknown Product"
            categories_array = []

        category_names = []
        for cid in categories_array:
            cur.execute("""
                SELECT name
                FROM h4h_import2.categories
                WHERE id = %s
                LIMIT 1;
            """, (cid,))
            cat_row = cur.fetchone()
            if cat_row:
                category_names.append(cat_row[0])
            else:
                category_names.append("Unknown Category")

        if len(category_names) > 0:
            parent_category = category_names[0]
        if len(category_names) > 1:
            subcategory_name = category_names[1]
        if len(category_names) > 2:
            collection_name = category_names[2]

        cur.close()
    except Exception as e:
        flagged_data.append(f"❌ SQL Error: {e}")
        print(f"❌ SQL Error: {e}")
    finally:
        if conn:
            conn.close()

    # Render template
    return render_template(
        'index.html',
        product_name=product_name,
        parent_category=parent_category,
        subcategory_name=subcategory_name,
        collection_name=collection_name,
        flagged_data=flagged_data,
        mapping_data=mapping_data,         # single "template" SKU attributes
        variant_sku=variant_sku,
        uploaded_data=uploaded_data,       # only 1 or 2 rows for the first SKU
        all_skus_data=all_skus_data,       # multi-SKU for the pop-up
        grouped_products=grouped_products, # multi-SKU for "Parent Product List"
        old_option_mappings=old_option_mappings,
        user_selections=session.get("user_selections", {})
    )


@app.route('/map_all', methods=['POST'])
def map_all():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided."}), 400

    available_skus = data.get("available_skus", [])
    option1_mapping = data.get("option1_mapping", "")
    option1_name = data.get("option1_name", "Option 1")
    option2_mapping = data.get("option2_mapping", "")
    option2_name = data.get("option2_name", "Option 2")

    # Build JSON to update the variants in your DB as needed...
    option_values = []
    if option1_mapping.strip():
        option_values.append({
            "option_id": "1",
            "option_display_name": option1_name,
            "label": option1_mapping
        })
    if option2_mapping.strip():
        option_values.append({
            "option_id": "2",
            "option_display_name": option2_name,
            "label": option2_mapping
        })
    # Then you'd do your DB update with these option_values for each SKU in available_skus
    # ...

    return jsonify({"success": True, "message": "All SKUs mapped successfully!"})

if __name__ == '__main__':
    app.run(debug=True)
