import pandas as pd
import numpy as np
import os

# Correct the file paths using raw string literals
excel_file_path = r"C:\Users\juddu\OneDrive\Mission Critical Projects\Flask GUI\etl_scripts\Ganesh_Mills_Price_Catalog_Effective_February_152024_in_excel.xlsx"
excel_output_file_path = r"C:\Users\juddu\OneDrive\Mission Critical Projects\Flask GUI\uploads\GaneshETL1.xlsx"

# Load the Excel file with all sheets
all_sheets_data = pd.read_excel(excel_file_path, sheet_name=None, na_values=['NA', '-', ''])

all_product_details = []

# Conversion dictionary for units
unit_conversion = {
    'dozen': 12,
    'each': 1,
    'ctn': 1,
    'dz': 12,
}

# Iterate over each sheet in the workbook
for sheet_name, sheet_data in all_sheets_data.items():
    current_parent_product = ""
    current_parent_details = {}

    # Iterate over each row in the sheet
    for index, row in sheet_data.iterrows():
        item_no = str(row['Ganesh Mills Item No.']) if pd.notnull(row['Ganesh Mills Item No.']) else ""
        
        # Identify parent products
        if item_no.isupper() and not any(char.isdigit() for char in item_no):
            current_parent_product = item_no
            current_parent_details = {}  # Reset for a new parent product
            print(f"Parent product found: {item_no}")

        # Populate current_parent_details with attributes from the parent product
        elif pd.notnull(row['Short Description']) and any(attr in row['Short Description'] for attr in ['Quality:', 'Specs:', 'Color:', 'Description:']):
            attribute_name, _, attribute_value = row['Short Description'].partition(':')
            current_parent_details[attribute_name.strip()] = row['Size'] if pd.notnull(row['Size']) else ''

        # Handle child products and their details
        elif pd.notnull(row['Size']):
            dims_str = str(row['Dims LxWxH']).strip()  # Trim spaces
            # Check if the string is empty or '0', indicating missing values
            if dims_str == '' or dims_str == '0':
                dims = ['', '', '']
            else:
                dims = dims_str.split('x')
                dims = [dim.strip() for dim in dims] + [''] * (3 - len(dims))  # Ensure list has 3 elements

            product_details = {
                'Brand': 'Ganesh Mills',  # Added 'Brand' column
                'Parent Product': current_parent_product,
                'SKU': row['Ganesh Mills Item No.'],  # Changed column name to 'SKU'
                'Child Category': row['Short Description'],
                'Product Size': row['Size'],
                'Product Weight': row['Weight'],
                'Cost Price Per Unit': row['Special Price'],
                'Unit': row['Unit'],
                'Case Qty':row['Case Qty'],
                'Bale / Carton': row['Bale / Carton'],
                'Case Length': dims[0],
                'Case Width': dims[1],
                'Case Height': dims[2],
                'Case Weight': row['Wt/Ctn'],  # Changed column name to 'Case Weight'
                **current_parent_details
            }
            all_product_details.append(product_details)
            print(f"Processing child product: {item_no}")


# After processing all sheets, convert and save the list of product details to a DataFrame
all_product_details_df = pd.DataFrame(all_product_details)

# Replace missing values across the entire DataFrame
all_product_details_df.replace({None: np.nan, '': np.nan, 'NA': np.nan, '-': np.nan}, inplace=True)

# Ensure the writing to Excel is within the with block
with pd.ExcelWriter(excel_output_file_path, engine='openpyxl', mode='w') as writer:
    # Exporting to Excel format
    all_product_details_df.to_excel(writer, sheet_name='Step 1', index=False)