import pandas as pd
import os
from os.path import dirname, join

# Correct the file paths using raw string literals
excel_file = 'GaneshMills-PriceCatalog-Effective-February-15-2024-excel.xlsx'
current_dir = dirname(__file__)
excel_file_path = join(current_dir, excel_file)
output_file_path = r'GaneshETL.csv'

# Load the Excel file with all sheets
all_sheets_data = pd.read_excel(excel_file_path, sheet_name=None)  # 'None' loads all sheets

# Initialize an empty list to store all product details across sheets
all_product_details = []

# Iterate over each sheet in the workbook
for sheet_name, sheet_data in all_sheets_data.items():
    current_parent_product = ""
    current_parent_details = {}  # Ensure this is reset for each sheet
    
    for index, row in sheet_data.iterrows():
        item_no = str(row['Ganesh Mills Item No.'])
        
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
         # Split 'Case Qty' into 'Quantity' and 'UOM'
            quantity, uom = ('', '')
            if pd.notnull(row['Case Qty']):
                case_qty_parts = str(row['Case Qty']).split(' ', 1)
                if len(case_qty_parts) == 2:
                    quantity, uom = case_qty_parts

            product_details = {
                'Parent Product': current_parent_product,
                'Ganesh Mills Item No.': row['Ganesh Mills Item No.'],
                'Short Description': row['Short Description'],
                'Size': row['Size'],
                'Weight': row['Weight'],
                'Special Price': row['Special Price'],
                'Unit': row['Unit'],
                'Quantity': quantity,
                'UOM': uom,
                'Bale / Carton': row['Bale / Carton'],
                'Case Length': dims[0],
                'Case Width': dims[1],
                'Case Height': dims[2],
                'Wt/Ctn': row['Wt/Ctn'],
                **current_parent_details
            }
            all_product_details.append(product_details)
            print(f"Processing child product: {item_no}")

# Convert and save the list of product details to a DataFrame and then to a CSV file
all_product_details_df = pd.DataFrame(all_product_details)
all_product_details_df['Quantity'] = pd.to_numeric(all_product_details_df['Quantity'], errors='coerce')
all_product_details_df.to_csv(output_file_path, index=False)
