import pandas as pd
import os

from os.path import dirname, join

# Correct the file paths to use raw string literals or double backslashes
excel_file = 'GaneshMills-PriceCatalog-Effective-February-15-2024-excel.xlsx'
current_dir = dirname(__file__)
excel_file_path = join(current_dir, excel_file)
print(excel_file_path)
output_file_path = 'GaneshETL-01.csv'

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
        
        if item_no.isupper() and not any(char.isdigit() for char in item_no):
            current_parent_product = item_no
            current_parent_details = {}  # Reset for a new parent product

        elif pd.notnull(row['Short Description']) and any(attr in row['Short Description'] for attr in ['Quality:', 'Specs:', 'Color:', 'Description:']):
            attribute_name = row['Short Description'].split(':')[0]
            attribute_value = row['Size'] if pd.notnull(row['Size']) else ''
            current_parent_details[attribute_name.strip()] = attribute_value

        elif pd.notnull(row['Size']):
            product_details = {
                'Parent Product': current_parent_product,
                'Ganesh Mills Item No.': row['Ganesh Mills Item No.'],
                'Short Description': row['Short Description'],
                'Size': row['Size'],
                'Weight': row['Weight'],
                'Special Price': row['Special Price'],
                'Unit': row['Unit'],
                'Case Qty': row['Case Qty'],
                'Bale / Carton': row['Bale / Carton'],
                'Dims LxWxH': row['Dims LxWxH'],
                'Wt/Ctn': row['Wt/Ctn'],
                **current_parent_details
            }
            all_product_details.append(product_details)
        print(f"Processing child product: {row['Ganesh Mills Item No.']}")

# Convert and save the list of product details after the loop
all_product_details_df = pd.DataFrame(all_product_details)
all_product_details_df.to_csv(output_file_path, index=False)



# Append the product details to the all_product_details DataFrame
all_product_details_df = pd.DataFrame(all_product_details)


# Output the DataFrame to a CSV file
all_product_details_df.to_csv(output_file_path, index=False)

if pd.notnull(row['Ganesh Mills Item No.']) and row['Ganesh Mills Item No.'].isupper():
    print(f"Parent product found: {row['Ganesh Mills Item No.']}")
    # Your code to handle parent products
    print(all_product_details_df.head())