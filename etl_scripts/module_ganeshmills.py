import os
import pandas as pd
import numpy as np
import re

# ----------------- Configuration ----------------- #
# Correct the file paths using raw string literals
excel_input_file_path = r"C:\Users\juddu\Downloads\PAM\MDMH4H\etl_scripts\Ganesh_Mills_Price_Catalog.xlsx"
excel_output_file_path = r"C:\Users\juddu\Downloads\PAM\Staging Area\Ganesh\GaneshETL1.xlsx"

# ----------------- Extraction Phase ----------------- #
def extract_data(file_path):
    """
    Reads an Excel workbook with multiple sheets,
    distinguishes parent and child rows, and builds a DataFrame.
    """
    # Read all sheets from the Excel file with NA value replacements
    all_sheets_data = pd.read_excel(file_path, sheet_name=None, na_values=['NA', '-', ''])
    all_product_details = []
    
    # Iterate over each sheet in the workbook
    for sheet_name, sheet_data in all_sheets_data.items():
        current_parent_product = ""
        current_parent_details = {}
        # Create a list of columns excluding the identifier column
        non_item_columns = list(sheet_data.columns)
        if 'Ganesh Mills Item No.' in non_item_columns:
            non_item_columns.remove('Ganesh Mills Item No.')
        
        for index, row in sheet_data.iterrows():
            item_no = str(row['Ganesh Mills Item No.']) if pd.notnull(row['Ganesh Mills Item No.']) else ""
            
            # Identify parent products:
            # If the item is uppercase and all other columns are NA, treat it as a parent row.
            rest_are_na = all(pd.isna(row[col]) for col in non_item_columns)
            if item_no.isupper() and rest_are_na:
                current_parent_product = item_no
                current_parent_details = {}  # Reset for a new parent product
                print(f"Parent product found: {item_no}")
            
            # Update parent attributes if the row contains any parent details.
            elif pd.notnull(row.get('Short Description')) and any(attr in row['Short Description'] for attr in ['Quality:', 'Specs:', 'Color:', 'Description:']):
                attribute_name, _, _ = row['Short Description'].partition(':')
                # Store the attribute value from the 'Size' column if available.
                current_parent_details[attribute_name.strip()] = row['Size'] if pd.notnull(row['Size']) else ''
            
            # Process child rows that hold product details
            elif pd.notnull(row.get('Size')):
                dims_str = str(row.get('Dims LxWxH', '')).strip()
                if dims_str == '' or dims_str == '0':
                    dims = ['', '', '']
                else:
                    # Split dimensions by 'x' and pad to 3 elements if necessary.
                    dims = [dim.strip() for dim in dims_str.split('x')]
                    dims += [''] * (3 - len(dims))
                
                product_details = {
                    'Brand': 'Ganesh Mills',  # Static brand value
                    'Parent Product': current_parent_product,
                    'SKU': row['Ganesh Mills Item No.'],  # SKU column based on item number
                    'Child Category': row.get('Short Description'),
                    'Product Size': row.get('Size'),
                    'Product Weight': row.get('Weight'),
                    'Cost Price Per Unit': row.get('Special Price'),
                    'Unit': row.get('Unit'),
                    'Case Qty': row.get('Case Qty'),
                    'Bale / Carton': row.get('Bale / Carton'),
                    'Case Length': dims[0],
                    'Case Width': dims[1],
                    'Case Height': dims[2],
                    'Case Weight': row.get('Wt/Ctn')  # Renamed column for clarity
                }
                # Merge in any parent-level attributes
                product_details.update(current_parent_details)
                all_product_details.append(product_details)
                print(f"Processing child product: {item_no}")
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(all_product_details)
    # Replace typical missing value representations with np.nan
    df.replace({None: np.nan, '': np.nan, 'NA': np.nan, '-': np.nan}, inplace=True)
    return df

# ----------------- Transformation Phase ----------------- #
def transform_data(df):
    """
    Cleans and transforms the extracted DataFrame.
    This includes processing weights, dimensions, costs and quantities.
    """
    # -- Process 'Product Weight' --
    # Extract a numeric weight and its unit using regex
    pattern = r'(?P<Weight>\d+(\.\d+)?)\s*(?P<UOM>lb|lbs|oz|grms/pc|GSM|kg)?'
    # Ensure the Product Weight column is treated as string
    extracted = df['Product Weight'].astype(str).str.extract(pattern)
    df['Product Weight'] = pd.to_numeric(extracted['Weight'], errors='coerce')
    df['Product Weight UOM'] = extracted['UOM'].fillna('')
    
    # -- Process 'Case Weight' --
    if 'Case Weight' in df.columns:
        df['Case Weight UOM'] = 'lbs'
        # Remove any textual representation of lbs and convert to float.
        df['Case Weight'] = df['Case Weight'].astype(str).str.replace(' lbs', '').str.replace('lbs', '')
        df['Case Weight'] = pd.to_numeric(df['Case Weight'], errors='coerce')
    
    # -- Standardize Case Dimensions UOM --
    df['Case Dims UOM'] = 'In'
    
    # -- Clean and convert 'Case Qty' --
    if 'Case Qty' in df.columns:
        df['Case Qty'] = df['Case Qty'].astype(str).str.extract('(\d+)').fillna(0).astype(int)
    
    # -- Calculate Cost Per Each and Cost Per Case --
    # Define a simple unit conversion dictionary that applies to the cost (assuming Unit is one of these)
    unit_conversion_dict = {'Dozen': 12, 'Each': 1}
    if 'Cost Price Per Unit' in df.columns and 'Unit' in df.columns:
        # Calculate Cost Per Each based on the unit conversion
        df['Cost Per Each'] = df.apply(
            lambda x: (x['Cost Price Per Unit'] / unit_conversion_dict.get(x['Unit'], 1)) if pd.notnull(x['Cost Price Per Unit']) and pd.notnull(x['Unit']) else np.nan,
            axis=1
        ).round(2)
    if 'Cost Per Each' in df.columns and 'Case Qty' in df.columns:
        df['Cost Per Case'] = (df['Case Qty'] * df['Cost Per Each']).round(2)
    
    # -- Drop columns that are no longer needed --
    for col in ['Unit', 'Cost Price Per Unit']:
        if col in df.columns:
            df.drop(col, axis=1, inplace=True)
    
    # -- Optionally reorder columns for the desired output --
    desired_order = [
        'SKU', 'Parent Product', 'Product Size', 'Product Weight', 'Product Weight UOM',
        'Cost Per Each', 'Case Qty', 'Cost Per Case', 'Case Length', 'Case Width',
        'Case Height', 'Case Dims UOM', 'Case Weight', 'Case Weight UOM', 'Bale / Carton',
        'Child Category', 'Quality', 'Specs', 'Color'
    ]
    # Only include columns that are present in the DataFrame
    df = df[[col for col in desired_order if col in df.columns]]
    
    return df

# ----------------- Main Execution ----------------- #
def main():
    print("Starting extraction process...")
    df_extracted = extract_data(excel_input_file_path)
    print("Extraction complete. Extracted data shape:", df_extracted.shape)
    
    print("Starting transformation process...")
    df_transformed = transform_data(df_extracted)
    print("Transformation complete. Transformed data shape:", df_transformed.shape)
    
    # Write the transformed data to the 'Step 2' sheet in the output Excel file.
    # If the file already exists, append (replacing the sheet); if not, create a new file.
    if os.path.exists(excel_output_file_path):
        mode = 'a'  # Append mode
        if_sheet_exists = 'replace'
    else:
        mode = 'w'
        if_sheet_exists = None
    
    with pd.ExcelWriter(excel_output_file_path, engine='openpyxl', mode=mode, if_sheet_exists=if_sheet_exists) as writer:
        df_transformed.to_excel(writer, sheet_name='Step 2', index=False)
    print(f"Data processing complete. Transformed data saved to sheet 'Step 2' in {excel_output_file_path}")

if __name__ == "__main__":
    main()
