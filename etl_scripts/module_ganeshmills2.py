import os
import pandas as pd
import numpy as np

# Specify the file path
file_path = r"C:\Users\juddu\OneDrive\Mission Critical Projects\Flask GUI\uploads\GaneshETL.xlsx"
df = pd.read_excel(file_path, sheet_name='Step 1')

# List of columns to clean
columns_to_clean = ['Child Category', 'Quality', 'Specs', 'Color', 'Description']
# List of columns to trim
columns_to_trim = ['Child Category', 'Quality', 'Specs', 'Color', 'Description']

# Load the data from the specified sheet
df = pd.read_excel(file_path, sheet_name='Step 1')


# Regex pattern to match the weight and its UOM
pattern = r'(?P<Weight>\d+(\.\d+)?)\s*(?P<UOM>lb|lbs|oz|grms/pc|GSM|kg)?'

# Use str.extract to create a new DataFrame with the extracted data
extracted_df = df['Product Weight'].str.extract(pattern)

# Check if there are any missing values in the extracted data
if extracted_df.isnull().any().any():
    print("Warning: Some rows did not match the pattern and have NaN values")

# Assign the new columns to the original DataFrame, handling potential NaN values
df['Product Weight'] = pd.to_numeric(extracted_df['Weight'], errors='coerce')
df['Product Weight UOM'] = extracted_df['UOM'].fillna('')  # Replace NaN with empty string if needed

# Optionally, if you want to see which rows had issues:
problematic_rows = df[df['Product Weight'].isnull() | df['Product Weight UOM'].isnull()]
print("Problematic rows:", problematic_rows)

# Display the updated DataFrame
print(df)

# Step 1: Separate "Case Weight" into weight and UOM
# Ensure that 'Case Weight' column exists before applying transformation
if 'Case Weight' in df.columns:
    df['Case Weight UOM'] = 'lbs'
    df['Case Weight'] = df['Case Weight'].str.replace(' lbs', '').str.replace('lbs', '').astype(float)

# Step 2: Add "Case Dims UOM" column
df['Case Dims UOM'] = 'In'

# Step 3: Remove "EA" & "DZ" from "Case Qty" and convert to integer
# Ensure that 'Case Qty' column exists before applying transformation
if 'Case Qty' in df.columns:
    df['Case Qty'] = df['Case Qty'].astype(str).str.extract('(\d+)').fillna(0).astype(int)

# Display the updated DataFrame
print(df)

# Step 4: Create a dictionary for unit conversion and apply it
# Make sure to define 'Unit' in your DataFrame if it's part of your data
if 'Unit' in df.columns:
    unit_conversion_dict = {'Dozen': 12, 'Each': 1}
    df['Case Qty'] = df['Case Qty'].astype(str).str.extract('(\d+)')[0].astype(int) * df['Unit'].map(unit_conversion_dict)

# Step 5: Calculate "Cost Per Each" and round to 2 decimal places
# Ensure 'Cost Price Per Unit' is a column in your DataFrame
if 'Cost Price Per Unit' in df.columns and 'Unit' in df.columns:
    df['Cost Per Each'] = df.apply(lambda x: (x['Cost Price Per Unit'] / unit_conversion_dict.get(x['Unit'], 1)) if pd.notnull(x['Unit']) else np.nan, axis=1).round(2)

# Step 6: Calculate "Cost Per Case" and round to 2 decimal places
if 'Cost Per Each' in df.columns:
    df['Cost Per Case'] = (df['Case Qty'] * df['Cost Per Each']).round(2)

# Assume 'Unit' and 'Cost Price Per Unit' are to be dropped as per your process
df.drop(['Unit', 'Cost Price Per Unit'], axis=1, inplace=True)

# Reorder and rename columns as per the new requirements
column_order = ['SKU', 'Parent Product', 'Product Size', 'Product Weight', 'Product Weight UOM',
                'Cost Per Each', 'Case Qty', 'Cost Per Case', 'Case Length', 'Case Width', 
                'Case Height', 'Case Dims UOM', 'Case Weight', 'Case Weight UOM', 'Bale / Carton',
                 'Description', 'Child Category', 'Quality', 'Specs', 'Color']

# Reorder the DataFrame according to the new column order
df = df[column_order]

# Specify the file path for the output Excel file
output_file_path = r"C:\Users\juddu\OneDrive\Mission Critical Projects\Flask GUI\uploads\GaneshETL.xlsx"

# Check if the output file already exists
if os.path.exists(output_file_path):
    mode = 'a'  # Append if already exists
    if_sheet_exists = 'replace'  # Replace the sheet if it exists
else:
    mode = 'w'  # Write a new file if not exists
    if_sheet_exists = None  # This argument is not needed for new files

# Using ExcelWriter to write to a specific sheet
with pd.ExcelWriter(output_file_path, engine='openpyxl', mode=mode, if_sheet_exists=if_sheet_exists) as writer:
    df.to_excel(writer, sheet_name='Step 2', index=False)

print("Data processing complete and saved to 'Step 2' sheet in the Excel file.")