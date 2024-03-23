import pandas as pd
import json
import os
from openpyxl import load_workbook

# Assuming df is your DataFrame
file_path = r"C:\Users\juddu\OneDrive\Mission Critical Projects\Flask GUI\uploads\GaneshETL.xlsx"
df = pd.read_excel(file_path, sheet_name='Step 2')
# List of columns to clean
columns_to_clean = ['Child Category', 'Quality', 'Specs', 'Color', 'Description']
# List of columns to trim
columns_to_trim = ['Child Category', 'Quality', 'Specs', 'Color', 'Description']

# Step 1: Fill NaN values with empty strings
df[columns_to_clean] = df[columns_to_clean].fillna('')
# Apply trimming
df[columns_to_trim] = df[columns_to_trim].applymap(lambda x: x.strip() if isinstance(x, str) else x)


# Step 2: Convert columns to strings (if they're not already)
df[columns_to_clean] = df[columns_to_clean].astype(str)

# Product Type Keywords (partial list for demonstration; should be completed with all relevant keywords)
type_keywords = ['washcloth', 'hand towel', 'bath towel', 'bath sheet', 'pool towel', 'spa', 'sports', 'tea towel','towels', 
                                                 'kitchen towel', 'neck towel', 'fingertip towel','standard', 'queen', 'king', 'twin', 'pillow case', 
                                                 'fitted', 'flat','bathmat', 'bath mat', 'full', 'full xl', 'std', 'vanilla', 'washcloth', 'hand towel', 
                                                 'bath towel', 'bath sheet/ pool towel', 'washcloth square', 'washcloth (square hemmed)', 'bath sheet / pool towel',
                                                  'bath towel/ pool towel', 'bath sheet/ pool towel * new', 'bath sheet/ pool towel *new', 'spa hand towel',	
                                                  'sports neck towel hemmed', 'tea towel/ hand towel', 'kimono', 'shawl collar', 'napkins', 'tablecloth', 'napkin', 
                                                  'standard', 'queen', 'king', 'twin', 'king', 'twin xl', 'twin small', 'pillow case - standard', 'pillow case - queen', 	
                                                  'pillow case - king', 'twin flat', 'full flat', 'queen flat', 'king flat', 'twin fitted', 'full fitted',	
                                                  'queen fitted', 'king fitted', 'hospital fitted', 'twin xl flat', 'full xl flat', 'queen xl flat', 'king xl flat', 
                                                  'queen xl', 'king xl', 'pillow sham','dish cloths','dishcloth','fitted sheets','flat sheets - 1" top hem & 1" bottom hem','flat sheets - 2" top and bottom hem','flat sheets - 2" top hem & 1" bottom hem','flat sheets - 2" top hem and 2" bottom hem','flat sheets - 3" top and bottom hem','flat sheets - 3" top hem and 1" bottom hem','flat sheets - 3" top hem and 2" bottom hem','flat sheets 2" top hem & 1" bottom hem','flat sheets 4" top hem & 1/2" 3 sides hem','full','full terry','kimono','king','kitchen towel','kitchen towels','lapkins','pillow case','pillow case - 2" hem','pillow case - 3" hem','pillow case 2" hem','pillow case 4" hem','pillow case envelop style - 3" hem','placemats ','potholder','potholder pocket','queen','twin','dish cloths','dishcloth','fitted sheets','flat sheets - 1" top hem & 1" bottom hem','flat sheets - 2" top and bottom hem','flat sheets - 2" top hem & 1" bottom hem','flat sheets - 2" top hem and 2" bottom hem','flat sheets - 3" top and bottom hem','flat sheets - 3" top hem and 1" bottom hem','flat sheets - 3" top hem and 2" bottom hem','flat sheets 2" top hem & 1" bottom hem','flat sheets 4" top hem & 1/2" 3 sides hem','full','full terry','kimono','king','kitchen towel','kitchen towels','lapkins','pillow case','pillow case - 2" hem','pillow case - 3" hem','pillow case 2" hem','pillow case 4" hem','pillow case envelop style - 3" hem','placemats ','potholder','potholder pocket','queen','twin',]
    
                                                  
    # Material/Composition Keywords (partial list for demonstration)
material_composition_keywords = ['100% cotton','70% ringspun cotton/ 30% polyester', '100% ringspun cotton','86% cotton / 14% polyester with 100% cotton loops','86% ringspun cotton/ 14% polyester; 100% cotton ringspun loops', '100% ringspun cotton 2 ply yarns',
                                                   '100% zero twist cotton','100% 2 ply combed cotton','100% super long staple combed cotton',
                                                   '100% zero twist miasma cotton','86% cotton/ 14% polyester ribbed towels with 100% ringspun loops & yarn dyed pin stripe',
                                                   '90% cotton/ 10% polyester horizontal stripe pool towel','100% 2 ply ringspun cotton white with vat dyed yarn stripe',
                                                   '75% cotton/ 25% polyester 2x2 vat dyed yarn cabana stripe towel','100% 2 ply ringspun cotton yarn vat dyed 2x2 cabana',
                                                   '100% cotton economy classic 10s dyed','100% ringspun terry towel reactive dyed','100% ringspun full terry hemmed vat dyed',
                                                   '100% ringspun cotton dobby hemmed','100% ringspun cotton terry lounge cover','100% cotton jacquard 6 design assortment',
                                                   '100% cotton yarn dyed 2x2 cabana stripe assorted','100% cotton yarn dyed terry towels 2x2 cabana multicolor stripe',
                                                   '100% cotton terry','100% cotton 10s','100% ringspun cotton terry cloth with back pique design','100% ringspun cotton vat dyed',
                                                   '100% polyester microfiber','100% ringspun cotton dyed','100% ringspun cotton white','sheared ringspun cotton white',
                                                   '100% ringspun cotton honeycomb outer layer waffle with inside lining terry',
                                                   '100% ringspun cotton vat yarn dyed checked','78% polyester 22% polyamide micro fiber', '100% spun 2 ply mjs polyester','55% cotton / 45% polyester',
                                                   '100% ringspun cotton vat yarn dyed checks','100% cotton w/ silicone dots','100% cotton full terry bar mops',
                                                   '100% 2 ply ringspun cotton','100% combed cotton','100% polyester shell hypo-allergenic siliconized fiberfill',\
                                                   '100% cotton cambric t233 shell','100% cotton shell','t180 55% cotton/ 45% polyester pillow protector/ pillow sham',
                                                   '100% polyester waterproof microfiber pillow protector','100% polypropylene non-woven fabrics (80 gsm)',
                                                   'top: 100% micro polyester jersey laminated; bottom: 100% polyester jersey',
                                                   'top: 50/50 cotton/polyester t140 ; bottom: 100% polyester; filling: 100 gsm polyester; skirt: 100% knit polyester',
                                                   'top: 100% microfiber polyester quilted mattress encasement laminated with pu coated with polyester fabric',
                                                   'top: 100% micro polyester quilted mattress encasement laminated with pu coated with polyester fabri',
                                                   'top: 100% cotton ; bottom: vinyl- filling: 200 gsm 100% polyester','100% micro polyester 95 gsm antistatic',
                                                   '100% micro polyester 120 gsm antistatic','55% cotton/ 45% polyester t180 mercerized','60% cotton/ 40% polyester t200 mercerized',
                                                   '52/48 polyester modal t210 bed linen','60/40 cotton/polyester plain weave satin bed linen t250 mercerized',
                                                   '60% cotton/ 40% polyester 0.5 cm sateen stripe t250 mercerized',
                                                   '65% cotton / 35% polyester bleached item #300 mercerized','80% cotton/ 20% polyester bed linen t300',
                                                   '100% combed cotton sateen bed linen t300 mercerized','65% cotton/ 35% polyester satin bed linen t300 mercerized',
                                                   '100% cotton sateen bed linen t400','100% cotton fitted sheet','55% polyester/ 45% cotton knitted fitted bedsheet',
                                                   '100% cotton hotel thermal blankets','100% polyester antipilling','100% polyester diamond check quilted bed topper',
                                                   '55/45 cotton/polyester t210 fabric w/ micro gel fiber polyester filling 210 gsm w/ 4 corner loops',
                                                   '100% cotton cambric t-233 micro gel fiber filling 225 gsm','cotton/poly plain t250 mercerized',
                                                   'cotton/polyester with 0.5 cm satin stripe t250 mercerized tone on tone (white x white)',
                                                   '60% cotton/ 40% polyester 5 mm tone on tone stripe t250 mercerized','100% polyester white jacquard - 200 gsm',
                                                   '100% polyester white jacquard - 295 gsm','100% polyester white crinkle weave - 140 gsm','52% cotton/ 48% polyester printed bedspread',
                                                   '100% polyester','100% spun 2 ply mjs polyester','100% spun single ply mjs polyester','microgel','microgel siliconized',
                                                   '100% cotton tufted bath rugs w/ rounded corners','100% cotton tufted bath rugs w/ frame design','60% combed cotton/ 40% polyester',]

       # Design/Feature Keywords (partial list for demonstration)
design_feature_keywords = ['4" pocket','5.5" top fold','"cosmetic" logo embroidered','"makeup" logo embroidered w/ serge hem','all over birds eye pattern','all over checker cam border','basketweave design','big honeycomb design (santa barbara)','birds eye frame pattern','broad band plain cam border 4"','cam border','can be used as a duvet insert or as a comforter','can be used as a duvet insert or as a comforter; w/ 4 corner loops','can be used as duvet insert or coverlet or bed topper','crinkle design','dobby border','dobby border & dobby edge','dobby border & dobby hemmed','dobby chekered border','dobby edge','dobby hemmed','dobby hemmed; bleach safe','dobby twill edge hemmed ','double sided','envelop style 15" pocket','herringbone design','hookless weighted bottom hem & water repellent','matelassé design (diamond)','piano design dobby border & dobby hemmed','plain cam hem','satin band','satin band ','satin band damask. mercerized & calendaring finish.','satin band w/ two side tuck selvedge','synthetic down','tone on tone','w/ 15" fitted','bottom: non woven fabric','filling: 100 gsm polyester', 'skirt: 100% knit polyester','w/ cam border','hemmed', 'over lock', 'double dobby', 'stripe', 'fringe', 'new', 'ribbed', 'dobby', 'Square Hemmed']
    # Color Keywords (partial list for demonstration)
color_design_keywords = ['tropcial stripe','beige solid','gold','white solid','paradise tequila sunrise','tropical kiwi','vanilla','blue center stripe', 'blue stripe', 'green stripe', 'gold stripe', 'white w/ black stripe', 'white w/ blue stripe', 'white w/ green stripe', 'white w/ red stripe', 
                                                     'white', 'blue x white', 'yellow x white', 'aqua x white',	'charcoal grey x white', 'navy blue x white', 'yellow stripe', 'tan stripe', 'grey stripe',	'royal blue stripe',
                                                     'turquoise stripe', 'medium blue stripe', 'light blue', 'yellow', 'german blue', 'aqua blue', 'bone', 'kashmir green', 'blue mist', 'colonial blue', 'charcoal grey', 'black', 
                                                     'royal blue', 'hunter green', 'navy blue',	'orange', 'admiral blue', 'aqua', 'apple green/ lime', 'sunrise yellow', 'taupe', 'beige', 'assorted', '6 color assortment', 
                                                     'multicolor stripe', 'brown', 'burgundy', 'navy', 'silver grey', 'hazelnut brown',	'eggplant',	'green', 'tan',	'white x green checked', 'white x tan checked',	
                                                     'tan x green checked', 'white x blue checked',	'blue',	'grey *new', 'grey * new', 'white x tan', 'black w/ white dots', 'white- full terry', 'white w/ center stripe blue', 
                                                     'white w/ black stripes',	'white w/ bamboo stripes',	'white w/ blue stripes',	'white w/ forest green stripes',	'white w/ gold stripes *new',	'white w/ navy blue stripes',	'white w/ burgundy stripes',	'white w/ royal blue stripes',	'white w/ red stripes',	'white w/ sage green stripes',	'chambray grey w/ white stripes',	'chocolate brown',	'ivory',	'rust',	'sandalwood',	'forest green',	'red',	'grey',	'sandal wood',	'corn yellow',	'dusty rose',	'peach',	'pink',	'purple',	'sea foam',	'teal',	'wedgewood blue',	'desert tan',	'jade', 
                                                      'zinnia','white', 'black', 'blue', 'green', 'yellow', 'red', 'grey', 'charcoal', 'navy', 'orange', 'tan', 'purple', 'pink', 'aqua', 'multicolor']
# Size Keywords
size_design_keywords =  ['120" round','132" round','14" hood piping','64" round','72" round','90" round']
    # duplicate skus
duplicate_sku_keywords = ['oxford']
    # Features
special_attribute_keywords = ['pool bleach safe','2" elastic anchor band on all four corners.','6 sides waterproof mattress encasement','zipper']
    # Misc.
# Initialize 'Special Attribute' and any other dynamic columns with default values
dynamic_columns = ['Type', 'Material/Composition', 'Design/Feature', 'Color Design', 'Size Design', 'Duplicate Sku', 'Special Attribute', 'Miscellaneous']
for col in dynamic_columns:
    if col not in df.columns:
        df[col] = None  # Or another appropriate default value like an empty string ''

    # Return all others in Miscelanious column
def categorize_row(row):

    # Check "Child Category" first
    matched = False
    for keyword in type_keywords:
        if keyword in row['Child Category'].lower():
            row['Type'] = keyword
            matched = True
            break  # Found a match in "Child Category", no need to check other columns for 'Type'

    # If not matched in "Child Category", then check other columns
    if not matched:
        for keyword in type_keywords:   
            if keyword in row['Child Category'].lower() or keyword in row['Quality'].lower() or keyword in row['Specs'].lower() or keyword in row['Color'].lower() or keyword in row['Description'].lower():
             row['Type'] = keyword
             matched = True
             break  # Stop if we found a keyword

    for keyword in material_composition_keywords:
        if keyword in row['Child Category'].lower() or keyword in row['Quality'].lower() or keyword in row['Specs'].lower() or keyword in row['Color'].lower() or keyword in row['Description'].lower():
            row['Material/Composition'] = keyword
            matched = True
            break  
        
    for keyword in design_feature_keywords:
        if keyword in row['Child Category'].lower() or keyword in row['Quality'].lower() or keyword in row['Specs'].lower() or keyword in row['Color'].lower() or keyword in row['Description'].lower():
            row['Design/Feature'] = keyword
            matched = True
            break  
        
    for keyword in color_design_keywords:
        if keyword in row['Child Category'].lower() or keyword in row['Quality'].lower() or keyword in row['Specs'].lower() or keyword in row['Color'].lower() or keyword in row['Description'].lower():
            row['Color Design'] = keyword
            matched = True
            break 
         
    for keyword in size_design_keywords:
        if keyword in row['Child Category'].lower() or keyword in row['Quality'].lower() or keyword in row['Specs'].lower() or keyword in row['Color'].lower() or keyword in row['Description'].lower():
            row['Size Design'] = keyword
            matched = True
            break  
    for keyword in duplicate_sku_keywords:
        if keyword in row['Child Category'].lower():
            row['Duplicate Sku'] = keyword
            matched = True
            break
    for keyword in special_attribute_keywords:
        if keyword in row['Child Category'].lower() or keyword in row['Quality'].lower() or keyword in row['Specs'].lower() or keyword in row['Color'].lower() or keyword in row['Description'].lower():
            row['Special Attribute'] = keyword
            matched = True
            break
             # If no specific category was matched, assign to 'Miscellaneous'
        if not matched:
            row['Miscellaneous'] = row['Child Category']

        return row
        
# Define the desired column order after categorization
desired_column_order = [
    'SKU', 'Parent Product', 'Product Size', 'Product Weight','Product Weight UOM', 'Cost Per Each', 'Case Qty', 'Cost Per Case',
    'Case Length', 'Case Width', 'Case Height', 'Case Dims UOM', 'Case Weight', 'Case Weight UOM', 'Bale / Carton', 
    'Type', 'Material/Composition', 'Design/Feature', 'Color Design', 'Size Design', 'Duplicate Sku', 
    'Special Attribute', 'Description', 'Child Category','Quality',	'Specs','Color'

]

# Safely reorder columns, skipping any that are not present in the DataFrame
df = df[[col for col in desired_column_order if col in df.columns]]
df.columns = [col.strip() for col in df.columns]  # Remove leading/trailing spaces



# Apply the function to each row
df = df.apply(categorize_row, axis=1)
# Reorder the DataFrame columns
df = df[desired_column_order]

# Specify the output file path
output_file_path = r"C:\Users\juddu\OneDrive\Mission Critical Projects\Flask GUI\uploads\processed_ganesh_mills_data.json"

# Convert DataFrame to JSON
output_json = df.to_json(orient='records', lines=True)

# Write the JSON output to a file
with open(output_file_path, 'w') as file:
    file.write(output_json)
