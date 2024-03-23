from db import get_db_connection
import openai
import os

@app.route('/gneratate_content', methods=['GET'])
def generate_content():
    # Step 1: Fetch data from your database
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM product_info WHERE id = %s",)
    product_data = cursor.fetchone()
    cursor.close()
    connection.close()

    # Step 2: Construct the prompt for the Title, Description and Meta Data
    product_title = product_data['title']
    prompt = f"Write a creative product name and meta description for the following product: {product_title}"
    
    product_description = product_data['description']
    prompt = f"Write a SEO Friendly product description in html that begins with an h1 tag for the following product: {product_description}"
    
    meta_title = product_data['meta_title']
    prompt = f"Write a creative product meta title for the following product: {meta_title}"
    
    meta_keywords = product_data['meta_keywords']
    prompt = f"Write a list of keywords separated by a comma: {meta_keywords}"
    
    meta_description = product_data['meta_description']
    prompt = f"Write a creative meta description for the following product: {meta_description}"
    
    alt_image = product_data['alt_image']
    prompt = f"write a list of alt image description: {alt_image}"
    

    # Step 3: Call the OpenAI API
    openai_api_key = os.getenv('OPENAI_API_KEY')

    response = openai.Completion.create(
        model="gpt-3.5-turbo-instruct",  # Adjust the model as necessary
        prompt=prompt,
        temperature=0.7,
        max_tokens=500  # Adjust based on the expected length of the content
    )
    generated_text = response.choices[0].text.strip()