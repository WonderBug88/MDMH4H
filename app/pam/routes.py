
from flask import (Blueprint, render_template, session, render_template_string,
                   redirect, url_for, request, jsonify)
from db.curd import DataRetriever


pam_bp = Blueprint("pam", __name__)


@pam_bp.route('/pam', methods=['GET', 'POST'])
def pam_main():
    if 'logged_in' not in session:
        return redirect(url_for('user.login'))

    db = DataRetriever(schema='h4h_import2')

    # Get the page number from the request or default to 1
    page = int(request.args.get('page', 1))
    limit = 10
    offset = (page - 1) * limit

    search = request.args.get('search', '')

    # Query to get the data
    query = f"""
    SELECT 
    brand_name,
    JSON_AGG(
        JSON_BUILD_OBJECT(
            'category', category_name,
            'products', product_names,
            'total_products', total_products
        )
    ) AS data
    FROM (
    SELECT 
        brand_name, 
        category_name, 
        ARRAY_AGG(DISTINCT product_name) AS product_names, -- Ensuring unique product names
        COUNT(DISTINCT product_name) AS total_products -- Ensuring count matches unique products
    FROM product_categories
    WHERE product_name ILIKE '%%{search}%%' OR 
    category_name ILIKE '%%{search}%%' OR 
    brand_name ILIKE '%%{search}%%'
    GROUP BY brand_name, category_name
    ) grouped_data
    GROUP BY brand_name
    ORDER BY brand_name
    LIMIT %s
    OFFSET %s;
    """
    product_categories = db.query(query, limit, offset)

    # Check if more data exists for the next page
    next_page = len(product_categories) == limit

    # If AJAX request, render HTML dynamically and return it for load more
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        html = render_template_string("""
        <div class="space-y-4">
            {% for brand in brands_data %}
            <div class="rounded-lg border border-gray-300 bg-white shadow-md">
                <div class="cursor-pointer flex items-center justify-between bg-gray-200 p-4" onclick="toggleSection('brand-{{ brand.brand_name }}')">
                    <h3 class="text-lg font-bold">
                                      {% if brand.brand_name %}
                                        {{ brand.brand_name }}
                                      {% else %}
                                        Other
                                      {% endif %}
                                      </h3>
                    <span class="text-sm text-gray-600">Categories: {{ brand.data|length }}</span>
                </div>
                <div id="brand-{{ brand.brand_name }}" class="hidden p-4">
                    {% for category in brand.data %}
                    <div class="mb-4 rounded-lg border border-gray-300 bg-gray-50">
                        <div class="cursor-pointer flex items-center justify-between p-4" onclick="toggleSection('{{brand.brand_name}}-category-{{ category.category }}-{{loop.index}}')">
                            <span class="font-semibold">{{ category.category }}</span>
                            <span class="text-sm text-gray-600">
                                Total Products: {{ category.total_products }}
                            </span>
                        </div>
                        <div id="{{brand.brand_name}}-category-{{ category.category }}-{{loop.index}}" class="hidden p-4">
                            <ul class="space-y-2">
                                {% for product in category.products %}
                                <li class="flex items-center justify-between rounded bg-gray-200 p-4">
                                    <span class="font-semibold">{{ product }}</span>
                                </li>
                                {% endfor %}
                            </ul>
                            <button onclick="openModal('{{ brand.brand_name }}')" class="mt-4 rounded bg-green-500 px-4 py-2 text-white hover:bg-green-400">
                                Add New Parent Product
                            </button>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endfor %}
        </div>
        """, brands_data=product_categories, page=page)

        return jsonify({'html': html, 'next_page': next_page})

    return render_template('pam.html', name=session.get('name'), brands_data=product_categories, page=page)
