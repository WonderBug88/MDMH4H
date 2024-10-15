# Description: This file contains all the SQL queries used in the application.
from db.curd import DataRetriever

all_queries = {
    "1888_mills": """ WITH product_hierarchy AS (
                        SELECT
                        title AS variant_title,
                        parent_product,
                        description,
                        category,
                        sub_category,
                        part_number,
                        type,
                        size,
                        weight,
                        color,
                        each_cost,
                        case_cost,
                        case_qty, case_weight, case_width, case_length, case_height,
                        image_url, 
                        url, active
                        FROM
                            product
                        )
                        SELECT
                            parent_product,
                            jsonb_agg(jsonb_build_object(
                                'title', variant_title,
                                'sku', part_number,
                                'description', description,
                                'category', category,
                                'sub_category', sub_category,
                                'type', type,
                                'size', size,
                                'weight', weight,
                                'color', color,
                                'each_cost', each_cost,
                                'case_cost', case_cost,
                                'images', image_url,
                                'url', url,
                                'active', active,
                                'case_qty', case_qty, 'case_weight', case_weight, 'case_width', case_width, 'case_length', case_length, 'case_height', case_height
                            )) AS variants
                        FROM
                            product_hierarchy
                        GROUP BY
                            parent_product
                        LIMIT %s
                        OFFSET %s;
                        """,
    "Thomaston Mills": """ WITH product_hierarchy AS (
                        SELECT
                        sku, title AS variant_title, parent_product,
                        pocket, type, size, material, lbs_dz, thread_count,
                        price_dz, dz_ctn, qty_case, price_ea, price_case,
                        image_url,
                        case_width, case_length, case_height, case_weight,
                        cube_cu_ft, dz_pallet, ea_pallet, hem_size,
                        category_type, description
                        FROM
                            products
                        )
                        SELECT
                            parent_product,
                            jsonb_agg(jsonb_build_object(
                                'title', variant_title,
                                'sku', sku,
                                'description', description,
                                'category', category_type,
                                'type', type,
                                'size', size,
                                'weight', lbs_dz,
                                'each_cost', price_ea,
                                'case_cost', price_case,
                                'images', image_url,
                                'case_qty', qty_case, 
                                'case_weight', case_weight,
                                'case_width', case_width, 
                                'case_length', case_length, 
                                'case_height', case_height
                            )) AS variants
                        FROM
                            product_hierarchy
                        GROUP BY
                            parent_product
                        LIMIT %s
                        OFFSET %s;
                        """,

    "berkshire": """WITH product_hierarchy AS (
                        SELECT
                        title AS variant_title,
                        parent_product,
                        sku,
                        category,
                        sub_category,
                        description,
                        type,
                        size,
                        color,
                        case_cost,
                        case_qty, case_weight, case_width, case_length, case_height,
                        image_list
                        FROM
                            products
                        )
                        SELECT
                            parent_product,
                            jsonb_agg(jsonb_build_object(
                                'title', variant_title,
                                'sku', sku,
                                'description', description,
                                'category', category,
                                'sub_category', sub_category,
                                'type', type,
                                'size', size,
                                'weight', case_weight,
                                'color', color,
                                'each_cost', case_cost,
                                'case_cost', case_cost,
                                'images', image_list,
                                'case_qty', case_qty, 'case_weight', case_weight, 
                                'case_width', case_width, 'case_length', case_length, 
                                'case_height', case_height
                            )) AS variants
                        FROM
                            product_hierarchy
                        GROUP BY
                            parent_product
                        LIMIT %s
                        OFFSET %s;
                        """,
    "ganesh": """ WITH product_hierarchy AS (
                        SELECT
                        parent_product AS variant_title,
                        parent_product,
                        sku,
                        child_category AS category,
                        description,
                        type,
                        product_size as size,
                        product_weight as weight,
                        color,
                        cost_per_each AS each_cost,
                        cost_per_case AS case_cost,
                        case_qty, case_weight, case_width, case_length, case_height,
                        images_url AS image_url
                        FROM
                            products
                        )
                        SELECT
                            parent_product,
                            jsonb_agg(jsonb_build_object(
                                'title', variant_title,
                                'sku', sku,
                                'description', description,
                                'category', category,
                                'type', type,
                                'size', size,
                                'weight', weight,
                                'color', color,
                                'each_cost', each_cost,
                                'case_cost', case_cost,
                                'images', image_url,
                                'case_qty', case_qty, 'case_weight', case_weight,
                                  'case_width', case_width, 
                                  'case_length', case_length, 
                                  'case_height', case_height
                            )) AS variants
                        FROM
                            product_hierarchy
                        GROUP BY
                            parent_product
                        LIMIT %s
                        OFFSET %s;
                        """,
    "bissel": """ 
        WITH product_hierarchy AS (
            SELECT
                p.parent_product AS parent_product,
                p.title AS variant_title,
                p.sku AS sku,
                p.gallery_images AS image_url,
                p.retail_price AS each_cost,
                p.price_per_unit AS case_cost,
                p.description AS description,

                pd.width AS width,
                pd.length AS length,
                pd.height AS height,
                pd.weight AS weight,
                pd.case_qty AS case_qty,
                pd.case_width AS case_width,
                pd.case_length AS case_length,
                pd.case_height AS case_height,
                pd.case_weight AS case_weight
            FROM
                product p
            LEFT JOIN
                product_dimensions pd ON pd.sku = p.sku
        )
        SELECT
            parent_product,
            jsonb_agg(jsonb_build_object(
                'title', variant_title,
                'sku', sku,
                'description', description,
                'size', width || 'x' || length || 'x' || height,
                'weight', weight,
                'each_cost', each_cost,
                'case_cost', case_cost,
                'images', image_url,
                'case_qty', case_qty,
                'case_weight', case_weight,
                'case_width', case_width,
                'case_length', case_length,
                'case_height', case_height
            )) AS variants
        FROM
            product_hierarchy
        GROUP BY
            parent_product
        LIMIT %s
        OFFSET %s;
    """,

    "downlite_import": """ WITH product_hierarchy AS (
            SELECT
                p.name AS parent_product,
                p.sku AS variant_title,
                p.type AS type,
                p.sku AS sku,
                p.description AS description,
                v.image_url AS image_url,
                p.price AS each_cost,
                p.cost_price AS case_cost,

                v.width AS width,
                v.depth AS length,
                v.height AS height,
                v.weight AS weight,
                v.sku AS case_width,
                v.sku AS case_length,
                v.sku AS case_height,
                v.sku AS case_weight
            FROM
                products p
            LEFT JOIN
                variants v ON v.product_id = p.id
        )
        SELECT
            parent_product,
            jsonb_agg(jsonb_build_object(
                'title', variant_title,
                'sku', sku,
                'description', description,
                'type', type,
                'size', width || 'x' || length || 'x' || height,
                'weight', weight,
                'each_cost', each_cost,
                'case_cost', case_cost,
                'images', image_url,
                'case_weight', weight,
                'case_width', width,
                'case_length', length,
                'case_height', height
            )) AS variants
        FROM
            product_hierarchy
        GROUP BY
            parent_product
        LIMIT %s
        OFFSET %s;
    """,
    "h4h_import2": """ WITH product_hierarchy AS (
            SELECT
                p.name AS parent_product,
                p.id AS id,
                p.name AS variant_title,
                p.type AS type,
                p.custom_url as custom_url,
                p.sku AS parent_sku,
                p.description AS description,
                v.image_url AS image_url,
                p.price AS each_cost,
                p.cost_price AS case_cost,

                v.width AS width,
                v.depth AS length,
                v.height AS height,
                v.weight AS weight,
                v.sku AS case_width,
                v.sku AS case_length,
                v.sku AS case_height,
                v.sku AS case_weight,
                v.sku AS sku
            FROM
                products p
            LEFT JOIN
                variants v ON v.product_id = p.id
        )
        SELECT
            parent_product,
            parent_sku,
            id,
            jsonb_agg(jsonb_build_object(
                'title', variant_title,
                'sku', sku,
                'description', description,
                'type', type,
                'custom_url', custom_url,
                'size', width || 'x' || length || 'x' || height,
                'weight', weight,
                'each_cost', each_cost,
                'case_cost', case_cost,
                'images', image_url,
                'case_weight', weight,
                'case_width', width,
                'case_length', length,
                'case_height', height
            )) AS variants
        FROM
            product_hierarchy
        GROUP BY
            parent_product,
            parent_sku,
            id
        LIMIT %s
        OFFSET %s;
    """,
}


def get_raw_query(schema_name: str):
    """Return the products raw query for the specified schema."""
    return all_queries.get(schema_name, '')


def get_gsc_query(custom_urls, start_date, end_date):
    """
    Retrieves data from gsc_data table based on custom URL and date range.

    Args:
      search_value: The Value to filter by can be a Custom URL, Category, or Sub-Category.
      start_date: The start date of the filter range.
      end_date: The end date of the filter range.

    Returns:
      The SQL query as a string.
    """

    # gsc_qry = f"""
    #     SELECT *
    #     FROM gsc_data
    #     WHERE LOWER(page) LIKE LOWER('%{custom_urls}%')
    #         AND date BETWEEN '{start_date}' AND '{end_date}'
    #     ORDER BY date DESC
    #     LIMIT {25} OFFSET {0};
    #     """

    # Ensure custom_urls is a list of strings, then convert to a properly formatted tuple
    custom_urls = ', '.join([f"'{url}'" for url in custom_urls])

    # Use parentheses around the list of URLs in the SQL query
    gsc_qry = f"""
        SELECT *
        FROM gsc_data
        WHERE page IN ({custom_urls})  -- Wrapped in parentheses for the IN clause
            AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date DESC;
    """

    return gsc_qry


def get_order_history_query(skus):

    query = f"""
        WITH sku_stats AS (
            SELECT 
                sku,
                COUNT(*) AS num_orders,  -- Total number of orders for each SKU
                SUM(order_qty_total) AS qty_sold,  -- Total units sold for each SKU
                MAX(order_date) AS last_sold_date,  -- Most recent order date for each SKU
                SUM(order_total) AS total_order_value  -- Total order value for each SKU
            FROM 
                orders
            WHERE 
                sku IN ({skus})
            GROUP BY 
                sku
        )
        SELECT 
            ss.sku,
            ss.num_orders,  -- Number of Orders
            ss.qty_sold,  -- Quantity Sold
            CURRENT_DATE - ss.last_sold_date AS days_since_last_sold,  -- Days Since Last Sold
            ROUND(ss.total_order_value / NULLIF(ss.qty_sold, 0), 2) AS selling_price,  -- Selling Price rounded to 2 digits
            ss.last_sold_date AS purchase_date  -- Purchase Date (last sold date)
        FROM 
            sku_stats ss
        WHERE 
            ss.sku IN ({skus})
    """

    return query


def get_product_categories_data(sku):
    query = f"""
        SELECT 
            p.sku AS sku,
            p.product_url AS product_url,
            
            -- Aggregating categories with id and URL
            jsonb_agg(DISTINCT jsonb_build_object(
                'id', p.category_id,
                'category_url', p.category_url
            )) AS categories,
            
            -- Aggregating brands with id and URL
            jsonb_agg(DISTINCT jsonb_build_object(
                'id', p.brand_id,
                'brand_url', p.brand_url
            )) AS brands

        FROM 
            product_categories p

        WHERE 
            p.sku = '{sku}' -- Filter by sku

        GROUP BY 
            p.sku, p.product_url;
    """
    pc_data = DataRetriever(schema='h4h_import2').query(query)
    return pc_data[0] if isinstance(pc_data, list) and len(pc_data) > 0 else pc_data
