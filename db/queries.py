
all_queries = {
    "1888_mills": """ WITH product_hierarchy AS (
                        SELECT
                        title AS variant_title,
                        parent_product,
                        description,
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
}


def get_raw_query(schema_name: str):
    """Return the products raw query for the specified schema."""
    return all_queries.get(schema_name, '')


def get_gsc_query(custom_url, start_date, end_date):
    """
    Retrieves data from gsc_data table based on custom URL and date range.

    Args:
      custom_url: The URL to filter by.
      start_date: The start date of the filter range.
      end_date: The end date of the filter range.

    Returns:
      The SQL query as a string.
    """

    gsc_qry = f"""
        SELECT * 
        FROM gsc_data 
        WHERE page LIKE '%{custom_url}%' 
            AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date DESC 
        LIMIT {10} OFFSET {0};"""

    return gsc_qry
