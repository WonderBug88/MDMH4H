import scrapy
import json


class WestPointSpider(scrapy.Spider):
    name = "westpoint"
    start_urls = ['https://buywestpointdirect.com/']

    def parse(self, response):
        self.logger.info(f"Accessing main page: {response.url}")
        # Extract all category links from the menu
        category_links = response.css(
            'ul.menu-drawer__menu a.menu-drawer__menu-item::attr(href)'
        ).getall()

        # Filter out unwanted links
        unwanted_links = ["/pages/contact-us", "/blogs/news"]
        category_links = [
            response.urljoin(link)
            for link in category_links
            if link not in unwanted_links
        ]

        self.logger.info(f"Found {len(category_links)} categories: {category_links}")
        # Follow each valid category link
        for link in category_links:
            self.logger.info(f"Following category: {link}")
            yield response.follow(link, self.parse_category)

    def parse_category(self, response):
        self.logger.info(f"Accessing category page: {response.url}")
        # Extract product links from the collection page
        product_links = response.css(
            'ul#product-grid li.grid__item a.full-unstyled-link::attr(href)'
        ).getall()

        self.logger.info(f"Found {len(product_links)} products in category: {response.url}")
        # Follow each product link
        for product_link in product_links:
            full_product_link = response.urljoin(product_link)
            self.logger.info(f"Following product link: {full_product_link}")
            yield response.follow(full_product_link, self.parse_product)

        # Handle pagination for the collection page
        next_page = response.css('a.pagination__next::attr(href)').get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            self.logger.info(f"Found next page: {next_page_url}")
            yield response.follow(next_page_url, self.parse_category)

    def parse_product(self, response):
        self.logger.info(f"Scraping product page: {response.url}")

        # Try extracting JSON data embedded in the script tag
        script_data = response.xpath('//script[contains(@type, "application/json")]/text()').get()
        if not script_data:
            self.logger.warning(f"No JSON script found on {response.url}")
            return

        try:
            # Parse JSON
            parsed_data = json.loads(script_data)
            self.logger.debug(f"Parsed JSON data from {response.url}")

            # Extract product information
            product_info = parsed_data.get('product', {})
            if product_info:
                yield self.extract_product_data(product_info)
            else:
                self.logger.error(f"Unexpected JSON structure on {response.url}")
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON on {response.url}: {e}")

    def extract_product_data(self, product):
        """Extract relevant data from the product dictionary."""
        return {
            'sku': product.get('sku'),
            'title': product.get('title'),
            'price': product.get('price', 0) / 100,  # Convert from cents
            'weight': product.get('weight', 0),  # Likely in grams
            'images': [image.get('src') for image in product.get('images', [])],
            'description': product.get('body_html'),
            'availability': product.get('available'),
            'options': product.get('options', []),
        }
