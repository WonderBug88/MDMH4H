import copy
from collections import OrderedDict
from datetime import datetime
from urllib.parse import urljoin

from scrapy import Spider, Request, signals


class AshSpider(Spider):
    name = "ahs"
    base_url = "https://www.ahscompany.com/"
    start_urls = ["https://www.ahscompany.com/"]

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408, 429],

        'FEEDS': {
            f'output/Ahs Company Products {datetime.now().strftime("%d%m%Y%H%M%S")}.json': {
                'format': 'json',
                'fields': ['product_id', 'title', 'price', 'retail_price', 'brand', 'stock_status',
                           'part_number', 'options', 'images', 'description', 'category',
                           'sub_category', 'size', 'type', 'material', 'url'],
            }
        }
    }

    headers = {
        'authority': 'www.ahscompany.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-PK,en;q=0.9,ur-PK;q=0.8,ur;q=0.7,en-US;q=0.6',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.brands = []
        self.total_brands_count = 0
        self.items_scraped_count = 0

    def parse(self, response, **kwargs):
        """
        - Parse Brands and their URLs.
        - Brands will be scraped one by one
        """

        brands = response.css('#catMenu2 .cat-list li')
        for brand in brands:
            name = brand.css('a ::text').get('')
            url = urljoin(self.base_url, brand.css('a::attr(href)').get(''))

            brand_info = {
                'name': name,
                'url': url
            }

            self.brands.append(brand_info)

        self.total_brands_count = len(self.brands)

    def parse_brand_categories(self, response):
        """
        - Parse brand pages to extract categories and all sub categories until the leaf category reached.
        - If there is no categories found, check if its a products listings page, if yes, send request for each that product
        - if its not a categories page nor a products page, check if its a details page of product, if yes, scrape the details of the product
        - At a time, there will be only one type of page. Either Category or products listings page or product details page

        """

        # If there are categories, parse them until the products listings page shows
        # Request on categories until the products listings page appears
        brand_categories = response.css('.sub-categories-format .sub-categories')

        for category in brand_categories:
            # Check and update meta information
            if not response.meta.get('category', ''):
                response.meta['category'] = category.css('.name::text').get('')

            elif not response.meta.get('sub_category', ''):
                response.meta['sub_category'] = category.css('.name::text').get('')

            elif not response.meta.get('sub_sub_category', ''):
                response.meta['sub_sub_category'] = category.css('.name::text').get('')

            # Extract the URL for the category
            category_url = urljoin(self.base_url, category.css('a::attr(href)').get())

            yield Request(url=category_url, callback=self.parse_brand_categories, meta=response.meta)

        # Check if there are products on the page, then send request for each that product
        products = response.css('#itemsBlock .productBlockContainer .product-item') or response.css(
            '#itemsBlock .productBlockContainer')
        for product in products:
            url = urljoin(self.base_url, product.css('.name a::attr(href)').get(''))

            yield Request(url, callback=self.parse_product_details, meta=response.meta)

        # Check if it is the detailed page of a product, then scrape the details from the parse_product_details method
        if not brand_categories and not products:
            yield from self.parse_product_details(response)

    def parse_product_details(self, response):
        """
        Parse product detail pages to extract product information.
        """

        try:
            pid = response.css('input[name="item_id"]::attr(value)').get('')
            title = response.css('h1[itemprop="name"] ::text').get('')
            add_to_card = response.css('#Add').get('')
            description = response.css('[itemprop="description"]  ::text').getall()
            price = response.css('meta[itemprop="price"]::attr(content)').get('')
            part_number = response.css('#product_id ::text').get('')
            # Adjust the part_number by removing any prefix before a space
            part_number = part_number.split(' ', 1)[-1].strip() if ' ' in part_number else part_number
            

            if not title and not pid:
                return

            item = OrderedDict()
            item['product_id'] = pid
            item['title'] = title
            item['price'] = str(price)
            item['retail_price'] = str(
                response.css('.retailprice span::text').get('').replace('$', '').replace(',', ''))
            item['brand'] = self.get_value(response, 'Brand') or ''.join(
                response.css('.breadcrumbs a ::text').getall()[1:2])
            item['stock_status'] = 'In Stock' if add_to_card else 'Out Of Stock'
            item['part_number'] = part_number
            item['images'] = self.get_product_images(response)
            item['description'] = '\n'.join(
                [item.strip() for item in description if item.strip()]) if description else ''
            item['category'] = ''.join(response.css('.breadcrumbs a ::text').getall()[2:3])
            item['sub_category'] = ', '.join(response.css('.breadcrumbs a ::text').getall()[3:])
            item['size'] = self.get_value(response, 'Size')
            item['type'] = self.get_value(response, 'Type')
            item['material'] = self.get_value(response, 'Material')
            item['options'] = {}
            item['url'] = response.css('link[rel="canonical"] ::attr(href)').get('') or response.url

            variants = response.css('#divOptionsBlock option')

            if not variants:
                self.items_scraped_count += 1
                print(f'Current items scraped: {self.items_scraped_count}')

                yield item

                return

            # Parse Variants
            # if there is one single type of variant

            if len(response.css('#divOptionsBlock .dropdown-format').getall()) == 1:
                variants = response.css('#divOptionsBlock option')[1:]
                label = response.css('#divOptionsBlock option::text').get('').strip()

                for variant in variants:
                    variant_item = copy.deepcopy(item)
                    options = self.get_variant_price(response, variant, price, part_number, label)
                    variant_item['price'] = options.get('price', '')
                    p_number = options.get('part_number', '')
                    p_number = p_number.split(' ', 1)[-1].strip() if ' ' in p_number else p_number
                    variant_item['part_number'] = p_number
                    variant_item['options'] = options.get('variant_name', '')
                    self.items_scraped_count += 1
                    print(f'Current items scraped: {self.items_scraped_count}')

                    yield variant_item
            else:
                # If there are 2 or 3 types of variants in dropdowns selection
                first_variant_group = response.css('#divOptionsBlock .container > :nth-child(1) option')[1:]
                first_variant_label = response.css('#divOptionsBlock .container > :nth-child(1) option::text').get(
                    '').strip()
                second_variant_group = response.css('#divOptionsBlock .container > :nth-child(3) option')[1:]
                second_variant_label = response.css('#divOptionsBlock .container > :nth-child(3) option::text').get(
                    '').strip()
                third_variant_group = response.css('#divOptionsBlock .container > :nth-child(5) option')[1:]
                third_variant_label = response.css('#divOptionsBlock .container > :nth-child(5) option::text').get(
                    '').strip()

                for first_variant in first_variant_group:
                    for second_variant in second_variant_group:
                        for third_variant in third_variant_group:
                            variant_item = copy.deepcopy(item)
                            options = self.get_variant_group(response, first_variant, first_variant_label,
                                                             second_variant, second_variant_label, price,
                                                             third_variant, third_variant_label, part_number)

                            variant_item['price'] = options.get('price', '')
                            variant_item['part_number'] = options.get('part_number', '')
                            variant_item['options'] = options.get('variant_name', '')
                            self.items_scraped_count += 1
                            print(f'Current items scraped: {self.items_scraped_count}')

                            yield variant_item

        except Exception as e:
            print(f'Error parsing product detail: {response.url}')
            self.logger.error(f'Error parsing product detail: {response.url} and Error : {e}')
            return

    def get_value(self, response, key):
        elements = response.css(f'.breadcrumbsBlock ul li:contains("{key}") ::text').getall()
        if elements:
            return elements[-1]
        else:
            return ''

    def get_product_images(self, response):
        images = response.css('.slides .prod-thumb a::attr(href)').getall() or []
        images = images or response.css('.main-image a::attr(href)').getall() or []
        return images

    def get_variant_price(self, response, variant, price, part_number, label):
        variant_value = variant.css('::attr(value)').get('')
        variant_name = variant.css('::text').get('').strip()

        if not variant_value:
            info = {
                'price': str(price),
                'part_number': part_number,
                'variant_name': {label: variant_name}
            }

            return info

        price_value = response.css(f'.container input[name="price_{variant_value}"]::attr(value)').get('')
        if price_value == 0 and price_value is None:
            variant_price = price
        else:
            variant_price = float(price) + float(price_value)

        script_tag = response.css(f'script[type="text/javascript"]:contains("{variant_value}")').get('')
        if script_tag:
            p_number = script_tag.split(variant_value)[1].split('=')[1].split(';')[0].strip().replace("'",
                                                                                                      "") or part_number
        else:
            p_number = part_number
        info = {
            'price': str(variant_price),
            'part_number': p_number,
            'variant_name': {label: variant_name}
        }

        return info

    def get_variant_group(self, response, first_variant, first_variant_label, second_variant, second_variant_label,
                          price, third_variant, third_variant_label, part_number):

        f_variant = self.get_variant_price(response, first_variant, price, part_number, first_variant_label)
        sec_variant = self.get_variant_price(response, second_variant, price, part_number, second_variant_label)
        thi_variant = self.get_variant_price(response, third_variant, price, part_number, third_variant_label)
        p_number = f_variant.get('part_number', '')
        f_variant_price = f_variant.get('price', '')

        info = {
            'part_number': p_number,
            'price': str(f_variant_price),
            'variant_name': {
                **f_variant.get('variant_name', {}),
                **sec_variant.get('variant_name', {}),
                **thi_variant.get('variant_name', {})}
        }

        return info

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(AshSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        """
        Handle spider idle state by crawling next brand if available.
        """

        print(f'\n\nTotal {self.items_scraped_count} items scraped')
        print(f'\n\n{len(self.brands)}/{self.total_brands_count} Brands left to Scrape\n\n')

        if self.brands:
            brand = self.brands.pop(0)
            brand_name = brand.get('name', '')
            brand_url = brand.get('url', '')

            req = Request(url=brand_url,
                          callback=self.parse_brand_categories, dont_filter=True,
                          meta={'handle_httpstatus_all': True, 'brand': brand_name})

            try:
                self.crawler.engine.crawl(req)  # For latest Python version
            except TypeError:
                self.crawler.engine.crawl(req, self)  # For old Python version < 10
