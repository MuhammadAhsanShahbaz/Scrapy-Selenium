from datetime import datetime
from urllib.parse import urljoin
from collections import OrderedDict

import requests
from bs4 import BeautifulSoup
from scrapy import Spider, Request, Selector


class SuperaSpider(Spider):
    name = "superatv"
    base_url = 'https://www.superatv.com/'

    headers = {
        'authority': 'www.superatv.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'pragma': 'no-cache',
        'referer': 'https://www.superatv.com/categories',
        'store': 'default',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }

    custom_settings = {
        'FEEDS': {
            f'output/SuperATV Products {datetime.now().strftime("%d%m%Y%H%M%S")}.json': {
                'format': 'json',
                'fields': ['URL', 'Brand', 'Name', 'SKU', 'Price', 'Stock_Status', 'Special_Notes',
                           'Installation_Instructions', 'Images', 'Videos', 'Description', 'Features', 'Fitment',
                           'Options'],
            }
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.brands_names = []
        self.errors = []

    def start_requests(self):
        yield Request(url='https://www.superatv.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!)%7Bcategory(id%3A%24idNum)%7Bid%20description%20category_information_content%20name%20url_key%20url_path%20product_count%20display_mode%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20__typename%7Dmeta_title%20meta_keywords%20meta_description%20canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20url_path%20description%20name%20image%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20position%20level%20name%20path%20url_path%20url_key%20product_count%20image%20thumbnail%20children%7Bid%20position%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7D__typename%7D%7D&operationName=category&variables=%7B%22id%22%3A2129%2C%22idNum%22%3A2129%7D',
            headers=self.headers, callback=self.parse_brand_names)

    def parse_brand_names(self, response):
        try:
            data = response.json().get('data', {}) or {}

            brands = data.get('categoryList', []) or []
            self.brands_names = [brand.get('name', '') for brand in brands[0].get('children', [])]

        except Exception as e:
            print(f"Error: {e}. An unexpected error occurred.")
            self.errors.append(f'Error From extracting Brands : {e}')
            return

        yield Request(url='https://www.superatv.com/graphql?query=query%20category(%24id%3AString!%2C%24idNum%3AInt!)%7Bcategory(id%3A%24idNum)%7Bid%20description%20category_information_content%20name%20url_key%20url_path%20product_count%20display_mode%20breadcrumbs%7Bcategory_level%20category_name%20category_url_key%20__typename%7Dmeta_title%20meta_keywords%20meta_description%20canonical_url%20__typename%7DcategoryList(filters%3A%7Bids%3A%7Bin%3A%5B%24id%5D%7D%7D)%7Bchildren_count%20url_key%20url_path%20description%20name%20image%20cms_block%7Bcontent%20identifier%20__typename%7Dchildren%7Bid%20position%20level%20name%20path%20url_path%20url_key%20product_count%20image%20thumbnail%20children%7Bid%20position%20level%20name%20path%20url_path%20url_key%20__typename%7D__typename%7D__typename%7D%7D&operationName=category&variables=%7B%22id%22%3A1453%2C%22idNum%22%3A1453%7D',
            headers=self.headers, callback=self.parse)

    def parse(self, response, **kwargs):
        try:
            categories = response.json().get('data', {}).get('categoryList', [{}])[0].get('children', [{}])
            categories_names = [cat_url.get('name') for cat_url in categories]

            for category_name in categories_names:
                form_data = self.get_form_data(category_name, page_no=1)
                respo = requests.post('https://alz60h.a.searchspring.io/api/search/search.json', params=form_data)

                if respo.status_code != 200:
                    self.errors.append(f"{category_name} ::Response is Not Correct. Pagination cannot proceed.")

                yield from self.parse_products_listings(response=respo, category=category_name)

        except Exception as e:
            print('Error from Parse Method:', e)
            self.errors.append(f'Error From Parse Method : {e}')
            return

    def parse_products_listings(self, response, category):
        try:
            data = response.json()
            products = data.get('results', []) or []

            for product in products:
                url = product.get('url', '').strip('/')

                yield Request(url=self.get_product_detail_page_url(url), callback=self.parse_details,
                              dont_filter=True, headers=self.headers)

            # Pagination
            next_page = data.get('pagination', {}).get('nextPage')
            if next_page:
                form_data = self.get_form_data(category, next_page)
                respo = requests.post('https://alz60h.a.searchspring.io/api/search/search.json', params=form_data)

                yield from self.parse_products_listings(response=respo, category=category)

        except Exception as e:
            print('Error from Pagination Method:', e)
            self.errors.append(f'Error From Pagination Method : {e}')

    def parse_details(self, response):
        try:
            data = response.json()
            product_json = data.get('data', {}).get('productDetail', {}).get('items', [])[0] or {}
        except Exception as e:
            self.errors.append(
                f"Response not in Json Format , URL:', {response.meta.get('product_url', '')} error: {e}")
            return

        if not product_json:
            return

        variants = product_json.get('variants', [])

        if not variants:
            item = self.get_item(product_json, variant_json={})
            yield item

            return

        for variant in variants:
            item = self.get_item(product_json, variant_json=variant)

            yield item

    def get_item(self, product_json, variant_json):
        try:
            product_title = product_json.get('name', '') or ''
            url = urljoin(self.base_url, product_json.get('url_key', ''))

            description = self.get_text_from_json_html(product_json, section_key='description')
            features = self.get_text_from_json_html(product_json, section_key='features')
            fitment = self.get_text_from_json_html(product_json, section_key='fitment')

            attachments = product_json.get('product_attachments', {}).get('product_attachments_data', []) or []
            installations_instructions = [file_url.get('file_url') for file_url in attachments]
            brand = self.get_brand_name(product_title)

            sku = product_json.get('sku', '')
            price = self.get_price(product_json)
            stock_status = product_json.get('stock_status', '')
            special_notes = product_json.get('special_notes', '')
            options = ''

            images = self.get_images(product_json)
            videos = self.get_videos(product_json)

            if variant_json:
                variant_json = variant_json.get('product', {})
                sku = variant_json.get('sku', '')
                price = self.get_price(variant_json)
                stock_status = variant_json.get('stock_status', '')
                special_notes = variant_json.get('special_notes', '')
                options = self.get_product_options(variant_json.get('attributes', []))

                images = self.get_images(variant_json) or images
                videos = self.get_videos(variant_json) or videos

            item = OrderedDict()

            item['URL'] = url
            item['Brand'] = brand
            item['Name'] = product_title
            item['SKU'] = sku
            item['Price'] = price
            item['Stock_Status'] = stock_status
            item['Special_Notes'] = special_notes
            item['Installation_Instructions'] = installations_instructions
            item['Images'] = images
            item['Videos'] = videos
            item['Description'] = description
            item['Features'] = features
            item['Fitment'] = fitment
            item['Options'] = options

            return item

        except Exception as e:
            self.errors.append(f"Detail page error {e}")

    def get_price(self, product_json):
        return product_json.get('price_range', {}).get('minimum_price', {}).get('final_price', {}).get('value', 0.0)

    def get_images(self, product_json):
        return [img.get('url', '') for img in product_json.get('media_gallery', []) if not img.get('video_content', {})]

    def get_videos(self, product_json):
        return [img.get('video_content', {}).get('video_url', '') for img in product_json.get('media_gallery', []) if img.get('video_content', {})]

    def get_form_data(self, cat_key, page_no):
        params = {
            'ajaxCatalog': 'v3',
            'resultsFormat': 'native',
            'siteId': 'alz60h',
            'page': page_no,
            'bgfilter.ss_category': f'Shop Make>Categories>{cat_key}',
        }
        return params

    def get_product_detail_page_url(self, product_url):
        url = f'https://www.superatv.com/graphql?query=query%20productDetail(%24urlKey%3AString%2C%24page_size%3AInt)%7BproductDetail%3Aproducts(filter%3A%7Burl_key%3A%7Beq%3A%24urlKey%7D%7D%2CpageSize%3A%24page_size)%7Bitems%7B__typename%20id%20sku%20name%20has_fitment_info%20universal_fit%20url_key%20stock_status%20review_details%7Breview_summary%20review_count%20__typename%7Drating_configurations%7Brating_attributes%20__typename%7Dshort_description%7Bhtml%20__typename%7Dfeatures%7Bhtml%20__typename%7Dfitment%7Bhtml%20__typename%7Ddescription%7Bhtml%20__typename%7Dwarranty%7Btitle%20content%20__typename%7Dspecial_notes%20product_subtitle%20satv_outofstock_note%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7Dmaximum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dprice_tiers%7Bfinal_price%7Bvalue%20__typename%7Dquantity%20__typename%7Dmedia_gallery%7Burl%20label%20disabled%20position%20full_grid_width...%20on%20ProductVideo%7Bvideo_content%7Bvideo_url%20video_title%20video_description%20video_metadata%20__typename%7D__typename%7D__typename%7Dimage%7Burl%20label%20__typename%7Dcategories%7Bbreadcrumbs%7Bcategory_id%20category_name%20__typename%7D__typename%7D...%20on%20ConfigurableProduct%7Bconfigurable_options%7Battribute_code%20attribute_id%20id%20label%20position%20values%7Bdefault_label%20label%20store_label%20use_default_value%20value_index%20swatch_data%7Btype%20value...%20on%20ImageSwatchData%7Bthumbnail%20__typename%7D__typename%7D__typename%7D__typename%7Dvariants%7Battributes%7Bcode%20value_index%20attribute_id%20label%20__typename%7Dproduct%7Bhas_fitment_info%20universal_fit%20id%20status%20price_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7Dmaximum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20currency%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dprice_tiers%7Bfinal_price%7Bvalue%20__typename%7Dquantity%20__typename%7Dproduct_subtitle%20satv_outofstock_note%20short_description%7Bhtml%20__typename%7Dfeatures%7Bhtml%20__typename%7Dfitment%7Bhtml%20__typename%7Ddescription%7Bhtml%20__typename%7Dspecial_notes%20media_gallery%7Burl%20label%20disabled%20position%20full_grid_width...%20on%20ProductVideo%7Bvideo_content%7Bvideo_url%20video_title%20video_description%20video_metadata%20__typename%7D__typename%7D__typename%7Dimage%7Burl%20label%20__typename%7Dsku%20stock_status%20related_products%7Bid%20name%20sku%20url_key%20stock_status%20status%20small_image%7Burl%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dprice_tiers%7Bfinal_price%7Bvalue%20__typename%7Dquantity%20__typename%7D__typename%7D__typename%7D__typename%7D__typename%7D...%20on%20GiftCardProduct%7Ballow_open_amount%20open_amount_min%20open_amount_max%20giftcard_type%20is_redeemable%20lifetime%20allow_message%20message_max_length%20giftcard_amounts%7Bvalue_id%20attribute_id%20value%20__typename%7D__typename%7Dmeta_title%20meta_keyword%20meta_description%20canonical_url%20product_attachments%7Bis_enabled%20is_show_tab%20tab_title%20is_show_icon%20is_show_size%20product_attachments_data%7Bicon_url%20file_url%20file_label%20size%20__typename%7D__typename%7Drelated_products%7Bid%20name%20sku%20url_key%20stock_status%20status%20small_image%7Burl%20__typename%7Dprice_range%7Bminimum_price%7Bregular_price%7Bvalue%20currency%20__typename%7Dfinal_price%7Bvalue%20__typename%7Ddiscount%7Bamount_off%20__typename%7D__typename%7D__typename%7Dprice_tiers%7Bfinal_price%7Bvalue%20__typename%7Dquantity%20__typename%7D__typename%7D%7D__typename%7D%7D&operationName=productDetail&variables=%7B%22page_size%22%3A1%2C%22urlKey%22%3A%22{product_url}%22%2C%22onServer%22%3Afalse%7D'
        return url

    def get_text_from_json_html(self, product_json, section_key):
        section_html = product_json.get(section_key, {}).get('html', '')

        if not section_html:
            return ''

        selector = Selector(text=section_html)
        raw_text = ''.join(selector.xpath('//div//text()').extract())
        soup = BeautifulSoup(raw_text, 'html.parser')
        text_content = soup.get_text()

        return text_content.strip('\n')

    def get_product_options(self, attributes: list):
        if not attributes:
            return {}

        return {option.get('code', ''): option.get('label', '') for option in attributes}

    def get_brand_name(self, product_name):
        for brand_name in self.brands_names:
            if product_name.lower().strip().startswith(brand_name.lower().strip()):
                return brand_name

        return ''

    def close(spider, reason):
        try:
            filename = 'errors.txt'

            with open(filename, 'w') as f:
                for error in spider.errors:
                    f.write(f"{error}\n")

            spider.logger.info(f"Errors written to {filename}")

        except Exception as e:
            spider.logger.error(f"Error in writing to file: {e}")