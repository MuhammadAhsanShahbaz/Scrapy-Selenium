import json
import urllib.parse
import re

from datetime import datetime
from scrapy import Spider, Request
from urllib.parse import urljoin
from collections import OrderedDict


class SekondaSpider(Spider):
    name = "sekonda"
    allowed_domains = ["www.sekonda.com"]
    start_urls = ["https://www.sekonda.com"]
    base_url = 'https://www.sekonda.com'

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/json',
        'origin': 'https://www.sekonda.com',
        'sec-ch-ua': '"Opera GX";v="109", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 OPR/109.0.0.0',
        'x-store-id': '3',
    }

    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': f'output/{datetime.now().strftime('%Y%m%d_%H%M%S')}/output.csv',
        'FEED_EXPORT_FIELDS': ['Title', 'Category', 'Description', 'Specifications', 'SKU', 'Original Price',
                               'Sale Price', 'Availability', 'URL', 'Images_URL', 'Gender']
    }

    sku = []
    extera_urls = ['/womens-watches/heritage', '/womens-watches/jewellery', '/mens-watches/modern']

    def parse(self, responses, **kwargs):
        params = {
            'requests': '[{"type":"block","filter":{"url":"page-footer"},"verbosity":3,"action":"find","children":[{"_reqId":0}]},{"type":"block","filter":{"url":"newsletter-footer"},"verbosity":3,"action":"find","children":[{"_reqId":1}]},{"type":"block","filter":{"url":"submenu-modal-id@meganav0"},"verbosity":3,"action":"find","children":[{"_reqId":2}]}]',
            'pushDeps': 'true',
        }

        base_url = 'https://www.sekonda.com/api/n/bundle'
        query_string = urllib.parse.urlencode(params)
        url = f"{base_url}?{query_string}"

        yield Request(url, callback=self.parse_filter_urls)

    def parse_filter_urls(self, response):
        paths = []
        try:
            blocks = response.json().get('catalog', [])[1].get('content', {}).get('menu_links', [])

            for block in blocks:
                data = block.get('level_2_block', [])

                if data:
                    try:
                        data = data[0].get('collection_items', '')
                        if data:
                            for d in data:
                                if d.get('cta_link', ''):
                                    paths.append(d.get('cta_link', ''))

                    except IndexError:
                        pass
                else:
                    paths.append(block.get('link'))

        except IndexError:
            pass

        for path in (paths + self.extera_urls):
            json_data = {
                'requests': [
                    {
                        'action': 'route',
                        'children': [
                            {
                                'path': path,
                                'pushDeps': True,
                                '_reqId': 0,
                            },
                        ],
                    },
                ],
                'pushDeps': True,
            }
            yield Request(url='https://www.sekonda.com/api/n/bundle', method='POST',
                          body=json.dumps(json_data), callback=self.get_detail_page_urls,
                          headers=self.headers)

    def get_detail_page_urls(self, response):
        urls = []
        blocks = response.json().get('catalog', [])

        if blocks:
            for block in blocks:
                if block.get('listing_page_name', ''):
                    urls.append(block.get('url', ''))

            for url in urls:
                yield Request(url=f'{urljoin(self.base_url, url)}/__data', headers=self.headers,
                              callback=self.get_detail_of_product, meta={'handle_httpstatus_list': [404]})

    def get_detail_of_product(self, response):
        if response.status == 404:
            yield self.getting_response_for_errors(response)

        else:
            try:
                data = json.loads(response.text.split('var __transferData=')[1].rstrip(';'))
                variants = data.get('assets', {}).get('product', [])

                for variant in variants:
                    try:
                        # if response.url.split('.com')[1][:21] in variant.get('url', ''):
                        item = OrderedDict()

                        item.update(self.parse_data(variant))

                        if item['SKU'] not in self.sku:
                            self.sku.append(item['SKU'])

                            yield item

                    except IndexError:
                        continue

            except IndexError:
                pass

    def getting_response_for_errors(self, response):
        pattern = r'(\/p\/[a-zA-Z0-9\-]+)'
        json_data = {
            'requests': [
                {
                    'action': 'route',
                    'children': [
                        {
                            'path': re.search(pattern, response.url).groups(0),
                            'pushDeps': True,
                            '_reqId': 0,
                        },
                    ],
                },
            ],
            'pushDeps': True,
        }
        return Request(url='https://www.sekonda.com/api/n/bundle', method='POST', body=json.dumps(json_data),
                       callback=self.getting_products_data, headers=self.headers)

    def getting_products_data(self, response):
        try:
            data = response.json().get('catalog', [])[0]
            item = OrderedDict()
            item.update(self.parse_data(data))

            if item['SKU'] not in self.sku:
                self.sku.append(item['SKU'])

                yield item

        except IndexError:
            pass

    def parse_data(self, data):
        item = OrderedDict()
        item['Title'] = data.get('name', '')
        item['URL'] = f"{self.base_url}{data.get('url', '')}"
        item['Category'] = data.get('item_category4', '')
        item['SKU'] = data.get('sku', '')
        item['Description'] = data.get('description', '')
        item['Specifications'] = re.sub(r'\n+', '\n', (re.sub(r'<.*?>', '\n', data.
                                                              get('product_features', '').replace('\r', '')))).strip()\
            if data.get('product_features', '') else ''
        item['Images_URL'] = self.get_images(data.get('media', []))
        item['Gender'] = data.get('item_category')
        item['Availability'] = data.get('meta').get('availability')

        item.update(self.get_price(data))

        return item

    def get_images(self, data):
        images = []

        for d in data:
            images.append(f"https://www.sekonda.com/static/media/catalog{d.get('image', '')}")

        return ', '.join(images)

    def get_price(self, data):
        price = OrderedDict()
        price1 = data.get('org_price', '')
        price2 = data.get('price', '')

        if price1 and price2:
            price['Original Price'] = price1
            price['Sale Price'] = price2

        else:
            price['Original Price'] = price2

        return price

