import json

from datetime import datetime
from scrapy import Spider, Request
from urllib.parse import urljoin
from collections import OrderedDict


class NiceonesaSpider(Spider):
    name = "niceonesa"
    allowed_domains = ["niceonesa.com"]
    start_urls = ["https://niceonesa.com/ar/perfume"]
    base_url = 'https://niceonesa.com'

    custom_settings = {
        'FEED_EXPORTERS': {'xlsx': 'scrapy_xlsx.XlsxItemExporter', },
        'FEED_FORMAT': 'xlsx',
        'FEED_URI': f'output/Results {datetime.now().strftime("%d%m%Y%H%M%S")}.xlsx',
        'FEED_EXPORT_FIELDS': ['Title', 'Brand', 'Rating', 'Votes Count', 'ISBN', 'Size', 'Original Price',
                               'Sale Price', 'Stock Status', 'Gender', 'Base Notes', 'Middle Notes', 'Top Notes',
                               'Description', 'URL', 'Images'],

        "CONCURRENT_REQUESTS": 5,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 403, 404, 408, 429, 401]
    }

    def parse(self, response, **kwargs):
        perfume_list = response.css('#category-header + div.grid-cols-2 a')

        for perfume in perfume_list:
            url = perfume.css('a::attr(href)').get('')
            rating = perfume.css('span.text-xs::text').get('')
            reviews_count = perfume.css('.ms-1::text').re_first(r'(\d+)') or ''
            status = perfume.css('.px-2 span::text').get('')
            price = perfume.css('.font-niceone-medium::text').get('')
            brand = perfume.css('.text-boulder ::text').get('')

            yield Request(url=urljoin(self.base_url, url), callback=self.parse_details, meta={'rating': rating,
                          'count': reviews_count, 'status': status, 'price': price, 'brand': brand})

        # Pagination
        next_url = response.css('.ms-3.flex.items-center.gap-4 a ::attr(href)').get('')
        if next_url:
            yield Request(url=urljoin(self.base_url, next_url), callback=self.parse)

    def parse_details(self, response):
        item = OrderedDict()
        
        item['URL'] = response.url
        item['Title'] = response.css('.text-xl::text').get('')
        item['Sale Price'] = response.css('.whitespace-nowrap::text').get('') or response.meta.get('price')
        item['Original Price'] = response.css('.line-through::text').get('')
        item['Rating'] = response.meta.get('rating')
        item['Votes Count'] = response.meta.get('count')
        item['Brand'] = response.meta.get('brand')
        item['Stock Status'] = response.css('[property="product:availability"] ::attr(content)').get('')
        item['Description'] = ''.join([text.strip() for text in response.css('.product-details ::text').getall() if text.strip()]).strip()

        data = json.loads(response.css('#__NUXT_DATA__ ::text').get())

        item['Images'] = self.get_image(data, response)
        item['ISBN'] = json.loads(response.css('[type="application/ld+json"]:contains("https://schema.org/") ::text').get()).get('isbn')
        item.update(self.get_specifications(data))

        sizes = self.get_size_variants(data)
        if sizes:
            images = item['Images']

            for size in sizes:
                item['Images'] = f'{data[data[size].get('image')]}, {images}'
                item['Size'] = data[data[size].get('name')]
                item['Sale Price'] = data[data[size].get('price_formated')]
                item['Original Price'] = ''
                yield item

        else:
           yield item

    def get_specifications(self, data):
        specification_items = dict()

        for i, d in enumerate(data):
            if isinstance(d, dict):
                if d.get('المكونات العليا'):
                    specification_items['Top Notes'] = data[d.get('المكونات العليا')]

                if d.get('المكونات الوسطى'):
                    specification_items['Middle Notes'] = data[d.get('المكونات الوسطى')]

                if d.get('مكونات القاعدة'):
                    specification_items['Base Notes'] = data[d.get('مكونات القاعدة')]

                if d.get('الجنس'):
                    specification_items['Gender'] = data[d.get('الجنس')]

        return specification_items

    def get_size_variants(self, data):
        for i, d in enumerate(data):
            if d == 'Size':
                return data[i + 1]

    def get_image(self, data, response):
        for i, d in enumerate(data):
            if d == response.css('#img-container img ::attr(src)').get(''):
                image_indexes = data[i + 1]
                break

        images = []
        if isinstance(image_indexes, list):
            for index in image_indexes:
                images.append(data[index])

        else:
            return response.css('#img-container img ::attr(src)').get('')

        return ', '.join(images)
