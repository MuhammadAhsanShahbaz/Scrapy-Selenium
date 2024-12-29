import re
from collections import OrderedDict
from datetime import datetime

from scrapy import Spider, Request


class AccuristSpider(Spider):
    name = "accurist"
    start_urls = ["https://www.accurist.com/"]

    current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
    custom_settings = {
        'CLOSESPIDER_ITEMCOUNT': 10,
        'FEED_FORMAT': 'csv',
        'FEED_URI': f'Output/{current_datetime}/accurist.csv',
        'FEED_EXPORT_FIELDS': ['Title', 'Category', 'Description', 'Specifications', 'SKU', 'Price', 'URL',
                               'Images_URL', 'Gender']
    }

    def start_requests(self):
        yield Request(url=self.start_urls[0], callback=self.parse)

    def parse(self, response, **kwargs):
        item_urls = response.css('div.wrap-m a::attr(href)').getall()

        for url in item_urls:
            if 'ladies-watches' in url or 'mens-watches' in url:
                yield Request(url=url, callback=self.parse_listing_items)

    def parse_listing_items(self, response):
        listing_items_url = response.css('a.o1.m-t-2::attr(href)').getall()

        for url in listing_items_url:
            full_url = response.urljoin(url)
            yield Request(url=full_url, callback=self.parse_detail,
                          meta={'full_url': full_url})

    def parse_detail(self, response):
        item = OrderedDict()

        title = response.css('.m-t-0-s::text').get('')
        item['URL'] = response.meta.get('full_url')
        item['Title'] = title
        item['Category'] = response.css('.m-b-0-s::text').get('')
        item['SKU'] = response.css('.w-6.p2::text').get('')
        item['Price'] = response.css('.fw-bold::text').get('')
        item['Description'] = "".join(response.css('.p1.col-12.p-t::text').getall())
        item['Specifications'] = "".join(response.css('.p-b-5 ::text').getall())
        item['image_urls'] = self.get_pictures(response)

        name = ['Men', 'Ladies', 'Unisex']

        for n in name:
            pattern = rf'{n}'
            gender = response.css('.m-t-0-s::text').re(pattern)

            if gender:
                item['Gender'] = gender[0]
                break

        item['Images_URL'] = ', '.join(self.get_pictures(response))

        yield item

    def get_pictures(self, response):
        image_urls = []
        urls = response.css('.bg-col-51.ratio-1-1 img ::attr("imagefakesrc")').re(r'https://[^\'"]+')

        if urls:
            for url in urls:
                image_urls.append(url)

        return image_urls