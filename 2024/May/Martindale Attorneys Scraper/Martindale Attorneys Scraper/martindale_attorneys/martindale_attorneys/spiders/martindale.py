import json
from collections import OrderedDict
from datetime import datetime

from scrapy import Spider, Request, signals


def get_scrapeops_api_key_from_file():
    with open('input/scrapeops_api_key.txt', mode='r', encoding='utf-8') as txt_file:
        return ''.join([line.strip() for line in txt_file.readlines() if line.strip()][:1])


class MartindaleSpider(Spider):
    name = 'martindale'
    base_url = 'https://www.martindale.com/by-location/texas-lawyers/'

    scrapeops_api_key = get_scrapeops_api_key_from_file()

    custom_settings = {
        'CONCURRENT_REQUESTS': 3,

        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 400, 402, 403, 404, 408, 429, 484, 10051],

        'SCRAPEOPS_API_KEY': scrapeops_api_key,
        'SCRAPEOPS_PROXY_ENABLED': True if scrapeops_api_key else False,

        'DOWNLOADER_MIDDLEWARES': {
            'scrapeops_scrapy_proxy_sdk.scrapeops_scrapy_proxy_sdk.ScrapeOpsScrapyProxySdk': 725,
        },

        'FEEDS': {
            f'output/MartinDale Attorneys {datetime.now().strftime("%d%m%Y%H%M")}.csv': {
                'format': 'csv',
                'fields': ['Name', 'Firm', 'Position at Firm', 'Address', 'City', 'State', 'Area of Practice',
                           'Website URL', 'Phone', 'Tagline', 'URL'],
            },
        }
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.search_urls = self.get_search_urls_from_file()
        self.items_scraped_count = 0

    def start_requests(self):
        yield Request(url=self.base_url, meta={'handle_httpstatus_all': True})

    def parse(self, response, **kwargs):
        search_url = self.search_urls.pop(0)

        yield Request(url=search_url, callback=self.parse_cities)

    def parse_cities(self, response):
        cities = response.css('.tabs-content .browse-list__a--grey.navigable')

        for city in cities:
            city_url = city.css('::attr(href)').get('')

            if not city_url:
                continue

            city_name = city.css('::attr(title)').re_first(r' in (.*)') or city.css(' ::text').get('').strip()

            yield Request(url=city_url, callback=self.parse_attorneys, meta={'city': city_name})

    def parse_attorneys(self, response):
        practice_area = response.css('.results__title ::text').re_first('(.*) Results', '').strip()
        attorneys = response.css('.results__result-row .card')

        for attorney in attorneys:
            attorney_url = attorney.css('.detail_title a::attr(href)').get('')

            if not attorney_url:
                continue

            city_state = response.meta.get('city').split(',')

            item = OrderedDict()
            item['Name'] = attorney.css("h3 ::text").get('')
            item['Firm'] = attorney.css('.detail_position a ::text').get('')
            item['Position at Firm'] = attorney.css('.detail_position::text').re_first('(.*)at', '').strip()
            #     #item['Address'] = response.css('div.experience-label:contains("Mailing Address:") + div.experience-value ::text').get('').strip() or response.css('address ::text').get('')
            item['City'] = city_state[0]
            item['State'] = city_state[1]
            item['Area of Practice'] = practice_area
            item['Website URL'] = attorney.css(".webstats-website-click ::attr(href)").get('')
            item['Phone'] = attorney.css(".webstats-phone-click ::attr(href)").re_first(r'tel:(.*)', '') or ''
            item['Tagline'] = attorney.css('.detail_tagline ::text').get('')
            item['URL'] = attorney_url

            self.items_scraped_count += 1

            print(f'\n\nItems Scraped Count: {self.items_scraped_count}\n\n')

            yield item

            # yield Request(url=attorney_url, callback=self.parse_details, meta=response.meta)

        next_page = response.css('.pagination [rel="next"]::attr(href)').get('')
        if next_page and 'javascript:' not in next_page:
            try:
                yield Request(url=next_page, callback=self.parse_attorneys, meta=response.meta)
            except ValueError as e:
                pass

    # def parse_details(self, response):
    #     meta = response.meta
    #
    #     firm_json = self.get_page_json(response.css('script:contains("displayFirmName") ::text'))
    #     contact_json = self.get_page_json(response.css('script:contains("tagline")::text'))
    #
    #     item = OrderedDict()
    #     item['Name'] = response.css('.profile-title--bold ::text').get('').strip()
    #     item['Firm'] = meta.get('firm') or firm_json.get('displayFirmName', '') or ''
    #     item['Position at Firm'] = meta.get('position') or firm_json.get('title', '')
    #     item['Address'] = response.css('div.experience-label:contains("Mailing Address:") + div.experience-value ::text').get('').strip() or response.css('address ::text').get('')
    #     item['City'] = meta.get('city')
    #     item['Area of Practice'] = meta.get('practice_area')
    #     item['Website URL'] = response.css('.navigable.view-website ::attr(href)').get('') or contact_json.get('websiteUrl', '')
    #     item['Phone'] = response.css('.navigable.webstats-phone-click::attr(href)').re_first(r'tel:(.*)', '') or contact_json.get('mainPhoneNumber', '') or ''
    #     item['Tagline'] = contact_json.get('tagline', '').replace('&amp;amp;', '&') or ''
    #     item['URL'] = meta.get('url')
    #
    #     self.items_scraped_count += 1
    #
    #     print(f'\n\nItems Scraped Count: {self.items_scraped_count}\n\n')
    #
    #     yield item

    def get_page_json(self, js_script_selector):
        try:
            return json.loads(js_script_selector.re_first(r"fixBadControlCharacters\(`(.*)`\)\);", "").replace('\\', '')) or {}
        except json.JSONDecodeError:
            return {}

    def get_search_urls_from_file(self):
        with open('input/search_urls.txt', mode='r', encoding='utf-8') as txt_file:
            return [line.strip() for line in txt_file.readlines() if line.strip()]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(MartindaleSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        if not self.search_urls:
            return

        req = Request(url=self.base_url,
                      callback=self.parse,
                      dont_filter=True,
                      meta={'handle_httpstatus_all': True})

        try:
            self.crawler.engine.crawl(req)  # For latest Python version
        except TypeError:
            self.crawler.engine.crawl(req, self)  # For old Python version < 10
