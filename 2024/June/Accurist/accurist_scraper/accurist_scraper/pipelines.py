# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import scrapy

from scrapy.pipelines.images import ImagesPipeline
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class CustomImagePipeline(ImagesPipeline):
    def get_media_requests(self, item, info):
        for index, image_url in enumerate(item["image_urls"], start=1):
            yield scrapy.Request(image_url, meta={'index': index})

    def file_path(self, request, response=None, info=None, *, item=None):
        title = item['SKU']
        index = request.meta['index']
        filename = f'{title}({index}).jpg'

        return filename
