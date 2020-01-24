import uuid

import scrapy
import re
from datetime import datetime

import logging

logger = logging.getLogger()


class CastItem(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    timestamp_crawl = scrapy.Field()
    name = scrapy.Field()
    date_of_birth = scrapy.Field()
    place_of_birth = scrapy.Field()
    date_of_death = scrapy.Field()
    place_of_death = scrapy.Field()


class CastSpider(scrapy.Spider):
    name = 'cast'
    handle_httpstatus_list = [200, 301] # deal with non '/' in extracted ending
    count = 0
    download_delay = 1.0
    base_url = 'https://www.imdb.com'

    custom_settings = {
        'FEED_URI': 'cast.jl',
        'FEED_FORMAT': 'jsonlines',
        'FEED_EXPORTERS': {
            'json': 'scrapy.exporters.JsonLinesItemExporter',
        },
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_FILE': 'cast-' + datetime.now().isoformat(timespec='minutes') + '.log',
    }

    def start_requests(self):
        urls = [
            'https://www.imdb.com/search/name/?gender=male%2Cfemale',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_page(self, response):
        item = CastItem(id=str(uuid.uuid4()), url='', timestamp_crawl='', name='', date_of_birth='', place_of_birth='', date_of_death='',
                 place_of_death='')
        item['url'] = response.url
        item['timestamp_crawl'] = datetime.now().isoformat(timespec='minutes')

        cast_overview = response.css('div#name-overview-widget')
        item['name'] = cast_overview.css('span.itemprop::text').get().strip()

        born_info = cast_overview.css('div#name-born-info')
        bday = born_info.css('time a::text').getall()
        if len(bday) != 0:
            item['date_of_birth'] = " ".join(bday)
        # search for text in <a> tag with link that have birth_place as query string
        born_info_link = [href for href in born_info.css('a') if 'birth_place' in href.css('a::attr(href)').get()]
        if len(born_info_link) != 0:
            item['place_of_birth'] = born_info_link[0].css('::text').get()

        die_info = cast_overview.css('div#name-death-info')
        dday = die_info.css('time a::text').getall()
        if len(dday) != 0:
            item['date_of_death'] = " ".join(dday)
        # search for text in <a> tag with link that have death_place as query string
        # TODO - might not work if there is a place but not a link ??
        death_info_link = [href for href in die_info.css('a') if 'death_place' in href.css('a::attr(href)').get()]
        if len(death_info_link) != 0:
            item['place_of_death'] = death_info_link[0].css('::text').get()

        yield item


    def parse(self, response):
        self.count += 1

        for header_item in response.css('h3.lister-item-header'):
            links = header_item.css('a::attr(href)').getall()
            name_link_regex = r'/name/[a-zA-Z0-9]*'
            for link in links:
                match = re.search(name_link_regex, link)
                if match is not None:
                    # add '/' to handle avoid 301 if we use link not ending with '/'
                    link = link + '/' if link[-1] != '/' else link
                    yield response.follow(link, callback=self.parse_page)

        next_page = response.css('div.desc a.lister-page-next::attr(href)').get()
        if next_page is not None and self.count < 100:
            logger.info(20*'*')
            logger.info('crawling page: ' + str(self.count))
            logger.info(20*'*')
            yield response.follow(next_page, callback=self.parse)
