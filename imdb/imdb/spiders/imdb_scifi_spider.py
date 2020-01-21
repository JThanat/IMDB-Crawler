import scrapy
import uuid
import re
from datetime import datetime

from scrapy.exporters import JsonItemExporter, JsonLinesItemExporter


class MovieItem(scrapy.Item):
    id = scrapy.Field()
    url = scrapy.Field()
    timestamp_crawl = scrapy.Field()
    title = scrapy.Field()
    release_date = scrapy.Field()
    budget = scrapy.Field()
    gross_usa = scrapy.Field()
    runtime = scrapy.Field()

    def print_item(self):
        print(self['title'])
        print(self['release_date'])
        print(self['budget'])
        print(self['gross_usa'])
        print(self['runtime'])


class IMDBSpider(scrapy.Spider):
    name = "IMDB"
    base_url = 'https://www.imdb.com'
    download_delay = 1.0
    count = 0

    def start_requests(self):
        urls = [
            'https://www.imdb.com/search/title/?genres=sci-fi&view=simple&sort=num_votes,desc&explore=title_type,genres',
            # 'https://www.imdb.com/search/title/?genres=sci-fi&view=simple&sort=num_votes,desc&start=3509&explore=title_type,genres&ref_=adv_nxt'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse_page(self, response):
        item = MovieItem(id=str(uuid.uuid4()), url='', timestamp_crawl='', title='', release_date='', budget='', gross_usa='',
                         runtime='')
        item['url'] = response.url
        item['timestamp_crawl'] = datetime.now().isoformat(timespec='minutes')
        item['title'] = response.css('div.title_wrapper h1::text').get().strip()
        texts = response.css('div#titleDetails.article div.txt-block')
        for text in texts:
            t = text.css("h4::text").get()
            if t == 'Release Date:':
                d = "".join(text.css("div::text").getall()).strip()
                date_regex = r'([0-9]{0,2} )?[a-zA-z]* [0-9]{4}'  # matching example: '19 January 2010' or 'December 2009'
                match = re.match(date_regex, d)
                if match is not None:
                    item['release_date'] = d[match.start(): match.end()] if match is not None else ''
            elif t == 'Budget:':
                item['budget'] = "".join(text.css("div::text").getall()).strip()
            elif t == 'Gross USA:':
                item['gross_usa'] = "".join(text.css("div::text").getall()).strip()
            elif t == 'Runtime:':
                rt = "".join(text.css("div time::text").getall()).strip()
                match = re.match(r'[0-9]* min', rt)
                if match is not None:
                    item['runtime'] = rt[match.start():match.end()]
        yield item

    def parse(self, response):
        self.count += 1

        for span in response.css('span.lister-item-header'):
            links = span.css('a::attr(href)').getall()
            if len(links) == 2:
                # We should go to episode page instead of the movie page
                yield response.follow(links[1], callback=self.parse_page)
            else:
                yield response.follow(links[0], callback=self.parse_page)

        next_page = response.css('div.desc a.lister-page-next::attr(href)').get()
        if next_page is not None and self.count < 100:
            yield response.follow(next_page, callback=self.parse)
