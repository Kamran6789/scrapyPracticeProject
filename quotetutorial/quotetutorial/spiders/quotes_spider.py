import scrapy
from scrapy.http import FormRequest
from ..items import QuotetutorialItem
from scrapy.utils.response import open_in_browser


class QuoteSpider(scrapy.Spider):
    name = 'quotes'
    start_urls = ['https://quotes.toscrape.com/login']

    def parse(self, response):
        token = response.css('form input::attr(value)').extract_first()
        return FormRequest.from_response(response, formdata={
            'csrf_token': token,
            'username': 'kamran@gmail.com',
            'password': 'dtasygd',

        }, callback=self.start_scraping)

    def start_scraping(self, response):
        open_in_browser(response)
        items = QuotetutorialItem()

        all_div_quotes = response.css('div.quote')

        for quote in all_div_quotes:
            title = quote.css('span.text::text').extract()
            author = quote.css('small.author::text').extract()
            tag = quote.css('a.tag::text').extract()

            items['title'] = title
            items['author'] = author
            items['tag'] = tag

            yield items
