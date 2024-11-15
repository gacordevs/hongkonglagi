import scrapy
import pymysql
from datetime import datetime

class NumberSpider(scrapy.Spider):
    name = 'number'
    allowed_domains = ['hongkonglotto.com']
    start_urls = ['https://hongkonglotto.com/update-loadball']

    def __init__(self, *args, **kwargs):
        super(NumberSpider, self).__init__(*args, **kwargs)  

    def parse(self, response):
        """Parse the response and extract the numbers."""
        first_place_numbers = []
        second_place_numbers = []
        third_place_numbers = []

        # Extract data for first, second, and third places
        for first in response.css('div[data-id="2526:6087"]'):
            first_place = first.css('div.frame-42234')
            number_1 = first_place.css('img::attr(alt)').getall()
            clean_number1 = [n.replace("Property 1=", "").replace(",", "") for n in number_1]
            first_place_numbers = clean_number1

        for second in response.css('div[data-id="2526:6088"]'):
            second_place = second.css('div.frame-42234')
            number_2 = second_place.css('img::attr(alt)').getall()
            clean_number2 = [n.replace("Property 1=", "").replace(",", "") for n in number_2]
            second_place_numbers = clean_number2

        for third in response.css('div[data-id="2526:6106"]'):
            third_place = third.css('div.frame-42234')
            number_3 = third_place.css('img::attr(alt)').getall()
            clean_number3 = [n.replace("Property 1=", "").replace(",", "") for n in number_3]
            third_place_numbers = clean_number3

        # Get current date and time
        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Yield the data to be processed in the pipeline
        yield {
            'date': current_date,
            'first': first_place_numbers,
            'second': second_place_numbers,
            'third': third_place_numbers
        }
