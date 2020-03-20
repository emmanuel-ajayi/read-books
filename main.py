# -*- coding: utf-8 -*-
"""

"""
import csv
import logging
import re
from datetime import datetime

import boto3
import scrapy
from botocore.exceptions import ClientError
from scrapy.crawler import CrawlerProcess
logger = logging.getLogger(__name__)


class BooksCrawler(scrapy.Spider):
    name = "books"
    base_url = 'http://books.toscrape.com/'
    crawled_results = []
    allowed_domains = ["books.toscrape.com"]
    current_iteration = 1
    total_iterations = 50

    def close(self, spider, reason):
        """
        Once the 20 crawls completes, we push to s3
        :param spider:
        :param reason:
        :return:
        """
        if self.current_iteration <= self.total_iterations:
            with open("data.csv", "w", newline='') as f:
                writer = csv.writer(f)
                writer.writerows(self.crawled_results)
            self.write_to_s3()

        closed = getattr(spider, 'closed', None)
        if callable(closed):
            return closed(reason)

    def start_requests(self):
        self.detect_and_set_next_iteration_via_s3()
        url = "http://books.toscrape.com/catalogue/page-" + str(self.current_iteration) + ".html"
        if self.current_iteration <= self.total_iterations:
            yield scrapy.Request(url, callback=self.parse)
        else:
            logger.info("Scrapping completed and cron removed")

    def parse(self, response):

        locations = response.xpath('//article[@class="product_pod"]/h3/a/@href').extract()
        for location in locations:
            absolute_url = self.base_url + "catalogue/" + location
            yield scrapy.Request(absolute_url, callback=self.parse_attr)

    def parse_attr(self, response):
        image_path = self.check_field_exists(
            response.xpath('//article[@class="product_page"]//div[@class="carousel-inner"]//img/@src').extract())
        if image_path.startswith("../../"):
            image_path = self.base_url + image_path.replace("../../", "")
        title = self.check_field_exists(
            response.xpath('//article[@class="product_page"]//div[@class="col-sm-6 product_main"]/h1/text()').extract())
        description = self.check_field_exists(response.xpath('//article[@class="product_page"]/p/text()').extract())
        other_info = response.xpath('//article[@class="product_page"]//table[@class="table table-striped"]/tr')
        upc = self.check_field_exists(other_info[0].xpath('td/text()').extract())
        product_type = self.check_field_exists(other_info[1].xpath('td/text()').extract())
        price_exc = self.check_field_exists(other_info[2].xpath('td/text()').extract())
        price_inc = self.check_field_exists(other_info[3].xpath('td/text()').extract())
        tax = self.check_field_exists(other_info[4].xpath('td/text()').extract())
        availability = self.check_field_exists(other_info[5].xpath('td/text()').extract())
        num_reviews = self.check_field_exists(other_info[6].xpath('td/text()').extract())

        result = [title, image_path, description, upc, product_type, price_exc, price_inc, tax, availability,
                  num_reviews]
        self.crawled_results.append(result)

    def check_field_exists(self, field):
        return field[0] if len(field) else ""

    def write_to_s3(self):
        """Upload to S3 bucket.

        :return: True if file was uploaded, else False
        """
        s3_client = boto3.client('s3', region_name='us-east-1')
        try:
            file_name = "all_product_books_" + str(self.current_iteration) + "_of_" + str(
                self.total_iterations) + "_" + str(datetime.now().strftime("%Y-%m-%d-%H-%M")) + ".csv"
            s3_client.upload_file("data.csv", "lavamap-test", file_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def detect_and_set_next_iteration_via_s3(self):
        s3_client = boto3.client('s3', region_name='us-east-1')
        try:
            response = s3_client.list_objects(
                Bucket='lavamap-test'
            )
            if "Contents" in response:
                new_response = sorted(response["Contents"], key=lambda k: k['LastModified'])
                last_uploaded_file_name = new_response[-1]["Key"]
                match = re.match(".*_books_(\d+)_of_.*", last_uploaded_file_name)
                if match:
                    self.current_iteration = int(match.groups()[0]) + 1

        except ClientError as e:
            logger.error(e)
            return False
        return True


def run_crawler():
    process = CrawlerProcess({
        'USER_AGENT': 'Lavamap 1.0'
    })
    process.crawl(BooksCrawler)
    process.start()


if __name__ == "__main__":
    run_crawler()