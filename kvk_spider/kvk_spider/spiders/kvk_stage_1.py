# -*- coding: utf-8 -*-
from __future__ import print_function
import pandas as pd
import scrapy
import logging
import w3lib.url
import json
import copy
import w3lib.url
from w3lib.url import url_query_parameter
from ..items import KvkSpiderItem


"""Please start the scrapper at scrapy.cfg's directory"""
class KvkStage1Spider(scrapy.Spider):
    name = 'kvk_stage_1'
    allowed_domains = ['kvk.nl']
    request_url = "https://zoeken.kvk.nl/JsonSearch.ashx?q=1100AC&prefproduct=&prefpayment=&start=0"

    def start_requests(self):
        df = None
        try:
            df = pd.read_csv("../input.csv")
        except Exception as e:
            logging.error(e)
            logging.error("Please check your input csv file, I can't find it. ")
            quit(1)

        post_code_list = df["post_code"]

        start_urls = []
        for post_code in post_code_list:
            url = w3lib.url.add_or_replace_parameter(self.request_url, "q", post_code)
            start_urls.append(copy.deepcopy(url))

        for target in start_urls:
            yield scrapy.Request(url=target, callback=self.parse)

    def parse(self, response):
        json_obj = json.loads(response.body_as_unicode())

        total_counts = int(int(json_obj['pageinfo']['resultscount'])/10)+1
        post_code = url_query_parameter(response.url, 'q')

        print("="*30)
        print("post code `%s` has `%s` first pages" % (post_code, str(total_counts)))
        print("+"*30)

        urls_of_post_code = []
        for i in range(0, total_counts):
            final_url = w3lib.url.add_or_replace_parameter(response.url, "start", str(i * 10))
            urls_of_post_code.append(copy.deepcopy(final_url))

        for url_with_post_code in urls_of_post_code:
            item = KvkSpiderItem()
            item['url'] = url_with_post_code
            item['post_code'] = post_code
            yield item
