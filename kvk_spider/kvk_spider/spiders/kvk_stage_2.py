# -*- coding: utf-8 -*-
from __future__ import print_function
from peewee import *
import re
import json
from w3lib.url import url_query_parameter
import scrapy
from ..items import KvkFinalItem


class KvkStage2Spider(scrapy.Spider):
    name = 'kvk_stage_2'
    allowed_domains = ['kvk.nl']

    def start_requests(self):
        db = SqliteDatabase("temp_kvk_1.db")
        db.connect()

        class BaseModel(Model):
            class Meta:
                database = db

        class kvkFirstStageModel(BaseModel):
            url = CharField(unique=True)
            post_code = CharField(unique=False)

        first_stage_results = kvkFirstStageModel.select()

        for result in first_stage_results:
            yield scrapy.Request(url=result.url, callback=self.parse)

    def parse(self, response):
        response_url = response.url
        json_obj = json.loads(response.body_as_unicode())

        for company_obj in json_obj['entries']:
            item = KvkFinalItem()

            print("=" * 30)
            print("Parsing Company information -- Stage 2")
            search_string = url_query_parameter(response_url, 'q')
            item['search_string'] = search_string
            print("********Search string: `%s`********" % (search_string))

            try:
                item['eight_digit_kvk'] = str(company_obj['dossiernummer']).strip()
            except Exception as e:
                print("8 Digit Corrupted. ")

            try:
                twelve_digit_kvk = url_query_parameter(str(company_obj['href']), 'kvknummer')
                item['twelve_digit_kvk'] = twelve_digit_kvk
                if len(twelve_digit_kvk) <= 11:
                    item['twelve_digit_kvk'] = str(twelve_digit_kvk) + "0000"
            except Exception as e:
                print("12 Digit Corrupted. ")

            try:
                item['vesting_nr'] = str(company_obj['vestigingsnummer'])
            except Exception as e:
                print("Vesting NR Corrupted")

            try:
                item['status_of_company'] = str(company_obj['status']).strip()
            except Exception as e:
                print("This company do not have a status. ")

            try:
                if (company_obj['type']).strip().startswith("H") is True:
                    item['headquarters'] = True
                else:
                    item['headquarters'] = False
            except Exception as e:
                item['headquarters'] = False
                print("This company is not a headquater. ")

            try:
                item['main_company_name'] = str(company_obj['handelsnaam'])
                print("********This company name is `%s`********" % str(company_obj['handelsnaam']))
            except Exception as e:
                print("Main company name Corrupted. ")

            try:
                for meerinfo in company_obj['meerinfo']:
                    if meerinfo['titel'].startswith("Be") is True:
                        item['bestaande_handelsnamen'] = str(meerinfo['tekst'])
                    else:
                        pass
            except Exception as e:
                print("This company do not have a Bestaande handelsnamen. ")


            try:
                for meerinfo in company_obj['meerinfo']:
                    if meerinfo['titel'].startswith("Stat") is True:
                        item['statutaire_naam'] = str(meerinfo['tekst'])
                    else:
                        pass
            except Exception as e:
                print("This company do not have a Statutaire naam. ")


            try:
                item['second_page_link'] = "https://www.kvk.nl/" + str(company_obj['href'])
            except Exception as e:
                print("Second Page Link Corrupted. ")

            try:
                item['street'] = str(company_obj['straat'])
            except Exception as e:
                print("This company's location do not have a street. ")

            try:
                item['housenumber'] = str(company_obj['huisnummer'])
            except Exception as e:
                print("This company's location do not have a house number. ")

            try:
                item['housenumber_extension'] = "No extension"
                item['housenumber_extension'] = str(company_obj['huisnummertoevoeging'])
            except Exception as e:
                print("This company's house number do not have an extension. ")

            try:
                item['postcode'] = str(company_obj['postcode'])
            except Exception as e:
                print("This company do not have a postcode. ")

            try:
                item['city'] = str(company_obj['plaats'])
            except Exception as e:
                print("This company's location do not have a city string. ")

            try:
                item['extra_infomation'] = str(company_obj['snippet'])
            except Exception as e:
                print("This company do not include extra information. ")

            try:
                item['second_page_company_name'] = str(company_obj['handelsnaam'])
            except Exception as e:
                print("Second page company name corrupted. ")

            try:
                website = re.search(
                    r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9]\.[^\s]{2,})',
                    company_obj['snippet'],
                    flags=re.IGNORECASE).group(0)
                if website is not None:
                    item['website'] = str(website)
                else:
                    print("This company do not have a website information. ")
            except:
                print("This company do not have a website information. ")

            yield item
            print("+" * 30)

