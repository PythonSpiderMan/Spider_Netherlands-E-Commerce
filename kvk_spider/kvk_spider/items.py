# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class KvkSpiderItem(Item):
    url = Field()
    post_code = Field()

class KvkFinalItem(Item):
    search_string = Field()
    eight_digit_kvk = Field()
    twelve_digit_kvk = Field()
    vesting_nr = Field()
    status_of_company = Field()
    headquarters = Field()
    main_company_name = Field()
    bestaande_handelsnamen = Field()
    statutaire_naam = Field()
    second_page_link = Field()
    street = Field()
    housenumber = Field()
    housenumber_extension = Field()
    postcode = Field()
    city = Field()
    extra_infomation = Field()
    second_page_company_name = Field()
    website = Field()
