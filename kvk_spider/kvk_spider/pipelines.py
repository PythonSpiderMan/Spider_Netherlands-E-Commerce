# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase
from .items import KvkSpiderItem, KvkFinalItem
import logging


db = SqliteDatabase("temp_kvk_1.db")
db.connect()
class BaseModel(Model):
    class Meta:
        database = db
class kvkFirstStageModel(BaseModel):
    url = CharField(unique=True)
    post_code = CharField(unique=False)
db.close()


final_db = SqliteDatabase("kvk_final.db")
final_db.connect()
class BaseModel(Model):
    class Meta:
        database = final_db
class kvkItemModel(BaseModel):
    search_string = CharField()
    eight_digit_kvk = CharField(null=True, unique=True)
    twelve_digit_kvk = CharField(null=True)
    vesting_nr = CharField(null=True)
    status_of_company = CharField(null=True)
    headquarters = BooleanField(null=True)
    main_company_name = CharField(null=True)
    bestaande_handelsnamen = CharField(null=True)
    statutaire_naam = CharField(null=True)
    second_page_link = CharField(null=True)
    street = CharField(null=True)
    housenumber = CharField(null=True)
    housenumber_extension = CharField(null=True)
    postcode = CharField(null=True)
    city = CharField(null=True)
    extra_infomation = CharField(null=True)
    second_page_company_name = CharField(null=True)
    website = CharField(null=True)
final_db.close()


items = []


class KvkSpiderPipeline(object):
    def close_spider(self, spider):
        global items
        for item in items:
            item.save()
        print("items saved! ")

    def open_spider(self, spider):
        if spider.name == "kvk_stage_1":
            print('This is stage 1 spider')
            db = SqliteDatabase("temp_kvk_1.db")
            db.connect()
            try:
                db.drop_tables([kvkFirstStageModel])
            except:
                pass
            db.create_tables([kvkFirstStageModel])
            db.close()

        elif spider.name == "kvk_stage_2":
            print("This is stage 2 spider")
            final_db = SqliteDatabase("kvk_final.db")
            final_db.connect()
            try:
                final_db.drop_tables([kvkItemModel])
            except:
                pass
            final_db.create_tables([kvkItemModel])
            print("=" * 30)
            print("Final sqlite database successfully inited!")
            print("=" * 30)


    def process_item(self, item, spider):
        if isinstance(item, KvkSpiderItem):
            kvk_item = kvkFirstStageModel()
            kvk_item.url = item['url']
            kvk_item.post_code = item['post_code']

            try:
                global items
                items.append(kvk_item)
                logging.info("This is a valid record. ")

                if len(items) >= 100:
                    for item in items:
                        print("Performing IO Operations. ")
                        item.save()
                    items = []
                else:
                    pass

            except Exception as e:
                logging.error(e)
                logging.info("duplicate record in first stage spider. ")

        elif isinstance(item, KvkFinalItem):
            item.setdefault('search_string', 'No Value')
            item.setdefault('eight_digit_kvk', 'No Value')
            item.setdefault('twelve_digit_kvk', 'No Value')
            item.setdefault('vesting_nr', 'No Value')
            item.setdefault('status_of_company', 'No Value')
            item.setdefault('headquarters', 'No Value')
            item.setdefault('main_company_name', 'No Value')
            item.setdefault('bestaande_handelsnamen', 'No Value')
            item.setdefault('statutaire_naam', 'No Value')
            item.setdefault('second_page_link', 'No Value')
            item.setdefault('street', 'No Value')
            item.setdefault('housenumber', 'No Value')
            item.setdefault('housenumber_extension', 'No Value')
            item.setdefault('postcode', 'No Value')
            item.setdefault('city', 'No Value')
            item.setdefault('extra_infomation', 'No Value')
            item.setdefault('second_page_company_name', 'No Value')
            item.setdefault('website', 'No Value')

            final_item = kvkItemModel()
            final_item.search_string = item['search_string']
            final_item.eight_digit_kvk = item['eight_digit_kvk']
            final_item.twelve_digit_kvk = item['twelve_digit_kvk']
            final_item.vesting_nr = item['vesting_nr']
            final_item.status_of_company = item['status_of_company']
            final_item.headquarters = item['headquarters']
            final_item.main_company_name = item['main_company_name']
            final_item.bestaande_handelsnamen = item['bestaande_handelsnamen']
            final_item.statutaire_naam = item['statutaire_naam']
            final_item.second_page_link = item['second_page_link']
            final_item.street = item['street']
            final_item.housenumber = item['housenumber']
            final_item.housenumber_extension = item['housenumber_extension']
            final_item.postcode = item['postcode']
            final_item.city = item['city']
            final_item.extra_infomation = item['extra_infomation']
            final_item.second_page_company_name = item['second_page_company_name']
            final_item.website = item['website']

            try:
                global items
                items.append(final_item)
                logging.info("This is a valid record. ")

                if len(items) >= 100:
                    for item in items:
                        print("Performing IO Operations. ")
                        item.save()
                    items = []
                else:
                    pass

            except Exception as e:
                logging.error(e)
                logging.info("duplicate record in final stage spider. ")

        return item
