# -*- coding: utf-8 -*-
from __future__ import print_function
import scrapy
from peewee import *
import pandas as pd


class KvkExportSpider(scrapy.Spider):
    name = 'kvk_export'
    allowed_domains = ['kvk.nl']

    def start_requests(self):
        db = SqliteDatabase("kvk_final.db")
        db.connect()

        class BaseModel(Model):
            class Meta:
                database = db

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

        log_cols = ["search_string", "8_digit_kvk", "12_digit_kvk", "vesting_nr", "status_of_company", "headquarters",
                    "website", "main_company_name", "bestaande_handelsnamen", "statutaire_naam",
                    "second_page_company_name", "street", "housenumber", "housenumber_extension", "postcode", "city",
                    "extra_infomation"]

        df = pd.DataFrame(columns=log_cols, dtype=str)
        for company in kvkItemModel.select():
            df_row = pd.DataFrame([[company.search_string, "%s\t" % company.eight_digit_kvk,
                                    "%s\t" % company.twelve_digit_kvk, "%s\t" % company.vesting_nr,
                                    company.status_of_company, company.headquarters, company.website,
                                    company.main_company_name, company.bestaande_handelsnamen, company.statutaire_naam,
                                    company.second_page_company_name, company.street, company.housenumber,
                                    company.housenumber_extension, company.postcode, company.city,
                                    company.extra_infomation]], columns=log_cols, dtype=str)
            df = df.append(df_row, ignore_index=True)

        df.to_csv("../output.csv")
        print("="*30)
        print("all collected data had exported to file.csv, please check your folder, goodbye. ")
        print("+"*30)

        yield scrapy.Request(url="", callback=None)
