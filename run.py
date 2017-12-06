from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import os
import logging
from parsel import Selector
from pprint import *
import time
from peewee import *
from playhouse.sqlite_ext import SqliteExtDatabase
import datetime
import re
import copy
import pandas as pd
from IPython.display import display, HTML
import psycopg2
import json
from w3lib.url import url_query_parameter
import w3lib
"""xpath start here"""
xpath_pagination = """//div[@class="search-result"]/ul[@class="pagination"]"""
xpath_more_information_hover = """//ul[contains(@class, "result")]/li"""
xpath_next_page = """//div[@class="search-result"]/ul[@class="pagination"]/li[contains(@class, "volgende")][not(contains(@class, "not"))]"""
xpath_next_page_url = """//div[@class="search-result"]/ul[@class="pagination"]/li[contains(@class, "volgende")][not(contains(@class, "not"))]/a/@href"""
xpath_search_string_insert = """//*[(@id = "search-company-input")]"""
xpath_search_button = """/html/body/div[2]/main/div/div/div/div[2]/button"""
xpath_eight_digit_kvk = """//*[contains(concat( " ", @class, " " ), concat( " ", "meta", " " ))]//p[(((count(preceding-sibling::*) + 1) = 1) and parent::*)]/text()"""
xpath_twenty_digit_kvk = """//*[contains(concat( " ", @class, " " ), concat( " ", "result", " " ))]/@href"""
xpath_vesting_nr = """//*[contains(concat( " ", @class, " " ), concat( " ", "meta", " " ))]//p[(((count(preceding-sibling::*) + 1) = 2) and parent::*)]/text()"""
xpath_status_of_company = """//li/a/p[@class="status"]/text()"""
xpath_headquarters = """//*[contains(concat( " ", @class, " " ), concat( " ", "type", " " ))]/text()"""
xpath_main_company_name = """//h3/text()"""
xpath_bestaande_handelsnamen = """//div[@class="more-info"]/h4[contains(text(), "Bestaande")]/following-sibling::p/text()"""
xpath_statutaire_naam = """//div[@class="more-info"]/h4[contains(text(), "Statutaire")]/following-sibling::p/text()"""
xpath_second_page_link = """//*[contains(concat( " ", @class, " " ), concat( " ", "result", " " ))]/@href"""
"""xpath stop  here"""

global db
db = SqliteDatabase("kvk_temp.db")
db.connect()
class BaseModel(Model):
    class Meta:
        global db
        database = db

class kvkItemModel(BaseModel):
    search_string = CharField()
    eight_digit_kvk = CharField(null=True, unique=True)
    twenty_digit_kvk = CharField(null=True)
    vesting_nr = CharField(null=True)
    status_of_company = CharField(null=True)
    headquarters = BooleanField(null=True)
    website = CharField(null=True)
    main_company_name = CharField(null=True)
    bestaande_handelsnamen = CharField(null=True)
    statutaire_naam = CharField(null=True)
    second_page_company_name = CharField(null=True)
    street = CharField(null=True)
    housenumber = CharField(null=True)
    housenumber_extension = CharField(null=True)
    postcode = CharField(null=True)
    city = CharField(null=True)
    extra_infomation = CharField(null=True)
    second_page_link = CharField(null=True)
try:
    db.drop_tables([kvkItemModel])
except:
    pass
db.create_tables([kvkItemModel])
print("=" * 30)
print("Sqllite Intialization Succeed !")
print("=" * 30)
db.close()


class DenmarkECommerceSpider:
    def __del__(self):
        print("="*30)
        print("First Pages of Postcode: `%s` Finished Scrapping" % self.input_post_code)
        print("Found %s companies on postcode: %s" % (self.input_post_code, self.total_companies_of_post_code))
        print("="*30)

    def __init__(self, input_post_code=None, driver=None):
        self.input_post_code = input_post_code
        self.driver = driver
        self.total_companies_of_post_code = 0

    def run(self):
        self.open_home_page()
        try:
            self.insert_search_string_click_search()
        except:
            print("="*30)
            print("The page is not responding, refresh and sleep for 2 seconds")
            print("="*30)
            self.open_home_page()
            time.sleep(2)
            try:
                self.insert_search_string_click_search()
            except:
                raise Exception("Please check your selenium grid container, you may need a better internet connection")

        print("=" * 30)
        print("Scraping process started !")
        print("=" * 30)
        self.extract_all_level1_company_info()

    def extract_all_level1_company_info(self):
        self.open_more_information()
        self.extract_level1_company_info_on_page()

        while self.has_next_page() is True:
            try:
                hover = ActionChains(driver).move_to_element(
                    driver.find_element_by_xpath(xpath_search_string_insert)
                ).click(driver.find_element_by_xpath(xpath_search_string_insert))
                hover.perform()
                ActionChains(driver).move_to_element_with_offset(
                    driver.find_element_by_xpath(xpath_next_page), 100, 1000).perform()
                time.sleep(0.1)
                ActionChains(driver).click(driver.find_element_by_xpath(xpath_next_page)).move_to_element_with_offset(
                    driver.find_element_by_xpath(xpath_next_page), 100, 1000).perform()
                self.open_more_information()
                self.extract_level1_company_info_on_page()

            except Exception as e:
                print(e)
                self.open_more_information()
                self.extract_level1_company_info_on_page()

    def open_home_page(self):
        driver = self.driver
        driver.get(url="about:blank")
        driver.get(url="https://www.kvk.nl/orderstraat/bedrijf-kiezen/?orig=#!shop?&q=")
        print("=" * 30)
        print("Home page opened in remote browser !")
        print("=" * 30)

    def insert_search_string_click_search(self):
        driver = self.driver
        input_post_code = self.input_post_code
        driver.find_element_by_xpath(xpath_search_string_insert).send_keys(input_post_code)
        driver.find_element_by_xpath(xpath_search_button).click()
        print("="*30)
        print("search string inserted !")
        print("="*30)

    def open_more_information(self):
        driver = self.driver
        try:
            WebDriverWait(self.driver, 2).until(
                EC.presence_of_element_located((By.XPATH, xpath_pagination))
            )
            WebDriverWait(self.driver, 2).until(
                EC.presence_of_element_located((By.XPATH, xpath_more_information_hover))
            )
        except:
            driver.back()
            print("Turning Back")
            time.sleep(1)


    def extract_level1_company_info_on_page(self):
        driver = self.driver
        input_post_code = self.input_post_code
        companies = Selector(text=driver.page_source).xpath(xpath_more_information_hover).extract()
        print("=" * 30)
        print("New page Opened - Level1, found `%s` companies on this page" % len(companies))
        print("=" * 30)
        for company in companies:
            company_obj = Selector(text=company)
            item = kvkItemModel()
            item.search_string = copy.deepcopy(input_post_code)
            item.eight_digit_kvk = str(company_obj.xpath(xpath_eight_digit_kvk).extract_first()).strip().split()[1]
            item.twenty_digit_kvk = \
                str(company_obj.xpath(xpath_twenty_digit_kvk).extract_first()).strip().split('kvknummer=')[-1].split('&')[0]
            if len(item.twenty_digit_kvk) <= 11:
                item.twenty_digit_kvk = str(item.twenty_digit_kvk) + "0000"
            item.vesting_nr = str(company_obj.xpath(xpath_vesting_nr).extract_first()).strip().split(" ")[-1]
            if str(item.vesting_nr).startswith("Ves") is True:
                item.vesting_nr = "None"
            item.status_of_company = str(company_obj.xpath(xpath_status_of_company).extract_first()).strip()
            if company_obj.xpath(xpath_headquarters).extract_first() is None:
                item.headquarters = False
            else:
                item.headquarters = True
            item.main_company_name = str(company_obj.xpath(xpath_main_company_name).extract_first())
            item.bestaande_handelsnamen = str(company_obj.xpath(xpath_bestaande_handelsnamen).extract_first())
            item.statutaire_naam = str(company_obj.xpath(xpath_statutaire_naam).extract_first())
            item.second_page_link = "https://www.kvk.nl/" + str(
                company_obj.xpath(xpath_second_page_link).extract_first())
            try:
                item.save()
                self.total_companies_of_post_code += 1
            except Exception as e:
                # This means we have inserted a duplicate record.
                pass

    def has_next_page(self):
        driver = self.driver
        html = driver.page_source
        if Selector(text=html).xpath(xpath_next_page).extract_first() is None:
            return False
        else:
            return True


if __name__ == '__main__':
    def init_sqlite_database():
        pass

    def start_web_driver():
        # # local browser
        # from selenium.webdriver.chrome.options import Options
        # chrome_options = Options()
        # chrome_options.add_argument("--headless")
        #
        # web_driver = webdriver.Chrome()
        # web_driver.get("about:blank")
        # return web_driver

        # remote browser
        web_driver = None
        try:
            web_driver = webdriver.Remote(command_executor="http://172.104.57.173:3000/wd/hub",
                                             desired_capabilities=DesiredCapabilities.CHROME)
            web_driver.get("about:blank")
            return web_driver
        except:
            logging.error("Remote Chrome Webdriver failed")

        raise Exception("There is no webdriver available for me")

    def read_post_codes_csv():
        df = pd.read_csv("postcode_list.csv")
        return df["post_code"]

    init_sqlite_database()
    driver = start_web_driver()
    post_codes_list = read_post_codes_csv()

    for index, post_code in enumerate(post_codes_list):
        spider = DenmarkECommerceSpider(input_post_code=post_code, driver=driver)
        spider.run()
        print("%s out of %s post code have scrapped" % (str(index+1), len(post_codes_list)))

        # TODO: Remove before take off
        if index == 5:
            break

    print("All the First Page had collected, now turn to all the second pages")
    driver.close()
