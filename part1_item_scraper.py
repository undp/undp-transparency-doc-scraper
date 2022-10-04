#need to sudo apt-get install tesseract-ocr
#need to also install poppler-utils
from PIL import Image 
#import pytesseract 
import sys 
from pdf2image import convert_from_path 
import os 
import pandas as pd, math, inspect, requests
from lxml import html
import re, json, io
import textract
import urllib.parse
import scrapy
from scrapy.crawler import CrawlerProcess
from PyPDF2 import PdfFileReader
import os
import urllib
import ssl
import pdfpages
import pikepdf
#from docx2pdf import convert
from datetime import date
today = date.today().strftime("%Y-%m-%d")
import pdfplumber

#Document Folder Scraper
#Get the folders containing the documents
#Hast to be executed separately and independently of document list scraper
class BootstrapTableSpider(scrapy.Spider):
    name = "un_test"
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'temp-documentfolders.csv'
    }
    def start_requests(self):
        urls = ['https://info.undp.org/docs/pdc/Documents/Forms/AllItems.aspx']
        for url in urls:
            yield scrapy.Request(url, self.parse)

    
    def parse(self, response):
        rows = response.css('.ms-vb')
    
        for row in rows:
            if row.css('a')[0].xpath('@href').extract_first().find('Forms')>0:
                yield {
                  'country_code' :  row.css('a::text').extract()[0],
                  'url' : 'https://info.undp.org' + row.css('a')[0].xpath('@href').extract_first() + '&FilterField1=Atlas_x0020_Document_x0020_Type&FilterValue1=1110&FilterOp1=In&FilterLookupId1=1&FilterData1=0%2C099f975e-b4d9-4bba-a499-dbcc387c61ad&SortField=Created&SortDir=Desc' 
                }

try:
    os.remove("temp-documentfolders.csv")
except:
    pass
                
process = CrawlerProcess();
process.crawl(BootstrapTableSpider);
process.start();