import time
import datetime

from crawler.configuration import Configuration
from crawler.sheet_crawler import SheetCrawler

config = Configuration()
crawler = SheetCrawler()

while True:
    count = crawler.crawl()
    print(f'{datetime.datetime.now()}\t{count:,} rows is crawled.')
    time.sleep(config.crawl_period_sec)
