import requests
import xmltodict

from crawler.configuration import Configuration


class CurrencyCrawler:

    def __init__(self):
        self._config = Configuration()

    def crawl(self):
        data = self._get_data()
        price = self._extract_price(data)
        return price

    def _get_data(self):
        response = requests.get(self._config.cbr_url)
        data = xmltodict.parse(response.content)
        return data

    def _extract_price(self, data):
        usd_rub_price = 0
        for item in data['ValCurs']['Valute']:
            if item['CharCode'] == self._config.currency:
                usd_rub_price = float(item['Value'].replace(',', '.'))

        return usd_rub_price
