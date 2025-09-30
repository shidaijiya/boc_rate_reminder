import requests
import os
from bs4 import BeautifulSoup
from lib.log_helper import log_print
from lib.utils import str_to_bool



boc_rate_url = 'https://www.bankofchina.com/sourcedb/whpj/index.html'


def fetch_exchange_rates():

    headers = {
        'User-Agent': 'ExchangeRateBot'
    }

    DEFAULT_PROXY = str_to_bool(os.getenv('DEFAULT_PROXY'))

    if DEFAULT_PROXY:
        proxies = {
            "http": f"http://{os.getenv('PROXY_HOST')}:{int(os.getenv('PROXY_PORT'))}",
            "https": f"http://{os.getenv('PROXY_HOST')}:{int(os.getenv('PROXY_PORT'))}"
        }
    else:
        proxies = None

    resp = requests.get(boc_rate_url,headers=headers , proxies=proxies)
    if resp.status_code != 200:
        log_print.error(f"network error")
        return

    soup = BeautifulSoup(resp.content.decode("utf-8"), "html.parser")
    table = soup.find("table", {
        "cellpadding": "0",
        "align": "left",
        "cellspacing": "0",
        "width": "100%"
    })

    if not table:
        log_print.error("Exchange rate table not found")
        return

    rates = []
    for tr in table.find_all("tr")[1:]:
        rate = []
        for td in tr.find_all('td')[:-1]:
            rate.append(td.text)
        rates.append(rate)
    log_print.info(f"fetched {len(rates)} exchange rate data")
    return rates







