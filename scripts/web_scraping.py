import sys
sys.path.insert(0, "/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer")
from scripts.utils.common_vars import funds

from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def search_url(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()

    return BeautifulSoup(webpage, 'html.parser')


def scraping_funds():
    funds_list = funds.list.value
    lake_path = "/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer/datalake/raw/"

    for f in funds_list:
        url = f"https://www.fundsexplorer.com.br/funds/{f}"
        bs = search_url(url)

        with open(f"{lake_path}{f.lower()}_webpage.html", "w") as file:
            file.write(str(bs))


if __name__ == '__main__':
    scraping_funds()
