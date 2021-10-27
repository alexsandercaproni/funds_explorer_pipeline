from urllib.request import Request, urlopen
from bs4 import BeautifulSoup


def search_url(url):
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    webpage = urlopen(req).read()

    return BeautifulSoup(webpage, 'html.parser')


def get_funds():
    return ['VILG11', 'XPLG11', 'VINO11', 'BBPO11',
            'XPML11', 'VISC11', 'BCFF11', 'MXRF11',
            'RECR11', 'HTMX11', 'TGAR11', 'HCTR11']


def main():
    funds = get_funds()

    for f in funds:
        url = f"https://www.fundsexplorer.com.br/funds/{f}"
        bs = search_url(url)

        with open(f"datalake/raw/{f.lower()}_webpage.html", "w") as file:
            file.write(str(bs))


if __name__ == '__main__':
    main()
    

#print(soup.find('h1', id="hello-world").get_text())
#print(soup.find('p'))