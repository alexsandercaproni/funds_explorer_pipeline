import sys
sys.path.insert(0, "/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer")
from scripts.utils.common_vars import funds

from bs4 import BeautifulSoup
import json


def ingest_into_trusted_zone(fund_dict, trusted_path, fund_name):
    with open(f"{trusted_path}{fund_name}_information.json", "w") as outfile:
        json.dump(fund_dict, outfile, indent=4)


def transform_html_files(soup, trusted_path, fund_name):
    # Create a directory
    fund_dict = {}

    # Code
    code = [code.text.strip() for code in soup.find_all('h1', {'class' : 'section-title'})]
    fund_dict['code'] = code

    # Name
    name = [name.text.strip() for name in soup.find_all('h3', {'class' : 'section-subtitle'})]
    fund_dict['name'] =  name[0]

    # Price
    price = [price.text.replace('R$', '') for price in soup.find_all('span', {'class' : 'price'})]
    fund_dict['last_price'] =  price[0].replace('.', '').replace(',', '.').strip()

    # Percentages
    perc_neg = [perc.text.strip() for perc in soup.find_all('span', {'class' : 'percentage negative'})]
    perc_pos = [perc.text.strip() for perc in soup.find_all('span', {'class' : 'percentage positive'})]
    
    if len(perc_neg) != 0:
        fund_dict['variation'] =  perc_neg[0].replace('.', '').replace(',', '.').replace('%', '').strip()
    else:
        fund_dict['variation'] =  perc_pos[0].replace('.', '').replace(',', '.').replace('%', '').strip()

    # Indicadores
    indicators = [indic.text.strip() for indic in soup.find_all('span', {'class' : 'indicator-value'})]
    
    ## Liquidez Diaria
    daily_liquidity = indicators[0].replace('.', '').replace(',', '.').strip()
    fund_dict['daily_liquidity'] = daily_liquidity
    
    ## Ultimo Rendimento
    last_income = indicators[1].replace('.', '').replace(',', '.').replace('R$', '').strip()
    fund_dict['last_income'] = last_income
    
    ## Patrimonio Liquido
    liquidity_equity = indicators[3].replace('.', '').replace(',', '.')\
                        .replace('R$', '').strip()
    fund_dict['liquidity_equity'] = liquidity_equity
    
    ## Valor Patrimonial
    equity_value = indicators[4].replace('.', '').replace(',', '.').replace('R$', '').strip()
    fund_dict['equity_value'] = equity_value
    
    ## Rentabilidade Mes
    month_gain = indicators[5].replace('.', '').replace(',', '.').replace('%', '').strip()
    fund_dict['month_gain'] = month_gain
    
    # Preco sobre Valor Patrimonial
    p_vp = indicators[6].replace('.', '').replace(',', '.').strip()
    fund_dict['p_vp'] = p_vp

    # Tabela de Dividendos
    html_table = soup.find("table", {'class':'table'})
    tbody = html_table.find('tbody')
    trs = tbody.find_all('tr')
    
    values = []
    percs = []
    for tr in trs:
        tds = tr.find_all('td')
        for td in tds:
            if 'R$' in td.text:
                values.append(td.text)
            if '%' in td.text:
                percs.append(td.text)

    # Valores 
    last_yield = values[0].replace('.', '').replace(',', '.').replace('R$', '').strip()
    last_tree_months = values[1].replace('.', '').replace(',', '.').replace('R$', '').strip()
    last_six_months = values[2].replace('.', '').replace(',', '.').replace('R$', '').strip()
    last_twelve_months = values[3].replace('.', '').replace(',', '.').replace('R$', '').strip()
    since_ipo = values[4].replace('.', '').replace(',', '.').replace('R$', '').strip()

    fund_dict['value_last_yield'] = last_yield
    fund_dict['value_last_tree_months'] = last_tree_months
    fund_dict['value_last_six_months'] = last_six_months
    fund_dict['value_last_twelve_months'] = last_twelve_months
    fund_dict['value_since_ipo'] = since_ipo

    # Percentuais
    last_yield_perc = percs[0].replace('.', '').replace(',', '.').replace('%', '').strip()
    last_tree_months_perc = percs[1].replace('.', '').replace(',', '.').replace('%', '').strip()
    last_six_months_perc = percs[2].replace('.', '').replace(',', '.').replace('%', '').strip()
    last_twelve_months_perc = percs[3].replace('.', '').replace(',', '.').replace('%', '').strip()
    since_ipo_perc = percs[4].replace('.', '').replace(',', '.').replace('%', '').strip()

    fund_dict['perc_last_yield'] = last_yield_perc
    fund_dict['perc_last_tree_months'] = last_tree_months_perc
    fund_dict['perc_last_six_months'] = last_six_months_perc
    fund_dict['perc_last_twelve_months'] = last_twelve_months_perc
    fund_dict['perc_since_ipo'] = since_ipo_perc

    ingest_into_trusted_zone(fund_dict, trusted_path, fund_name)


def extract_funds_information():
    # Paths
    root = "/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer/datalake/"
    raw_path = f"{root}raw/"
    trusted_path = f"{root}trusted/"

    funds_list = funds.list.value
    for f in funds_list:
        with open(f"{raw_path}{f.lower()}_webpage.html") as fp:
            soup = BeautifulSoup(fp, 'html.parser')
            transform_html_files(soup, trusted_path, f.lower())


if __name__ == '__main__':
    extract_funds_information()
    