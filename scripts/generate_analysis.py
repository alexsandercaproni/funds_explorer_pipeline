import sys
sys.path.insert(0, "/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer")

from scripts.utils.common_vars import funds

import pandas as pd
import os
import json


def ingest_into_refined_zone(refined_path, df):
    try:
        df.to_excel(f"{refined_path}funds_to_analyze.xlsx", index=False)
    except:
        print('It is impossible to export funds report.')


def create_funds_dataframe():
    path = os.path.join(
        '/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer/',
        'datalake/trusted/'
    )

    df_funds = pd.DataFrame()
    idx = 0
    has_idx = True

    for f in funds.list.value:
        with open(f"{path}{f.lower()}_information.json", 'r') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data, index=[idx])        
        df_funds = df_funds.append(df, ignore_index=has_idx)
        
        if idx == 0:
            has_idx = False

        idx += 1
    
    ingest_into_refined_zone(path.replace('trusted', 'refined'), df_funds)


if __name__ == '__main__':
    create_funds_dataframe()
