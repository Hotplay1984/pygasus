import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
from send_email import *
import traceback
engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}

columns = ['value_date', 'bond_type', 'bond_name', 'bond_code', 'full_price', 'clean_price',
      'yeild', 'coupon_type']
url_0 = 'http://www.chinamoney.com.cn/fe-c/finalValuation.do?showDate=@@showDate&bondType=@@bond_type'

def download_valuation(datadate):
    date_dt = dt.datetime.strptime(datadate, '%Y%m%d')
    showDate = date_dt.strftime('%Y-%m-%d')
    bond_types = ['100001', '100002', '999901', '100003', '100027', '100006', '100010',
             '100004', '100029', '100041', '100011']

    df_dict = {}
    for bond_type in bond_types[:]:
        url = url_0.replace('@@showDate', showDate)
        url = url.replace('@@bond_type', bond_type)
        print(bond_type)
        with requests.Session() as session:
                retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[ 500, 502, 503, 504 ])
                session.mount('http://',HTTPAdapter(max_retries=retries))
                res = session.get(url, headers=headers,timeout = 40)
        bs = BeautifulSoup(res.text, 'lxml')

        tds = bs.findAll('tr')[0].findAll('tr')[1:]
        value_lists = [[] for row in range(len(tds))]
        for row_n in range(len(tds)):
            for val_tag in tds[row_n].findAll('td')[:-1]:
                val = val_tag.text.replace('\r', '')
                val = val.replace('\n', '')
                val = val.replace('\t', '')
                val = val.replace('\xa0', '')
                value_lists[row_n].append(val)
        df = pd.DataFrame(value_lists, columns = columns)
        df_dict[bond_type] = df
        
    df = pd.DataFrame([[None for x in range(len(columns))]], columns = columns)
    for df_temp in df_dict.values():
        df = pd.concat([df, df_temp])
    df = df.dropna().reset_index()[columns]
    df['datadate'] = [dt.datetime.now().strftime('%Y%m%d') for x in range(len(df))]
    df['time_stp'] = [dt.datetime.now() for x in range(len(df))]
    return df

def send_data(df):
    local_path = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\'
    file_name = local_path + 'daily_update_cfets_bond_valuation.xls'
    df.to_excel(file_name)
    df.to_sql(
        'cfets_bond_valuation',
        engine,
        schema='cfets',
        if_exists = 'append',
        index = False
    )
    send_mail_via_com('hello Will', 
        'daily_update', 
        'sunweiyao@sinopac.com', 
        select_file = 'daily_update_cfets_bond_valuation.xls')

def main():
    print('Start downloading Cfets bond valuation......')
    today_dt = dt.datetime.now()
    today_str = today_dt.strftime('%Y%m%d')
    try:
        df = download_valuation(today_str)
        send_data(df)
        print('finished!')
        os.system('pause')
    except:
        traceback.print_exc()
        os.system('pause')

if __name__ == '__main__':
    
	main()
	