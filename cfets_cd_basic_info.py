import requests, json, asyncio
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
import os, traceback
from time import sleep
from upload_cd import *

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
# proxies = {
# 	'http':'http://118.89.165.109:8888',
# 	'https':'http://118.89.165.109:8888'
# }
proxies = {
	'http':'http://119.28.222.122:8888',
	'https':'https://119.28.222.122:8888'
}

url_0 = '''http://www.chinamoney.com.cn/fe/chinamoney/searchCertificatesOfDepositInfoForward.action?pagingPage_il_=@@n&issueEnty=&issueYear=@@year&bondName=&bondCode=&'''

fields = ['cd_code_filed', 'issuer', 'rating_agency', 'issuer_rating', 'cd_code_1', 
		  'cd_name_full', 'cd_name_short', 'planned_issue_amt_100mio', 'actual_issue_amt_100mio',
		 'int_type', 'term', 'benchmark_rate', 'benchmark_rate_percision', 'rate', 
		 'spread', 'frequency', 'int_set_date', 'int_start_date', 'first_int_date','maturity_date',
		 'maturity_date_1', 'issue_start_date', 'issue_end_date', 'min_change',
		 'min_purchase', 'buyback_clause', 'withdraw_clause', 'note']
chn_fields = ['自律机制备案编码', '发行人', '主体评级机构', '主体评级等级', '存单代码',
			 '存单全称', '存单简称', '计划发行量(亿元)', '实际认购量(亿元)', 
			 '计息类型', '存单期限', '基准利率', '基准利率精度', '利率(%)', 
			 '利差', '付息频率', '首次利率确定日', '起息日', '首次付息日', '到期日', 
			 '兑付日', '发行开始时间', '发行结束时间', '最小变动单位(元)', 
			 '起存金额(万)', '提前赎回条件', '提前支取条件', '备注']
field_dict = {x: y for x, y in zip(chn_fields, fields)}


def get_res(url, max_retry=3):
	try_counter = 0
	res_text = None
	while try_counter <= max_retry:
		if res_text is None:
			with requests.Session() as session:
				retries = Retry(total=10,
						backoff_factor=0.1,
						status_forcelist=[ 500, 502, 503, 504 ])
				session.mount('http://',HTTPAdapter(max_retries=retries))
				# res = session.get(url, headers=headers,timeout = 40)
				res = session.get(url, headers=headers,timeout = 40, proxies=proxies)
			res_text = res.text 
			if res_text is not None:
				return res_text
			else:
				try_counter += 1

def get_total_page(year):
	url = url_0.replace('@@n', '1')
	url = url.replace('@@year', str(year))
	res = get_res(url)
	bs = BeautifulSoup(res, 'lxml')
	return int(bs.find('td', {'class':'market-note-1'}).text.split('/')[1].split('页')[0])

def current_year():
	return dt.datetime.now().year

def get_existing_cds():
	path = r'C:\Users\client\Desktop\python_work\work\market_data\bond_basic_info\cd_data'
	bond_codes = []
	for _, __, file_names in os.walk(path):
		bond_codes = [x.split('.')[0] for x in file_names]
	return bond_codes

def get_links_by_page(n, year):
	links = []
	url = url_0.replace('@@n', str(n))
	url = url.replace('@@year', str(year))
	res = get_res(url)
	bs = BeautifulSoup(res, 'lxml')
	for a in bs.find_all('a',href=True):
		if 'action?' in a['href']:
			links.append('http://www.chinamoney.com.cn%s' % a['href'])
	cd_codes = []
	texts = [x.get_text().replace('\xa0', '') for x in bs.select('td[class*="dreport-row"]')]
	for i in range(1, len(texts), 5):
		cd_codes.append(texts[i])
	existing_codes = get_existing_cds()
	new_links = []
	for code, link in zip(cd_codes, links):
		if code not in existing_codes:
			new_links.append(link)
	return new_links

def get_cd_info(url):
	cd_info_res = get_res(url)
	bs_cd_info = BeautifulSoup(cd_info_res, 'lxml')

	replaces = ['\xa0', ' ', '\n', '\r', '\t']
	values = []
	for raw in bs_cd_info.findAll('td', {'class':['bdr-rgt1', 'row2']}):
		text = raw.text
		for replace in replaces:
			text = text.replace(replace, '')
		values.append(text)

	value_dict = {}
	for i in range(0, len(values)-1, 2):
		value_dict[field_dict[values[i]]] = values[i + 1]
	file_dir = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_basic_info\\cd_data\\' + value_dict['cd_code_filed'] + '.json'
	with open(file_dir, 'w', encoding='utf8') as f:
		json.dump(value_dict, f, ensure_ascii=False)
	print(file_dir)


print('Updating CD Basic Infomation')
year = current_year()
max_n = get_total_page(year)
for page_n in range(1, max_n+1):
	print('Downloading page %s/%s....' % (str(page_n), str(max_n)))
	try:
		links = get_links_by_page(page_n, year)
		if len(links) > 0:
			async def single(link):
				loop = asyncio.get_event_loop()
				await loop.run_in_executor(None, get_cd_info, link)
			async def multiple(link, i):
				print('协程%s启动' % str(i))
				await single(link)
			tasks = []
			for i in range(len(links)):
				tasks.append(multiple(links[i], i))
			loop = asyncio.get_event_loop()
			loop.run_until_complete(asyncio.wait(tasks))
		else:
			print('No new cd')
			break
	except:
		traceback.print_exc()
		continue
update_sql_and_send_email_cd()
print('All Done!')
os.system('pause')