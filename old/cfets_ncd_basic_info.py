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
from upload_ncd import *

path = r'C:\Users\client\Desktop\python_work\work\market_data\bond_basic_info\ncd_data'
bond_codes = []
error_list = []
for _, __, file_names in os.walk(path):
	bond_codes = [x.split('.')[0] for x in file_names]

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

url_0 = '''http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/bondInfoList?bondType=@@bond_type&issueYear=@@year&pagingPage_il_=1'''
url_page = '''http://www.chinamoney.com.cn/dqs/rest/cm-u-pt/bondInfoList?bondType=@@bond_type&issueYear=@@year&pagingPage_il_=@@n'''
bond_type = '100041'

def get_res(url, max_retry=3):
	try_counter = 0
	res_text = None
	while try_counter <= max_retry:
		if res_text is None:
			with requests.Session() as session:
				retries = Retry(total=10,
						backoff_factor=0.1,
						status_forcelist=[ 404, 500, 502, 503, 504 ])
				session.mount('http://',HTTPAdapter(max_retries=retries))
				# res = session.get(url, headers=headers,timeout = 40)
				res = session.get(url, headers=headers,timeout = (40, 40), proxies=proxies)
			res_text = res.text 
			if res_text is not None:
				return res_text
			else:
				print(res.status_code)
				try_counter += 1
				sleep(1)

def get_total_page(bond_type):
	print('Downloading NCD Basic Infomation...')
	current_year = str(dt.datetime.now().year)
	url = url_0.replace('@@bond_type', str(bond_type))
	url = url.replace('@@year', current_year)
	# url = url.replace('@@year', '2017')
	res = get_res(url)
	full_dict = json.loads(get_res(url))
	total_page = full_dict['data']['totalPages']
	print('total page: %s' % str(total_page))
	return int(total_page)

def get_define_codes(page_n, bond_type):
	current_year = str(dt.datetime.now().year)
	url = url_page.replace('@@n', str(page_n))
	url = url.replace('@@bond_type', str(bond_type))
	url = url.replace('@@year', current_year)
	print(url)
	# url = url.replace('@@year', '2017')
	res = get_res(url)
	define_codes = []
	try:
		full_dict = json.loads(res)
		for record in full_dict['records']:
			bond_code, issue_date = record['bondCode'], record['issueDate']
			if bond_code not in bond_codes:
				define_codes.append(record['definedCode'])
	except:
		print('Error URL: %s ' % url)
		error_list.append(url)
		traceback.print_exc()
		print('\n')
		print(res)
	return define_codes


def get_table_text(define_code):
	if define_code == '':
		return []
	url_tmp = '''http://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo?bondDefinedCode=@@code'''
	url_tmp = url_tmp.replace('@@code', define_code)
	res_tmp = get_res(url_tmp)
	js = json.loads(res_tmp)
	print(url_tmp)
	return js['data']['bondBaseInfo']

	
def write_to_local(info_dict):
	file_dir = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_basic_info\\ncd_data\\' + info_dict['bondCode'] + '.json'
	with open(file_dir, 'w', encoding='utf8') as f:
		json.dump(info_dict, f, ensure_ascii=False)
	print(file_dir)


def download_single(define_code):
	try:
		js = get_table_text(define_code)
		write_to_local(js)
	except:
		traceback.print_exc()

max_n = get_total_page(str(bond_type))
for page_n in range(1, max_n + 1):
	print('Downloading page %s/%s of %s...' % (str(page_n), str(max_n), str(bond_type)))
	try:
		define_codes = get_define_codes(page_n, bond_type)
		if len(define_codes) > 0:
			async def single(define_code):
				loop = asyncio.get_event_loop()
				await loop.run_in_executor(None, download_single, define_code)
			async def multiple(define_code, i):
				print('协程%s启动' % str(i))
				await single(define_code)
			tasks = []
			for i in range(len(define_codes)):
				if define_codes[i] != '-':
					tasks.append(multiple(define_codes[i], i))
			if len(tasks) > 0:
				loop = asyncio.get_event_loop()
				loop.run_until_complete(asyncio.wait(tasks))
		else:
			print('No new bond')
			break
	except:
		traceback.print_exc()
		continue

if len(error_list) > 0:
	df_error = pd.DataFrame()
	df_error['url'] = error_list
	df_error['time_stp'] = [dt.datetime.now() for x in range(len(error_list))]
	df_error.to_excel('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\NCD_Error.xlsx') 

update_sql_and_send_email()
print('All Done!!!')
os.system('pause')
