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

import upload
import upload_ncd

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
proxies = {
	'http':'http://119.28.222.122:8888',
	'https':'https://119.28.222.122:8888'
}

def split_task(task_list, n):
	return [task_list[i: i+n] for i in range(len(task_list)) if i%n == 0]

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

def get_unlisted():
	print('Updating Unlist Bond Information...')
	print('Reading local files')
	define_codes = []
	path_ncd = r'C:\Users\client\Desktop\python_work\work\market_data\bond_basic_info\ncd_data'
	path_bond = r'C:\Users\client\Desktop\python_work\work\market_data\bond_basic_info\data'
	for path in [path_ncd, path_bond]:
		for dir_, sub_folders, file_names in os.walk(path):
			for file in file_names:
				if '-' not in file:
					file_path = '%s\%s' % (path, file)
					with open(file_path, 'r', encoding='utf8') as f:
						info_dict = json.loads(f.read())
						if info_dict['lstngDate'] == '---':
							define_codes.append(info_dict['bondDefinedCode'])
	return define_codes

def get_table_text(define_code):
	if define_code == '':
		return []
	url_tmp = '''http://www.chinamoney.com.cn/ags/ms/cm-u-bond-md/BondDetailInfo?bondDefinedCode=@@code'''
	url_tmp = url_tmp.replace('@@code', define_code)
	res_tmp = get_res(url_tmp)
	js = json.loads(res_tmp)
	print(url_tmp)
	if js['data']['bondBaseInfo']['lstngDate'] != '---':
		return js['data']['bondBaseInfo']

def write_to_local(info_dict):
	if info_dict['bondType'] == '同业存单':
		file_dir = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_basic_info\\ncd_data\\' + info_dict['bondCode'] + '.json'
	else:
		file_dir = 'C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\bond_basic_info\\data\\' + info_dict['bondCode'] + '.json'

	with open(file_dir, 'w', encoding='utf8') as f:
		json.dump(info_dict, f, ensure_ascii=False)
	print(file_dir)
	
def download_single(define_code):
	try:
		js_dict = get_table_text(define_code)
		if js_dict:
			write_to_local(js_dict)
	except:
		traceback.print_exc()
		
def update_unlisted(n):
	define_codes = get_unlisted()
	task_lists = split_task(define_codes, n)

	for task_num in range(len(task_lists)):
		print('Start task group No.%s/Total %s' % (str(task_num + 1), str(len(task_lists))))
		async def single(define_code):
			loop = asyncio.get_event_loop()
			await loop.run_in_executor(None, download_single, define_code)
		async def multiple(define_code, i):
			print('协程%s启动' % str(i))
			await single(define_code)
		tasks = []
		for i in range(len(task_lists[task_num])):
			tasks.append(multiple(task_lists[task_num][i], i))
		if len(tasks) > 0:
			loop = asyncio.get_event_loop()
			loop.run_until_complete(asyncio.wait(tasks))

if __name__ == '__main__':
	try:
		update_unlisted(20)
	except:
		traceback.print_exc()
		os.system('pause')
	os.system('pause')