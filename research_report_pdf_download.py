import requests, json, asyncio
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2 as sql
from sqlalchemy import create_engine
import datetime as dt
import os, traceback

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
engine = create_engine(engine_str)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}

proxies = {
	'http':'http://119.28.222.122:8888',
	'https':'https://119.28.222.122:8888'
}

def get_file_size(path):
	fsize = os.path.getsize(path)
	return round(fsize/float(1024), 2)

def split_task(task_list, n):
	return [task_list[i: i+n] for i in range(len(task_list)) if i%n == 0]

def get_res(url, string=True, stream=True):
	res_text = None
	with requests.Session() as session:
		retries = Retry(total=10,
				backoff_factor=0.1,
				status_forcelist=[500, 502, 503, 504 ])
		session.mount('http://',HTTPAdapter(max_retries=retries))
		# res = session.get(url, headers=headers,timeout = 30)
		res = session.get(url, headers=headers,timeout = 40, proxies=proxies)
		res.encoding = 'utf-8'
	res_text = res.text 
	if string:
		return res_text
	else:
		return res

def clean_title(title):
	shit_list = [':', '：', '"', "“", '”', "?",
			 "/", "<", "|", "\\", ">", "*", "×",
				'【', '】']
	for shit in shit_list:
		if shit in title:
			title = title.replace(shit, '_')
	return title

def get_task(n):
	sql_str = '''
		select '宏观' as industry, institution || '_' || title|| '_' || report_date || '_' || pdf_id as title, pdf_link
		from analysis_report.macro
		where pdf_link is not null
		and pdf_link <> ''
		union all
		select industry, institution || '_' || title|| '_' || report_date || '_' || pdf_id as title, pdf_link
		from analysis_report.industry
		where pdf_link is not null
		and pdf_link <> ''
	'''
	df_link = pd.read_sql_query(sql_str, engine)
	
	path_head = 'D:\\report_pdf\\'
	pending_dict = {}

	for ix in df_link.index:
		folder = df_link.at[ix, 'industry']
		title = clean_title(df_link.at[ix, 'title'])
		url = df_link.at[ix, 'pdf_link']

		for _, sub_folders, _1 in os.walk(path_head):
			sub_folder_path = path_head + folder
			if not os.path.exists(sub_folder_path):
				os.makedirs(sub_folder_path)

		file_path = '%s%s\\%s.pdf' % (path_head, folder, title)
		if not os.path.exists(file_path):
			pending_dict[title] = [url, file_path]
		elif os.path.getsize(file_path) == 0.:
			pending_dict[title] = [url, file_path]
	task_list = list(pending_dict.values())
	print('Total File: %s' % str(len(task_list)))
	task_sub_list = split_task(task_list, n)
	if len(task_sub_list) > 0:
		return task_sub_list
	else:
		return [], 0

def download_file(addrs):
	start = dt.datetime.now().strftime('%H:%M:%S')
	url, file_path = addrs[0], addrs[1]
	try:
		r = get_res(url, string=False)
	except:
		traceback.print_exc()
		return 
	with open(file_path, 'wb') as f:
		f.write(r.content)
	end = dt.datetime.now().strftime('%H:%M:%S')
	print('Downloading %s finished. \nStart @ %s, End @ %s.' % (file_path, start, end))
	print('File Size: %s KB' % (get_file_size(file_path)))

def multi_run(task_sub_list):
	counter = 0
	for addr_group in task_sub_list:
		sub_total = len(task_sub_list)
		async def single(addrs):
			loop = asyncio.get_event_loop()
			await loop.run_in_executor(None, download_file, addrs)
		async def multiple(n , addrs):
			print('协程No.%s启动' % str(n + 1))
			await single(addrs)
		tasks = []
		for i in range(len(addr_group)):
			tasks.append(multiple(i, addr_group[i]))
		loop = asyncio.get_event_loop()
		loop.run_until_complete(asyncio.wait(tasks))
		counter += 1
		print('\n\nProgress: %s/%s\n\n' % (str(counter), str(len(task_sub_list))))


def download_pdf():
	print('Start Downloaing PDF reports...')
	task_sub_list = get_task(20)
	if len(task_sub_list) > 0:
		multi_run(task_sub_list)
	else:
		print('No new file.')

if __name__ == '__main__':
	download_pdf()