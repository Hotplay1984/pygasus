import jieba
import jieba.analyse
import pandas as pd
import datetime as dt

import jieba
import jieba.analyse
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import json, io, os
import psycopg2 as sql
from sqlalchemy import create_engine
import requests
import traceback

from send_email import *


engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'

category = {
	'hg':['http://datainterface.eastmoney.com//EM_DataCenter/js.aspx?type=SR&sty=HGYJ&cmd=4&code=&ps=50&p=',
		 ['report_time_stp', 'url_tail', 'code',
		  'institution', 'sub_code', 'title'],
		  '宏观研报',
		  'C:\\Users\\client\\Desktop\\python_work\\work\\research_report\\macro\\',
		  'macro',
		  ],
	'hy':['http://datainterface.eastmoney.com//EM_DataCenter/js.aspx?type=SR&sty=HYSR&mkt=0&stat=0&cmd=4&code=&sc=&ps=50&p=',
		['rating_change', 'report_time_stp', 'url_tail', 'code',
		  'institution', 'sub_code', 'sub_code_1', 'rating', 'rating_1',
		  'title', 'industry', 'daily_return'],
		  '行业研报',
		  'C:\\Users\\client\\Desktop\\python_work\\work\\research_report\\industry\\',
		  'industry',
		  ],
}

proxies = {
	'http':'http://119.28.222.122:8888',
	'https':'https://119.28.222.122:8888'
}

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}

def get_res(url, string=True):
	res_text = None
	with requests.Session() as session:
		retries = Retry(total=10,
				backoff_factor=0.1,
				status_forcelist=[500, 502, 503, 504 ])
		session.mount('http://',HTTPAdapter(max_retries=retries))
		res = session.get(url, headers=headers,timeout = 30)
		# res = session.get(url, headers=headers,timeout = 40, proxies=proxies)
	res_text = res.text 
	if string:
		return res_text
	else:
		return res

def get_pdf_url(soup):
	pdf_url = ''
	for span in soup.findAll('span'):
		if '查看PDF原文' in span.text:
			pdf_url = span.find('a')['href']
	return pdf_url

def get_report_content(url):
	html = get_res(url)
	soup = BeautifulSoup(html, 'lxml')
	res = soup.find('div', {'class': 'newsContent'}).findAll('p')
	res = [str(x).replace('<p>', '') for x in res]
	res = [x.replace('</p>', '') for x in res]
	res = [x for x in res if x != '']
	content = ''
	for r in res:
		content = content + r
		content = content + '\n'
	pdf_url = get_pdf_url(soup)
	pdf_id = pdf_url.split('/')[-1].split('.')[0]
	return content, pdf_url, pdf_id


def get_title_df(page_n, category_i):
	print('Retrieving report links on page%s of %s' % (str(page_n), category[category_i][2]))
	url_0 = category[category_i][0]
	url = url_0 + str(page_n)
	report_url_header = 'http://data.eastmoney.com/report/'
	html = get_res(url)
	bs = BeautifulSoup(html, 'lxml')
	res = bs.find('p')
	txt = res.text
	raw_list = txt.split('"')
	title_list = []
	for raw in raw_list:
		if '/' in raw:
			title_list.append(raw)
	columns = category[category_i][1]
	df = pd.DataFrame([[None for i in range(len(columns))]])
	for title in title_list:
		values = title.split(',')
		if len(values) == len(columns):
			df = df.append([values])
	df.columns = columns
	df = df[1:]

	time_stps = df['report_time_stp'].tolist()
	report_dates = []
	for stp in time_stps:
		stp = dt.datetime.strptime(stp, '%Y/%m/%d %H:%M:%S')
		stp = stp.strftime('%Y%m%d')
		report_dates.append(stp)
	df['report_date'] = report_dates

	columns = df.columns.tolist()
	df = df.reset_index()[columns]

	links = []
	for i in df.index:
		link = report_url_header + \
			   df.ix[i]['report_date'] + '/' + \
			   category_i + ',' + df.ix[i]['url_tail'] + \
			   '.html'
		links.append(link)
	df['link'] = links
	del df['url_tail']

	try:
		sql_engine = engine_str
		engine = create_engine(sql_engine)
		df.to_sql(category_i + '_links',
				  engine,
				  schema='analysis_report',
				  if_exists='replace'
		)
	except:
		df.to_json('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\' + category_i + '_links.json')
		print('%s链接文件写入数据库失败，写入本地json文件' % category_i)

	return df


def get_report_by_page(start_page, end_page, category_i):
	pages = [x for x in range(start_page, end_page + 1)]
	df = pd.DataFrame()
	for p in pages:
		df = df.append(get_title_df(str(p), category_i))
	columns = df.columns.tolist()
	df = df.reset_index()[columns]

	contents = []
	pdf_links = []
	pdf_ids = []
	tag_list = []
	fails = []
	total = len(df)
	process = 0
	for i in df.index:
		url = df.ix[i]['link']
		try:
			content, pdf_url, pdf_id = get_report_content(url)
		except:
			print('Fail: %s' % url)
			fails.append(url)
			content = 'not found'
			pdf_url = ''
			pdf_id = ''
		contents.append(content)
		pdf_links.append(pdf_url)
		pdf_ids.append(pdf_id)

		allowPOS = ('n', 'nr', 'eng', 'ns', 'j', 
					'nrt', 'nz','l', 's')
		jieba.analyse.set_stop_words('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\stop_words.txt')
		tags = jieba.analyse.extract_tags(content, topK=20, allowPOS = allowPOS)
		tag_str = ''
		for tag in tags:
			tag_str = tag_str + tag + '-'
		tag_str = tag_str.strip('-')

		tag_list.append(tag_str)
		process += 1
		print('{:.2%}'.format(process / total) + ':' + str(process) + '/' + str(total))

	print('length of contents: ' + str(len(contents)))
	print('length of df: ' + str(len(df)))

	df['content'] = contents
	df['tags'] = tag_list
	df['pdf_link'] = pdf_links
	df['pdf_id'] = pdf_ids
	return df

def report_to_txt(df, category_i):
	report_dict = {}
	for i in df.index:
		date = df.ix[i]['report_date']
		institution = df.ix[i]['institution']
		title = df.ix[i]['title']
		content = df.ix[i]['content']
		title = title.replace('-', '_')
		report_dict[title + '-' + institution+ '-' + date] = content
	for title in list(report_dict.keys()):
		shit_list = [':', '：', '"', "“", '”', "?",
					 "/", "<", "|", "\\", ">", "*", "×" ]
		title_txt = title
		for shit in shit_list:
			title_txt = title_txt.replace(shit, "_")
		addr = category[category_i][3] + \
			   title_txt + '.txt'

		content = report_dict[title]

		with open(addr, 'w') as f:
			json.dump(content, f, ensure_ascii=False)
			print(title)

def report_update(df, update_method, category_i):
	print('update database...')
	time_stp = pd.Timestamp(dt.datetime.now())
	time_stps = [time_stp for x in range(len(df))]
	df['time_stp'] = time_stps

	sql_engine = engine_str
	engine = create_engine(sql_engine)
	conn = sql.connect(sql_conn_str)
	cur = conn.cursor()
	try:
		df.to_sql(
			category[category_i][4],
			engine,
			schema='analysis_report',
			if_exists = update_method
		)
		print('一次性写入全部数据')
	except:
		df.to_sql(
			category[category_i][4],
			engine,
			schema='analysis_report',
			if_exists = update_method,
			chunksize = 10000
		)
		print('多次写入')
	conn.close()
	print('done!')

def get_sql_titles(category_i):

	sql_engine = engine_str
	engine = create_engine(sql_engine)
	conn = sql.connect(sql_conn_str)
	cur = conn.cursor()

	sql_str = '''
		select report_time_stp, title from analysis_report.%s
		order by report_time_stp desc
	''' % category[category_i][4]
	cur.execute(sql_str)
	raw = cur.fetchall()
	conn.close()

	sql_titles = [''.join(x) for x in raw]

	return sql_titles

def daily_update(start_page_num, end_page_num, category_i):
	print('更新%s中......' % category[category_i][3] )
	sql_titles = get_sql_titles(category_i)
	df = get_report_by_page(start_page_num, end_page_num, category_i)
	df_new = pd.DataFrame()

	for i in df.index:
		tmp = df.ix[i]['report_time_stp'] + df.ix[i]['title']
		if tmp not in sql_titles:
			df_new = df_new.append(df.ix[i])
	columns = df_new.columns.tolist()
	df_new = df_new.reset_index()[columns]
	try:
		report_update(df_new, 'append', category_i)
		df_new.to_excel('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_%s.xls' % category_i)
	except:
		try:
			df_new.to_json('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\ark.json')
		except:
			df_new.to_csv('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\ark.csv')
	print('%s更新完成!' % category[category_i][3])

	return df_new

# daily_update(1, 15, 'hg')
# daily_update(1, 15, 'hy')

# if __name__ == '__main__':

# 	df_hg = daily_update(1, 3, 'hg')
# 	df_hy = daily_update(1, 5, 'hy')
# 	try:
# 		report_to_txt(df_hg, 'hg')
# 		report_to_txt(df_hy, 'hy')
# 	except:
# 		print('txt files failed to write...')

# 	send_mail_via_com('hello Will', 'daily_update', 'sunweiyao@sinopac.com')
# 	os.system('pause')

