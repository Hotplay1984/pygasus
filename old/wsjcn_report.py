import jieba
import jieba.analyse
import pandas as pd
import datetime as dt
from urllib.request import urlopen
from bs4 import BeautifulSoup
import jieba
import jieba.analyse
import psycopg2 as sql
from sqlalchemy import create_engine
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'


def get_article_df():
	home_url = 'https://wallstreetcn.com'
	s = requests.Session()
	retries = Retry(total=5,
					backoff_factor=0.1,
					status_forcelist=[ 500, 502, 503, 504 ])
	s.mount('http://', HTTPAdapter(max_retries=retries))
	html = s.get(home_url, timeout = 10).text
	bs = BeautifulSoup(html, 'lxml')
	raws = bs.findAll('a', {'class': 'home-news-item__main__title'})
	df = pd.DataFrame()
	title_list, link_list = [], []
	for raw in raws:
		if 'premium' not in raw.attrs['href'] and 'http' not in raw.attrs['href']:
			link_list.append(home_url + raw.attrs['href'])
			title_list.append(raw.text.replace('\n', ''))
	df['title'] = title_list
	df['link'] = link_list

	time_stps = []
	content_list = []
	tag_list = []
	for i in range(len(link_list)):
		link = link_list[i]
		print(link)
		html = s.get(link, timeout = 10).text
		bs = BeautifulSoup(html, 'lxml')
		time_stp = bs.find('span', {'class':'meta-item__text'}).text
		time_stps.append(time_stp)
		raws_content = bs.findAll('p')
		contents = []
		for raw in raws_content:
			contents.append(raw.text + '\n')
		content = ''.join(contents)
		content_list.append(content)
		allowPOS = ('n', 'nr', 'eng', 'ns', 'j', 
					'nrt', 'nz','l', 's')
		jieba.analyse.set_stop_words('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\stop_words.txt')
		tags = jieba.analyse.extract_tags(content, topK=20, allowPOS = allowPOS)
		tag_str = ''
		for tag in tags:
			tag_str += tag + '-'
		tag_str = tag_str.strip('-')
		tag_list.append(tag_str)
	df['content'] = content_list
	df['tags'] = tag_list
	df['time_stp'] = time_stps
	df['report_date'] = [dt.datetime.strptime(x[:10], '%Y-%m-%d') for x in df['time_stp'].tolist()]
	df['report_date'] = [x.strftime('%Y%m%d') for x in df['report_date'].tolist()]
	df['institution'] = ['华尔街见闻' for x in range(len(df))]
	df['industry'] = ['中文财经' for x in range(len(df))]
	return df

def wsjcn_report_upload(df, update_method):
	
	sql_engine = engine_str
	engine = create_engine(sql_engine)
	conn = sql.connect(sql_conn_str)
	cur = conn.cursor()
	print('uploading...')
	df.to_sql(
				'wsjcn_report',
				engine,
				schema='analysis_report',
				if_exists = update_method
			)
	print('done!')
	return df

def wsjcn_daily_upload():
	print('updating wsjcn reports...')
	sql_str = '''
		select distinct link from analysis_report.wsjcn_report
	'''
	
	sql_engine = engine_str
	engine = create_engine(sql_engine)
	conn = sql.connect(sql_conn_str)
	cur = conn.cursor()
	cur.execute(sql_str)
	raw = cur.fetchall()
	conn.close()
	sql_links = [''.join(x) for x in raw]
	
	df_upload = pd.DataFrame()
	df_article = get_article_df()
	
	for i in df_article.index:
		link = df_article.at[i, 'link']
		if link not in sql_links:
			if link != 'dead link':
				df_upload = df_upload.append(df_article.ix[i])
	df_upload = wsjcn_report_upload(df_upload, 'append')
	df_upload.to_excel('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_wsjcn.xls')
	return df_upload

if __name__ == '__main__':
	df = wsjcn_daily_upload()