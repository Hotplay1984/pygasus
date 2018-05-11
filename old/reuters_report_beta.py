import jieba
import jieba.analyse
import pandas as pd
import datetime as dt
from pytz import common_timezones, all_timezones
from urllib.request import urlopen
from bs4 import BeautifulSoup
import psycopg2 as sql
from sqlalchemy import create_engine
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from send_email import *
import os,traceback
from proxy_setting import *
from time import sleep

engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
sql_conn_str = 'dbname = quant user=postgres password=sunweiyao366 host=localhost port=5432'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
home_url = 'http://cn.reuters.com'
channel_dict = {
	'cnInvNews':['投资资讯',],
	'CNAnalysesNews':['深度分析','/news/analyses'],
	'CNTopGenNews':['时事要闻','/news/generalnews'],
	'CNColumn':['专栏-中国财经','/news/CnColumn'],
	'IntColumn':['专栏-国际财经','/news/IntColumn'],
	'ComColumn':['专栏-大宗商品','/news/ComColumn'],
	'chinaNews':['中国财经','/news/china'],
	'CNIntlBizNews':['国际财经','/news/internationalbusiness'],
	'opinions':['财经视点','/news/opinions'],
	'asiaNews':['亚洲',],
	'CNEntNews':['娱乐体育'],
}
channel_list = ['首页新闻', '中国财经', '深度分析', '时事要闻', 
				'国际财经', '新闻人物', '财经视点']

s = requests.Session()
retries = Retry(total=5,
				backoff_factor=0.1,
				status_forcelist=[404, 500, 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

def reuter_stp(dt_0):
	week_dict = {
		'0': '星期一',
		'1': '星期二',
		'2': '星期三',
		'3': '星期四',
		'4': '星期五',
		'5': '星期六',
		'6': '星期七',
	}
	year, month, day = str(dt_0.year), str(dt_0.month), str(dt_0.day)
	weekday = str(dt_0.weekday())
	hour, minute = str(dt_0.hour), str(dt_0.minute)
	if len(hour) == 1:
		hour = '0' + hour 
	if len(minute) == 1:
		minute = '0' + minute 

	reuter_stp = year + '年' + month + ' 月' + day + '日' + ' ' +\
				week_dict[weekday] + ' ' + hour + ':' + minute + ' BJT'

	return reuter_stp

def story_links(channel_url):
	print('Retrieving story links from %s' % channel_url)
	url = home_url + channel_url
	html = s.get(url, headers=headers, timeout = (40, 40)).text
	bs = BeautifulSoup(html, 'lxml')

	top_story = bs.find('div',{'class':'topStory'}).find('a').attrs['href']
	other_storys = bs.findAll('div', {'class':'feature'})
	other_links = []
	for story in other_storys:
		link = story.find('a').attrs['href']
		if 'article' in link:
			other_links.append(link)
	links = [top_story,] + other_links
	links = list(set(links))
	return links

def all_links():
	links = []
	for channel in channel_dict.keys():
		if len(channel_dict[channel]) > 1:
			channel_url = channel_dict[channel][1]
			links += story_links(channel_url)
	return links

def article_content(article_url):
	url = home_url + article_url
	html = s.get(url, headers=headers,timeout = (40, 40)).text
	bs = BeautifulSoup(html, 'lxml')

	channel_code = bs.find('meta', {'name': 'DCSext.ContentChannel'}).attrs['content']
	try:
		channel = channel_dict[channel_code][0]
	except:
		channel = channel_code
	print(channel)
	time_str = bs.find('meta', {'name': 'REVISION_DATE'}).attrs['content']
	stamp = pd.Timestamp(dt.datetime.strptime(time_str, '%a %b %d %H:%M:%S UTC %Y')).tz_localize('utc')
	time_stamp = pd.to_datetime(stamp.tz_convert('Etc/GMT-8'))
	time_stamp_str = reuter_stp(time_stamp)
	report_date = dt.datetime(time_stamp.year, time_stamp.month, time_stamp.day)
	title = bs.find('meta', {'name':'sailthru.title'}).attrs['content']
	content, raw_content_0 = '    ', ''
	if bs.find('pre'):
		raw_content_0 = bs.find('pre').text
	raw_content = bs.findAll('p')
	content = content + raw_content_0
	for p in raw_content[1:]:
		content += p.text + '\n    '
	allowPOS = ('n', 'nr', 'eng', 'ns', 'j', 
				'nrt', 'nz','l', 's')
	tags = jieba.analyse.extract_tags(content, topK=20, allowPOS = allowPOS)
	tag_str = ''
	for tag in tags:
		tag_str += tag + '-'
		
	content_list = [title, time_stamp_str, channel, url, 
					tag_str, content, report_date]
	df = pd.DataFrame(content_list).transpose()
	df.columns = ['title', 'time_stp', 'section', 'link', 'tags',
					'content', 'report_date']
	print('Channel: %s; Title: %s' %(channel, title))
	return df

def all_article():
	links = all_links()
	df = pd.DataFrame()
	for link in links:
		df_temp = article_content(link)
		df = df.append(df_temp)
	time_stp = pd.Timestamp(dt.datetime.now())
	time_stps = [time_stp for x in range(len(df))]
	df['upload_time_stp'] = time_stps
	columns = df.columns.tolist()
	df = df.reset_index()[columns]
	return df

def reuters_report_upload(df, update_method):
	sql_engine = engine_str
	engine = create_engine(sql_engine)
	conn = sql.connect(sql_conn_str)
	cur = conn.cursor()
	print('uploading...')
	df.to_sql(
				'reuters_report',
				engine,
				schema='analysis_report',
				if_exists = update_method
			)
	print('done!')
	return df

def reuters_daily_upload():
	print('updating reuters reports...')
	sql_str = '''
		select distinct link from analysis_report.reuters_report
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
	df_article = all_article()
	
	for i in df_article.index:
		link = df_article.at[i, 'link']
		if link not in sql_links:
			if link != 'dead link':
				df_upload = df_upload.append(df_article.ix[i])
	df_upload = reuters_report_upload(df_upload, 'append')
	df_upload.to_excel('C:\\Users\\client\\Desktop\\python_work\\work\\market_data\\daily_update_reuters.xls')
	send_mail_via_com('hello Will', 
		'daily_update', 
		'sunweiyao@sinopac.com', 
		select_file = 'daily_update_reuters.xls')
	return df_upload

if __name__ == '__main__':
	try:
		proxy_setting = Proxy_Setting()
		proxy_setting.start_proxy()
		sleep(5)
		df = reuters_daily_upload()
		print('Reuters Articles Updated!')
		os.system('pause')
	except:
		traceback.print_exc()
		os.system('pause')