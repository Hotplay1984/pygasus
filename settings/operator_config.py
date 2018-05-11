sql_engine_str = 'postgresql://postgres:sunweiyao366@localhost:5432/quant'
headers_setting = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.62 Safari/537.36'}
proxy_setting = {
	'http':'http://119.28.222.122:8888',
	'https':'https://119.28.222.122:8888'
}

sql_setting = {
	'sql_type': 'postgresql',
	'user': 'progres',
	'pwd': 'sunweiyao366',
	'server_ip': 'localhost',
	'server_port': '5432',
	'database': 'quant'
}

def get_sql_setting():
	return '%s://%s:%s@%s:%s/%s' % (sql_setting['sql_type'], 
		sql_setting['user'],
		sql_setting['pwd'],
		sql_setting['server_ip'],
		sql_setting['server_port'], 
		sql_setting['database'])