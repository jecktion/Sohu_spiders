# -*- coding: utf-8 -*-
# 此程序用来抓取搜狐视频的数据
import csv
import hashlib
import os

import requests
import time
import random
import re
from fake_useragent import UserAgent, FakeUserAgentError
from multiprocessing.dummy import Pool
from save_data import database

class Spider(object):
	def __init__(self):
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		# self.date = '2000-10-01'
		# self.limit = 5000
		self.db = database()
	
	def get_headers(self):
		# user_agent = self.ua.chrome
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
					   'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
					   'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
					   'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
					   'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
					   'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
					   'Opera/9.52 (Windows NT 5.0; U; en)',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
					   'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
					   'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
					   'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'host': "api.my.tv.sohu.com",
		           'connection': "keep-alive",
		           'user-agent': user_agent,
		           'accept': "*/*",
		           'referer': "http://www.iqiyi.com/lib/m_216426014.html?src=search",
		           'accept-encoding': "gzip, deflate",
		           'accept-language': "zh-CN,zh;q=0.9"
		           }
		return headers

	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub('/', ";", x)
		x = re.sub(re.compile('\s{2,}'), ' ', x)
		x = re.sub(re.compile('[\n\r]'), ' ', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HK847SP62Z59N54D"
		proxyPass = "C0604DD40C0DD358"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies

	def get_comments_pagenums(self, videoid):  # 获取某一个视频的总评论页数
		print videoid
		url = "https://api.my.tv.sohu.com/comment/api/v1/load"
		querystring = {"topic_id": videoid,
		               "topic_type": "1",
		               "source": "2",
		               "page_size": "10",
		               "sort": "0",
		               "ssl": "0",
		               "page_no": "1",
		               "reply_size": "2"}
		retry = 5
		while 1:
			try:
				total = requests.get(url, proxies=self.GetProxies(), params=querystring, timeout=10).json()['data'][
					'total_page']
				# if int(total) > self.limit / 10:
				# 	total = self.limit / 10
				return int(total)
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue

	def get_comments(self, film_url, product_number, plat_number, plid):  # 获取某单个视频的所有评论
		pagenums = self.get_comments_pagenums(plid)
		if pagenums is None:
			return None
		else:
			print u'pagenums:%d' % pagenums
			ss = []
			for page in range(1, pagenums + 1):
				ss.append([film_url, product_number, plat_number, plid, page])
			pool = Pool(8)
			items = pool.map(self.get_comments_page, ss)
			pool.close()
			pool.join()
			mm = []
			for item in items:
				if item is not None:
					mm.extend(item)

			with open('data_comment.csv', 'a') as f:
				writer = csv.writer(f, lineterminator='\n')
				writer.writerows(mm)

			# print u'%s 开始录入数据库' % product_number
			# self.save_sql('T_COMMENTS_PUB_MOVIE', mm)  # 手动修改需要录入的库的名称
			# print u'%s 录入数据库完毕' % product_number
	
	def get_comments_page(self, ss):  # 获取所有评论数据
		film_url, product_number, plat_number, plid, page = ss
		print page
		url = "http://api.my.tv.sohu.com/comment/api/v1/load"
		querystring = {"topic_id": plid,
		               "topic_type": "1",
		               "source": "2",
		               "page_size": "10",
		               "sort": "0",
		               "ssl": "0",
		               "page_no": str(page),
		               "reply_size": "2"}
		results = []
		retry = 20
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(), timeout=10,
				                    params=querystring).json()['data']
				content = text['comments']
				last_modify_date = self.p_time(time.time())
				for item in content:
					try:
						nick_name = item['user']['nickname']
					except:
						nick_name = ''
					try:
						tmp1 = self.p_time(item['createtime'])
						cmt_date = tmp1.split()[0]
						cmt_time = tmp1
						# if cmt_date < self.date:
						# 	continue
					except:
						cmt_date = ''
						cmt_time = ''
					try:
						comments = self.replace(item['content'])
					except:
						comments = ''
					try:
						like_cnt = str(item['like_count'])
					except:
						like_cnt = '0'
					try:
						cmt_reply_cnt = str(item['reply_count'])
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					source_url = film_url
					tmp = [product_number, plat_number, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, source_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print e
					return results
				else:
					continue
	
	def get_pl_id(self, film_url):  # 获取电影id
		retry = 5
		while 1:
			try:
				text = requests.get(film_url, timeout=10).text
				if '/item/' in film_url:
					p = re.compile('var playlistId="(\d+?)";')
				else:
					p = re.compile('var playlistId = "(\d+?)";')
				pl_id = re.findall(p, text)[0]
				return pl_id
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_film_id(self, plid):  # 获取电视剧每一集的id
		url = "http://pl.hd.sohu.com/videolist"
		querystring = {"playlistid": plid, "order": "0", "cnt": "1", "withLookPoint": "1", "preVideoRule": "1",
		               "ssl": "0"}
		retry = 5
		while 1:
			try:
				film_ids = []
				text = requests.get(url,  params=querystring, timeout=10).json()['videos']
				for item in text:
					film_ids.append(item['vid'])
				if len(film_ids) == 0:
					return None
				else:
					return film_ids
			except:
				retry -= 1
				if retry == 0:
					return None
				else:
					continue
	
	def get_comments_all(self, product_url, product_number, plat_number):  # 获取某个产品的所有评论
		plid = self.get_pl_id(product_url)
		if plid is None:
			return None
		else:
			vids = self.get_film_id(plid)
			print u'共 %d 集' % len(vids)
			if vids is None:
				return None
			else:
				for vid in vids:
					self.get_comments(product_url, product_number, plat_number, vid)


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('new_data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'http' in i[2]:
				s.append([i[2], i[0], 'P05'])
	for j in s:
		print j[1],j[0]
		spider.get_comments_all(j[0], j[1], j[2])
	spider.db.db.close()
