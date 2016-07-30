#encoding=utf-8
import threading
import sys
import time
import os
import re
import signal
import json
import urllib2
import codecs
from bs4 import BeautifulSoup
from selenium import webdriver


class PageInfo:

	def __init__(self, url=None):
		self.url = 'None'
		self.title = 'None'
		self.host_content = 'None'
		self.host_author = 'None'
		self.publish_time = 'None'
		self.flower = list()
		self.available = True

		if url is not None:
			self.url = url
			self.page_info_extractor(url)


	def print_page(self):
		print self.title
		print self.host_author, self.host_content, self.publish_time
		for item in self.flower:
			print item['author'], item['content'], item['publish_time']


	def to_json_format(self):
		page_dic = dict()
		page_dic['title'] = self.title
		page_dic['host_content'] = self.host_content
		page_dic['publish_time'] = self.publish_time
		page_dic['host_author'] = self.host_author

		#print page_dic['title']
		#print page_dic['host_author'], page_dic['host_content'], page_dic['publish_time']

		page_dic['comments'] = list()
		for item in self.flower:
			page_dic['comments'].append({'content': item['content'], 'author': item['author'], 'publish_time': item['publish_time']})
			#print item['author'], item['content'], item['publish_time']

		#print page_dic
		return json.dumps(page_dic, encoding='utf-8')


	def write2file(self, fo):
		fo.write('url::%s\n'%self.url)
		fo.write('title::%s\n'%self.title)
		fo.write('host_author::%s\thost_content::%s\tpublish_time::%s\n'%(self.host_author, self.host_content, self.publish_time))

		for item in self.flower:
			fo.write('author::%s\tcontent::%s\tpublish_time::%s\n'%(item['author'], item['content'], item['publish_time']))

		fo.write('--Page End--\n')


	def page_info_extractor(self, url):

		try:
			page = urllib2.urlopen(url, timeout=6).read()
		except:
			print 'timeout at crawl url:%s'%url

		soup = BeautifulSoup(page, from_encoding='UTF-8')

		try:
			self.title = soup.find(name='h3', attrs={'class': 'core_title_txt pull-left text-overflow '}).get_text()
			comments_dom = soup.find_all(name='div', attrs={'class': 'l_post l_post_bright j_l_post clearfix '})
		except:
			print 'Exception at url:%s'%url
			self.available = False
			return

		count = 0
		for comment in comments_dom:
			try:
				content = comment.find(name='div', attrs={'class': 'd_post_content j_d_post_content '}).get_text()

				i = 0
				while i < len(content) and content[i] == ' ':
					i += 1
				if i >= len(content):
					continue
				content = content[i:]

				while '  ' in content:
					content = content.replace('  ', ' ')

				content = content.replace('\r', '')
				content = content.replace('\n', ' ')
				content = content.replace('\t', ' ')

				author = comment.find(name='a', attrs={'class': 'p_author_name j_user_card'}).get_text()
				tail_infos = comment.find_all(name='span', attrs={'class': 'tail-info'})

				publish_time = tail_infos[len(tail_infos) - 1].get_text()

				if count == 0:
					self.host_content = content
					self.host_author = author
					self.publish_time = publish_time
					# print self.host_author, self.host_content, self.publish_time
				else:
					self.flower.append({'content': content, 'author': author, 'publish_time': publish_time})
					# print author, content, publish_time

				count += 1

			except:
				#print 'Exception in url:%s'%url
				continue


def page_url_crawler(url_prefix, max_num, step=50):
	page_urls = list()
	num = 0
	while num < max_num	:
		url = url_prefix + str(num)

		print '\rnum:%d page_url_N:%d'%(num, len(page_urls)),
		sys.stdout.flush()

		num += step

		page = urllib2.urlopen(url, timeout=6).read()
		soup = BeautifulSoup(page, from_encoding='UTF-8')

		itemlist = soup.find_all(name='a', attrs={'class': 'j_th_tit '})
		for item in itemlist:
			page_urls.append('http://tieba.baidu.com' + item.get('href'))

	print '%d pages in total'%len(page_urls)

	return page_urls


allurls = list()
#out_data_fo = ''
#urlindex = 0


def multi_thread_data_crawler(urls_path, threadN=4):
	global allurls

	fi = open(urls_path, 'r')
	for line in fi:
		line = line.replace('\n', '')
		allurls.append(line)
	fi.close()
	print '%d urls in total'%len(allurls)

	threads = []
	i = 0
	while i < threadN:
		thread = wormThread(i)
		i += 1
		thread.start()
		threads.append(thread)
		time.sleep(2)

	while True:
		alive = False
		for i in range(threadN):
			alive = alive or threads[i].isAlive()
		if not alive:
			break
		time.sleep(6)


class wormThread(threading.Thread):
	def __init__(self, threadID):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.urlidx = 0
		self.count = 0

	def run(self):
		global urlindex
		global urlindexLock
		global fileWriteLock
		global allurls
		global out_data_fo

		while True:
			urlindexLock.acquire()
			self.urlidx = urlindex
			urlindex += 1
			urlindexLock.release()
			if self.urlidx >= len(allurls):
				break

			page = PageInfo(allurls[self.urlidx])
			if page.available:
				fileWriteLock.acquire()
				page.write2file(out_data_fo)
				fileWriteLock.release()
				self.count += 1

			print 'Thread_%d wrote pageinfo %d to file, wrote %d total'%(self.threadID, self.urlidx, self.count)


def data_crawler(url_prefix, opath):
	urls = page_url_crawler(url_prefix, 50)

	count = 0
	fo = codecs.open(opath, 'w', 'utf-8')
	for url in urls:
		page = PageInfo(url)
		if page.available:
			page.write2file(fo)

		count += 1
		print '\rcrawl %d / %d pages'%(count, len(urls)),
		sys.stdout.flush()

	fo.close()


def output_urls(url_prefix, opath):
	fo = open(opath, 'w')
	urls = page_url_crawler(url_prefix, 3950)
	for url in urls:
		fo.write('%s\n'%url)
	fo.close()


if __name__ == '__main__':
	#url_prefix = 'http://tieba.baidu.com/f?kw=%E6%B5%A6%E5%8F%91&ie=utf-8&pn='
	#opath = 'urls.txt'
	#output_urls(url_prefix, opath)

	urlindexLock = threading.Lock()
	fileWriteLock = threading.Lock()
	urlindex = 0
	opath = 'spd_tieba_data.txt'
	out_data_fo = codecs.open(opath, 'w', 'utf-8')

	multi_thread_data_crawler('urls.txt', threadN=10)

	#data_crawler(url_prefix, opath)
