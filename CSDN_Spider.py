#!/usr/bin/env python3.7
# -*- coding:utf-8 -*-

import os
import sys
import xlwt
import time
import parsel
import requests
import argparse


# 1、获取问题网址
# 2、获取搜索链接
# 3、获取详情页链接
# 4、提取详情页文本
# 5、写出文本

header = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36 Edg/84.0.522.48'}

# 创建输出文件夹
def clean_and_make(abs_path):
    if not os.path.exists(abs_path):
        try:
            os.mkdir(abs_path)
        except FileExistsError:
            print('请检查输出文件夹!!!')

# 抛出报错信息
class Spider_Error(Exception):

    def __init__(self, e='爬取网页错误!'):
        self.e = e
        super().__init__()

    def __str__(self):
        return self.e

def timer(delay):
  time_start = time.time()
  print('开始爬虫: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
  time.sleep(delay)
  time_end = time.time()
  print('结束爬虫: ' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
  print('完成,总用时{}s,祝好运!'.format(round(time_end - time_start, 4)))

class csdn_spider:

    def __init__(self, key):
        self.key = key

    def getkey_url(self):
        url = 'https://so.csdn.net/so/search/s.do?q=%s&t=&u=' % (self.key)
        global header
        time.sleep(1)
        content = requests.get(url, headers = header)
        content = parsel.Selector(content.text)
        key_urls = content.xpath('//div/dl[@class="search-list J_search"]//a/@href').extract()
        return (key_urls)

    @staticmethod
    def getkey_text(url):
        global header
        time.sleep(1)
        content = requests.get(url, headers=header)
        content = parsel.Selector(content.text)
        key_text = content.xpath('//div[@id="content_views"]//p/strong/text() | //div[@id="content_views"]//p/text()').extract()
        return(''.join(key_text))

def receiver(input, output):
    clean_and_make(os.path.abspath(output))
    print('清理并创建{}输出文件夹'.format(output))
    with open(input, 'r', encoding = 'utf-8') as i, open(output + '/' + 'spider.txt', 'w', encoding = 'utf-8') as f:
        for line in i:
            key_urls = csdn_spider(line).getkey_url()
            key_text = []
            for url in key_urls:
                key_text.append(csdn_spider.getkey_text(url))
            f.write('\n'.join(key_text))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Spider text from CSDN website')
    parser.add_argument("-i",
                        metavar='file',
                        type=str,
                        nargs='?',
                        help='--help input file; "list of questions"')
    parser.add_argument("-o",
                        metavar='dir',
                        type=str,
                        nargs='?',
                        help='--help output file; "default named spider.txt"')
    args = parser.parse_args()
    if not all([args.i, args.o]):
        parser.print_help()
        sys.exit(1)
    receiver(args.i, args.o)