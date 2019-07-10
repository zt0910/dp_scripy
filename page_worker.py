import requests
import json
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
import re
import redis
import random
from hdfs.client import Client


def getListProxies():
    ip_list = []
    session = requests.session()
    headers = {'User-Agent': UserAgent().random}
    page = session.get("http://www.xicidaili.com/nn", headers=headers)
    soup = BeautifulSoup(page.text, 'lxml')
    taglist = soup.find_all('tr', attrs={'class': re.compile("(odd)|()")})
    for trtag in taglist:
        tdlist = trtag.find_all('td')
        proxy = {'http': 'http://' + tdlist[1].string + ':' + tdlist[2].string}
        ip_list.append(proxy)
    return ip_list


ip_list=getListProxies()


class RedisQueue(object):
    def __init__(self, name, namespace='queue', **redis_kwargs):
        self.__db = redis.Redis(**redis_kwargs)
        self.key = '%s:%s' % (namespace, name)

    def queue_size(self):
        return self.__db.llen(self.key)

    def put(self, item):
        self.__db.rpush(self.key,item)

    def get_wait(self, timeout=None):
        item = self.__db.blpop(self.key, timeout=timeout)
        return item

    def get_nowait(self):
        item = self.__db.lpop(self.key)
        return item



client = Client("http://192.168.31.51:50070")

q = RedisQueue('ftshop')
r = redis.Redis(host='localhost', port=6379)
# 这个字典是shopid中的字典
region_dict = {'市中心区': 1949,
 '车公庙': 7475,
 '上沙/下沙': 12322,
 '梅林': 1560,
 '华强北': 1556,
 '欢乐海岸': 30824,
 '皇岗': 1559,
 '景田': 12321,
 '新洲': 12225,
 '香蜜湖': 1951,
 '荔枝公园片区': 1573,
 '石厦': 12226,
 '八卦岭/园岭': 1557,
 '竹子林': 12324,
 '市民中心': 12323,
 '华强南': 3138,
 '岗厦': 12320,
 '福田保税区': 12319}


def save_page_hdfs(ipPort, file_path, contents):
    """保存网页源码到hdfs

    :param ipPort: hdfs连接地址
    :param file_path: 文件路径
    :param contents: 网页内容
    :return: None
    """
    client = Client(ipPort)
    with client.write(file_path) as writer:
        writer.write(bytes(contents, encoding='utf8'))


proxy = ip_list[0]
error=0
while 1:
    queue_len = r.llen('queue:ftshop')
    queue_index = 0
    s = requests.session()
    n = str(q.get_nowait(), encoding='utf8')
    data = json.loads(n)
    shopid = data['shopid']
    region = data['region']
    area = data['area'].encode("utf-8").decode("utf-8")
    headers = {'User-Agent': UserAgent().random,
               'Referer': 'https://m.dianping.com/shenzhen/ch10/r{0}'.format(region_dict[area])}
    url = 'https://m.dianping.com/shop/' + shopid
    try:
        respon = s.get(url, headers=headers, proxies=proxy)
    except Exception as e:
        error = 1
    i = 0
    while '验证中心' in respon.text or '抱歉！页面暂' in respon.text or respon.status_code != 200 or error == 1:
        i = i + 1
        if i < len(ip_list):
            proxy = ip_list[i]
            try:
                respon = s.get(url, headers=headers, proxies=proxy)
            except Exception as e:
                error == 1
        else:
            q.put(n)
            break

    # 用来判断队列是否循环一遍，更新ip池
    queue_index += 1
    if queue_index == queue_len:
        ip_list = getListProxies()

    if '验证中心' not in respon.text:
        if '抱歉！页面暂' not in respon.text:
            print('success')
            print(n)
            filepath = '/dazhongdianping/sz/{0}/{1}/{2}.html'.format(region, area, shopid)
            try:
                save_page_hdfs('http://192.168.31.51:50070', filepath, respon.text)
            except Exception as e:
                pass
    time.sleep(1)