import requests
import re
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from time import sleep
import random
import redis
import json


# 构建redis队列
class RedisQueue:
    def __init__(self, host, port, name):
        self.host = host
        self.port = port
        self.name = name
        self.__db = redis.Redis(host=self.host, port=self.port)
        self.key = 'queue:%s' % (self.name)

    def queue_size(self):
        return self.__db.llen(self.key)

    def put(self, item):
        self.__db.rpush(self.key, item)

    def get_wait(self, timeout=None):
        item = self.__db.blpop(self.key, timeout=timeout)
        return item

    def get_nowait(self):
        item = self.__db.lpop(self.key)
        return item


# 获取食品类别
def get_classfy():
    classfy_list = []
    url = 'http://www.dianping.com/shenzhen/ch10'
    user_agent = UserAgent().random
    headers = {'User-Agent': user_agent}
    res = requests.get(url, headers=headers)
    print(res.text)
    soup = BeautifulSoup(res.text, 'html')
    classfy = soup.find('div', id='classfy')
    for i in range(len(classfy.find_all('a'))):
        classfy_list.append(int(classfy.find_all('a')[i]['data-cat-id']))
    return classfy_list


regionList = [
    # 福田区
    ('futian', 'http://m.dianping.com/shenzhen/ch10/r29'),
    # 南山区
    ('nanshan', 'http://m.dianping.com/shenzhen/ch10/r31'),
    # 罗湖区
    ('luohu', 'http://m.dianping.com/shenzhen/ch10/r30'),
    # 盐田区
    ('yantian', 'http://m.dianping.com/shenzhen/ch10/r32'),
    # 龙华区
    ('longhua', 'http://m.dianping.com/shenzhen/ch10/r12033'),
    # 龙岗区
    ('longgang', 'http://m.dianping.com/shenzhen/ch10/r34'),
    # 宝安区
    ('baoan', 'http://m.dianping.com/shenzhen/ch10/r33'),
    # 坪山区
    ('pingshan', 'http://m.dianping.com/shenzhen/ch10/r12035'),
    # 光明区
    ('guangming', 'http://m.dianping.com/shenzhen/ch10/r89951')
]


# 获取各行政区单位id
def get_region_list(regionUrl):
    region_id_name = []
    user_agent = UserAgent().random
    headers = {'User-Agent': user_agent}
    res = requests.get(regionUrl, headers=headers)
    print(res.text)
    soup = BeautifulSoup(res.text, 'lxml')
    region = soup.find('div', class_='menu sub')
    for i in range(1, len(region.find_all('a', class_="item Fix"))):
        region_id_name.append(
            (int(region.find_all('a')[i]['data-itemid']), str(region.find_all('a')[i]['data-itemname'])))
    return region_id_name


# 将各个区域串起来
def get_all_area_list(regionurlList):
    all_area = []
    for regionname, regionurl in regionurlList:
        region_dict = {}
        region_id_name = get_region_list(regionurl)
        region_dict[regionname] = region_id_name
        all_area.append(region_dict)
    return all_area


# 组合美食分类和区域ID，获得url
def recostution_url(classfy_list, all_area):
    Reurl = []
    for classfy in classfy_list:
        for data in all_area:
            for region, regiondata in data.items():
                for area_id, area_name in regiondata:
                    Reurl.append((region, area_name, area_id,
                                  'http://m.dianping.com/shenzhen/ch10/' + 'g' + str(classfy) + 'r' + str(area_id)))
    return Reurl


# 获取页面
def get_shopContent(Reurl):
    user_agent = UserAgent().random
    headers = {'User-Agent': user_agent}
    res = requests.get(Reurl, headers=headers)
    return res.text


# 获取shopid
def get_shopId(Reurl):
    resource = get_shopContent(Reurl)
    while '验证中心' in resource:
        print('出现验证码')
        sleep(random.randint(1, 3))
        resource = get_shopContent(Reurl)
        # 保存网页源码
    shopidList = []
    soup = BeautifulSoup(resource, 'lxml')
    p2 = re.compile(r'{.*}', re.S)
    string = soup.find_all('script')[2].string.strip()
    string = string.replace('true', 'True')
    string = string.replace('false', 'False')
    content = eval(re.findall(p2, string)[0])
    for adshop in content['mapiSearch']['data']['list']:
        shopidList.append(adshop['shopUuid'])
    return shopidList


# 爬取宝安区的shopid
baoan = [('baoan', 'http://m.dianping.com/shenzhen/ch10/r33')]
classfy_list = get_classfy()
all_area = get_all_area_list(baoan)

Reurl = recostution_url(classfy_list, all_area)

# 区域对应的字典，在爬取详情页会用到
region_name_id_dict = {}
for data in all_area:
    for region, regiondata in data.items():
        for area_id, area_name in regiondata:
            region_name_id_dict[area_name] = area_id

# 保存shopid
shopBAList = []
for i in range(len(Reurl)):
    print(i)
    shopidList = get_shopId(Reurl[i][3])
    for shopid in set(shopidList):
        shop_location = {}
        shop_location['shopid'] = shopid
        shop_location['region'] = Reurl[i][0]
        shop_location['area'] = Reurl[i][1]
        shopBAList.append(shop_location)
    sleep(random.randint(1, 3))

# 将shopid加入到队列中
shoptask = RedisQueue('192.168.31.51', 6379, 'baoan_shopid')
for x in shopBAList:
    shoptask.put(json.dumps(x))