rom page_content import *
from google.protobuf import json_format
from hdfs.client import Client
import redis
import proto_demo_pb2
import pymongo
from fontTools.ttLib import TTFont
import re
import requests

mongoclient = pymongo.MongoClient("mongodb://localhost:5002/")
meta_db = mongoclient["dp_merchant_data"]
meta_col = meta_db["merchant_meta_data"]
data_col = meta_db['merchant_detail_data']

client = Client("http://192.168.31.51:50070")

def get_svg_url(soup):
    svgtextcss = re.search(r'href="([^"]+svgtextcss[^"]+)"', str(soup), re.M)
    woff_url = 'http:' + svgtextcss.group(1)
    return woff_url


def get_commentCount_woff(woff_url):
    svg_html = requests.get(woff_url).text
    lines = svg_html.split('PingFangSC-')
    partern = re.compile(r',(url.*commentCount)')
    for line in lines:
        out = partern.findall(line)
        if len(out) > 0:
            woff = re.compile('\((.*?)\)')
            comment_url = 'http:' + woff.findall(out[0])[0].replace('"', '')
    with open('/home/tao/woff/comment.woff', 'wb') as writer:
        writer.write(requests.get(comment_url).content)
    return None
def get_number_woff(woff_url):
    svg_html = requests.get(woff_url).text
    lines = svg_html.split('PingFangSC-')
    partern = re.compile(r',(url.*number)')
    for line in lines:
        out = partern.findall(line)
        if len(out) > 0:
            woff = re.compile('\((.*?)\)')
            number_url = 'http:' + woff.findall(out[0])[0].replace('"', '')
    with open('/home/tao/woff/number.woff', 'wb') as writer:
        writer.write(requests.get(number_url).content)
    return None
def get_address_woff(woff_url):
    svg_html = requests.get(woff_url).text
    lines = svg_html.split('PingFangSC-')
    partern = re.compile(r',(url.*address)')
    for line in lines:
        out = partern.findall(line)
        if len(out) > 0:
            woff = re.compile('\((.*?)\)')
            address_url = 'http:' + woff.findall(out[0])[0].replace('"', '')
            print(address_url)
    with open('/home/tao/woff/address.woff', 'wb') as writer:
        writer.write(requests.get(address_url).content)
    return None

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

html_parser = RedisQueue('192.168.31.51', 6379, 'html_ns')
while 1:
    merchant = proto_demo_pb2.DianpingMerchant()
    n = html_parser.get_nowait()
    file = str(n, encoding='utf8')
    print(file)
    shopid, city, region, area, filepath = get_file_info(file)

    meta_dict = {'shopid': shopid, 'city': city, 'region': region, 'area': area, 'filepath': filepath,
                 'url': 'https://m.dianping.com/shop/' + shopid}
    meta_col.insert_one(meta_dict)
    soup = read_html(file)
    woff_url = get_svg_url(soup)
    get_commentCount_woff(woff_url)
    get_number_woff(woff_url)
    get_address_woff(woff_url)
    # number
    numberfont = TTFont('/home/tao/woff/number.woff')
    numberfont.saveXML('/home/tao/woff/number.xml')
    # comment
    commentfont = TTFont('/home/tao/woff/comment.woff')
    commentfont.saveXML('/home/tao/woff/comment.xml')

    # address
    addressfont = TTFont('/home/tao/woff/address.woff')
    addressfont.saveXML('/home/tao/woff/address.xml')

    number_TTGlyphs = numberfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]
    comment_TTGlyphs = commentfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]
    address_TTGlyphs = addressfont['cmap'].tables[0].ttFont.getGlyphOrder()[2:]

    number_dict = {}
    for i, x in enumerate(number_TTGlyphs):
        number_dict[x] = i
comment_dict = {}
    for i, x in enumerate(comment_TTGlyphs):
        comment_dict[x] = i

    address_dict = {}
    for i, x in enumerate(address_TTGlyphs):
        address_dict[x] = i

    shopname = get_shop_name(soup)
    print(shopname)
    shop_id, city_id, category = get_baseinfo(soup)
    print(shop_id, city_id, category)
    comment_count = get_comment_count(soup,comment_TTGlyphs,comment_dict)
    print(comment_count)
    star, taste, environment, service = get_merchant_score(soup,number_TTGlyphs,number_dict)
    print(star, taste, environment, service)
    tell_number = get_telphonenumber(soup)
    picCount = get_picture_coount(soup)
    print(picCount)
    price = get_price(soup)
    print(price)
    address = get_adress(soup,address_TTGlyphs,address_dict)
    print(address)
    open_time = opening_hours(soup)
    print(open_time)
    rankname, ranking = get_rank(soup)
    print(rankname, ranking)
    dish_name, recommender_count = get_recommend(soup)
    print(dish_name, recommender_count)
    coupon, discount_price,original_price, sale_count = get_coupon(soup)
    print(coupon, original_price, discount_price, sale_count)
    merchant.name = shopname
    merchant.shop_id = shop_id
    merchant.city_id = city_id
    merchant.category = category
    merchant.pic_count = picCount

    score = merchant.scores
    score.overall = int(star)/10
    score.taste = taste
    score.environment = environment
    score.service = service

    merchant.comment_count = comment_count
    merchant.avg_price = price
    merchant.address = address
    if len(tell_number)!=0:
        merchant.phone_number = tell_number
    if len(rankname)!=0:
        rank = merchant.rankings.add()
        rank.name = rankname
        rank.ranking = ranking

    for i in range(len(coupon)):
        coupons = merchant.coupons.add()
        if coupon[i] == 'CASH':
            coupons.type = proto_demo_pb2.CASH
        else:
            coupons.type = proto_demo_pb2.COMBO
        coupons.original_price =int( original_price[i])
        coupons.discount_price =int( discount_price[i])
        coupons.sales_count = int(sale_count[i])

    for i in range(len(dish_name)):
        recommend = merchant.recommended_dishes.add()
        recommend.dish_name = dish_name[i]
        recommend.recommender_count = recommender_count[i]
    if len(open_time)!=0:
        merchant.open_hours = open_time
    data = json_format.MessageToDict(merchant,including_default_value_fields=True)
    data_col.insert_one(data)