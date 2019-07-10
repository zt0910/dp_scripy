from html_parser.common import *
from bs4 import BeautifulSoup
from hdfs.client import Client
import re

client = Client("http://192.168.31.51:50070")


def get_file_info(filepath):
    file_info = filepath.split('/')
    city = file_info[2]
    region = file_info[3]
    area = file_info[4]
    shopid = file_info[5].split('.')[0]
    return shopid, city, region, area, filepath


def read_html(filepath):
    with client.read(filepath, encoding='utf-8') as reader:
        respon = reader.read()
    soup = BeautifulSoup(respon, 'lxml')
    return soup


def get_shop_name(soup):
    try:
        shopname = soup.find('h1', class_='mutilPics-shop-name').string
    except:
        shopname = soup.find('h1', class_='shopName').string
    return shopname


def get_baseinfo(soup):
    Idpattern = re.compile('.*\{.*{(.*?)}}')
    idinfo = Idpattern.findall(str(soup.find('div', class_='address_left')))
    idInfodict = {}
    for x in idinfo[0].split(','):
        idInfodict[x.split(':')[0]] = x.split(':')[1].strip("''")
    shop_id = idInfodict['shopid']
    city_id = idInfodict['city_id']
    category = idInfodict['category']
    return shop_id, city_id, category


def get_comment_count(soup, comment_TTGlyphs, comment_dict):
    comment_count = 0
    try:
        taglist = ['Multi-itemNum', 'itemNum']
        for tag in taglist:
            comment = soup.find('div', class_=tag)
            if comment != None:
                comment = comment.text.strip()
                break
            else:
                continue
        comment_count = int(woff_change(comment, comment_TTGlyphs, comment_dict)[:-1])
        return comment_count
    except:
        return comment_count


def get_merchant_score(soup, number_TTGlyphs, number_dict):
    starpattern = re.compile('class="star starBig star-(.*)">')
    star = starpattern.findall(str(soup))[0]
    environment, taste, service = 0, 0, 0
    try:
        descrip_list = [('div', 'Multi-description'), ('div', 'description')]
        for desc in descrip_list:
            score_mes = soup.find(desc[0], class_=desc[1])
            score_digit = []
            if score_mes != None:
                for x in score_mes.text.strip().split('\n'):
                    detailist = x.split(':')[1].split('.')
                    score_digit.append(int(woff_change(detailist, number_TTGlyphs, number_dict)) / 10)
                    taste = score_digit[0],
                    environment = score_digit[1]
                    service = score_digit[2]
                    break
            else:
                continue
        return star, taste, environment, service
    except:
        return star, taste, environment, service


def get_telphonenumber(soup):
    tell_number = ''
    try:
        tell_info = soup.find('div', class_='aboutPhoneNum')
        tell_number = tell_info.find('a', class_='tel')['href'].split(':')[1]
        return tell_number
    except:
        return tell_number


def get_picture_coount(soup):
    picCount = 0
    try:
        picCount = soup.find('div', class_='picCount').string
        return picCount
    except:
        return picCount


def get_price(soup):
    avg_price = 0
    try:
        pricedict = {'div': 'Multi-price', 'span': 'price'}
        for k, v in pricedict.items():
            pricecontent = soup.find(k, class_=v)
            if pricecontent != None:
                avg_price = pricecontent.string
                print(avg_price)
                break
            else:
                continue
    except:
        pass
    return avg_price


def get_adress(soup, address_TTGlyphs, address_dict):
    pattern = re.compile('>(.*?)<')
    adress = pattern.findall(str(soup.find_all('span', class_='addressText')[0]))
    location = woff_change(adress, address_TTGlyphs, address_dict)
    return location


def open_time(soup):
    open_time = ''
    try:
        taglist = soup.find_all('div', class_='otherInfo')
        for tr in taglist:
            td = tr.find_all('div')
            open_time += td[1].string.strip().strip('\n')
        open_time.replace('\n', '')
        return open_time
    except:
        return open_time


def get_rank(soup):
    ranking, rankname = '', ''
    try:
        rankname = soup.find('div', class_="rankText").string
        ranking = ''
        pattern = re.compile('>(.*?)<')
        for x in pattern.findall(str(soup.find('div', class_='rankNum'))):
            ranking += x
        return rankname, ranking
    except:
        return rankname, ranking


def get_recommend(soup):
    recommender_count = []
    dish_name = []
    numpattern = re.compile('\d+')
    try:
        dish_info=soup.find('div',class_='dishPics')
        if dish_info !=None:
            for tag_a in dish_info.find_all('a',class_='dishItem'):
                dish_name.append(tag_a.find('div',class_='dishName').string)
                if tag_a.find('div',class_='recommendonfo')!=None:
                    recommender_count.append(numpattern.findall((tag_a.find('div',class_='recommendInfo').text)[0]))
                else:
                    recommender_count.append(0)
        return dish_name, recommender_count
    except:
        return dish_name, recommender_count


def get_coupon(soup):
    coupon, original_price, discount_price, sale_count = [], [], [], []
    try:
        tuanlist = soup.find('div', class_='tuan-list')
        coupon = []
        original_price = []
        discount_price = []
        sale_count = []
        for info in tuanlist.find_all('div', class_='newtitle'):
            if '代金券' in info.string:
                couponType = 'CASH'
            else:
                couponType = 'COMBO'
            coupon.append(couponType)

        for price in tuanlist.find_all('div', class_='price'):
            original_price.append(float(price.string) * 100)

        for price in tuanlist.find_all('div', class_='o-price'):
            discount_price.append(float(price.string) * 100)

        for count in tuanlist.find_all('span', class_='soldNumNew'):
            sale_count.append(int(count.string[2:]) * 100)
        return coupon, original_price, discount_price, sale_count
    except:
        return coupon, original_price, discount_price, sale_count