# coding=utf-8
"""
采集武汉环保网各监测地空气质量数据，自动生成：武汉_start_date_end_date.csv 文件
"""
import codecs
import copy

import requests
from lxml import etree
import csv
import logging
import sys

import proxy_manager

reload(sys)
sys.setdefaultencoding('utf8')

url = "http://www.whepb.gov.cn/airSubair_water_lake_infoView.jspx?"

HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3",
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:42.0) Gecko/20100101 Firefox/42.0",
    "Host": "www.whepb.gov.cn",
    "Referer": "http://www.whepb.gov.cn/airSubair_water_lake_infoView.jspx?"
}

post_data = {
    "Dic": "",
    "cdateBeginDic": "",
    "cdateEndDic": "",
    "pageNo1": ""
}


def gen_location_info(location_file="locations.txt"):
    dic = {}

    with open(location_file) as f:
        for line in f.readlines():
            no = line.split('"')[1]
            location = line.split('>')[1].strip("</option")
            dic[location] = no
    return dic


def get_data_list(resp):
    content = resp.content
    root = etree.HTML(content)
    nodes_list = root.xpath('//tr')
    target_data = {}
    data_list = []
    for node in nodes_list:
        try:
            target_data['period_date'] = node.xpath('td[1]/text()')[0]
            target_data['location'] = node.xpath('td[2]/text()')[0]
            target_data['so2'] = node.xpath('td[3]/text()')[0]
            target_data['no2'] = node.xpath('td[4]/text()')[0]
            target_data['breath_dirty'] = node.xpath('td[5]/text()')[0]
            target_data['co'] = node.xpath('td[6]/text()')[0]
            target_data['o3'] = node.xpath('td[7]/text()')[0]
            target_data['fuck_things'] = node.xpath('td[8]/text()')[0]

            target_data['AQI'] = node.xpath('td[9]/text()')[0]
            target_data['main_polution'] = node.xpath('td[10]/text()')[0]
            target_data['AQI_level'] = node.xpath('td[11]/text()')[0]
            target_data['AQI_'] = node.xpath('td[12]/text()')[0]
            insert_data = copy.deepcopy(target_data)
            data_list.append(insert_data)
        except Exception as e:
            print e
            continue
    return data_list


def crawl(start_date, end_date, file_name):
    """

    :param city_name: 例如： 武汉
    :param start_date: 例如"2015-02-05 02:00:00"
    :param end_date:
    :return:
    """
    csvfile = file(file_name, 'ab')
    csvfile.write(codecs.BOM_UTF8)
    writer = csv.writer(csvfile)
    location_dic = gen_location_info()
    try:
        for key, value in location_dic.items():
            page = 1
            while True:
                post_data = {
                    "type": '0',
                    "Dic": value,
                    "cdateBeginDic": start_date,
                    "cdateEndDic": end_date,
                    "pageNo1": str(page)
                }

                logging.info("accessing location:{},start_date:{}, end_date:{}, page:{}".format(key, s_date, e_date, page))
                print "accessing location:{},start_date:{}, end_date:{}, page:{}".format(key, s_date, e_date, page)
                ip = proxy_manager.get_proxyip()
                proxy = {'http': "http://{}:{}".format(ip["host"].encode("UTF-8"), ip["port"])}
                resp = requests.post(url, proxies=proxy, data=post_data, headers=HEADERS)
                # resp = requests.post(url, params=params, headers=HEADERS)
                items = get_data_list(resp)
                if not items:
                    logging.info("no more pages")
                    break
                for item in items:
                    p_d = item.get('period_date') # 2/3/2015 18:0:0

                    loc = item.get('location', '')
                    so2 = item.get('so2', '')
                    no2 = item.get('no2', '')
                    b_d = item.get('breath_dirty', '')
                    co = item.get('co', '')
                    o3 = item.get('o3', '')
                    fuck_things = item.get('fuck_things', '')
                    aqi = item.get('AQI', '')
                    m_polu = item.get('main_polution', '')
                    aqi_l = item.get('AQI_level', '')
                    aqi_ = item.get('AQI_', '')

                    logging.info("write item:{} in to file:{}".format(item, file_name))
                    writer.writerow((p_d, loc, so2, no2, b_d, co, o3, fuck_things, aqi, m_polu, aqi_l, aqi_))
                page += 1
    except Exception as e:
        logging.error('unknow exception with: {}'.format(e))
    except (KeyboardInterrupt, SystemExit):
        print "interrupted"
        csvfile.close()
    finally:
        csvfile.close()


def init_csv(file_name, titles):
    csvfile = file(file_name, 'w')
    csvfile.write(codecs.BOM_UTF8)
    writer = csv.writer(csvfile)
    writer.writerow(titles)
    csvfile.close()

logger = logging.basicConfig(level=logging.INFO,
                                 format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                                 filename="wuhan_aq.log",
                                 filemode='a+')

if __name__ == "__main__":
    s_date = "2005/02/01"  # 开始日期
    e_date = "2017/07/01"  # 截至日期
    table_titles = ['日期', '监测单位', '二氧化硫', '二氧化氮', '一氧化氮', '可吸入颗粒物', '臭氧', '细颗粒物', '空气质量指数', '首要污染物', 'AQI指数级别', 'AQI指数类别']
    target_file = u"{}_{}_{}.csv".format("武汉市环保局空气质量日报", s_date[:4], e_date[:4])
    init_csv(target_file, table_titles)
    crawl(start_date=s_date, end_date=e_date, file_name=target_file)
