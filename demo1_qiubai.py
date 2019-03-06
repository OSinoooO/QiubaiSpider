# coding=utf-8
import json
import requests
import time
from lxml import etree
from retrying import retry


class QiuBaiSpider(object):
    """糗事百科爬虫"""
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
        }

        with open('qiubai.json', 'a') as f:
            f.write('[')

    def get_url_list(self):  # 构造url列表
        page_num = 1
        base_url = 'https://www.qiushibaike.com/8hr/page/{}/'
        try:
            response = requests.get(base_url.format(page_num), headers=self.headers, timeout=10)
        except Exception as e:
            raise e
        else:
            html = etree.HTML(response.text)
            pages = int(html.xpath('//span[@class="page-numbers"]/text()')[-1].strip())
            url_list = [base_url.format(i) for i in range(1, pages + 1)]
            return url_list

    @retry(stop_max_attempt_number=3)
    def parse_url(self, url):  # 发送请求，获取响应
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            print(response.url, response.status_code)
        except Exception as e:
            raise e
        else:
            return response.content.decode('utf-8', 'ignore')

    def parse_item(self, response):
        item = {}
        html = etree.HTML(response)
        ret_list = html.xpath('//li[starts-with(@id, "qiushi_tag_")]')
        for ret in ret_list:
            # 详情页
            href = ret.xpath('./a[contains(@class, "recmd-left")]/@href')
            item['href'] = 'https://www.qiushibaike.com' + href[0] if len(href) else None
            # 标题图片
            title_img = ret.xpath('./a[contains(@class, "recmd-left")]/img/@src')
            item['title_img'] = 'https:' + title_img[0] if len(title_img) else None
            # 标题
            title = ret.xpath('.//a[@class="recmd-content"]/text()')
            item['title'] = title[0].strip() if len(title) else None
            # 点赞数
            zan_num = ret.xpath('.//div[@class="recmd-num"]/span[position()=1]/text()')
            item['zan_num'] = zan_num[0] if len(zan_num) else None
            # 回复数
            reply_num = ret.xpath('.//div[@class="recmd-num"]/span[position()=4]/text()')
            item['reply_num'] = reply_num[0] if len(reply_num) else None
            # 昵称
            nickname = ret.xpath('.//span[@class="recmd-name"]/text()')
            item['nickname'] = nickname[0].strip() if len(nickname) else None
            # 头像
            avatar = ret.xpath('//a[@class="recmd-user"]/img/@src')
            item['avatar'] = 'https:' + avatar[0] if len(avatar) else None
            yield item

    def save_item(self, rets):  # 保存数据
        with open('qiubai.json', 'a', encoding='utf-8') as f:
            for ret in rets:
                f.write(json.dumps(ret, ensure_ascii=False) + ',\n')

    def run(self):
        # 构造url列表
        url_list = self.get_url_list()
        # 发送请求，获取响应
        for url in url_list:
            response = self.parse_url(url)
            # 提取数据
            rets = self.parse_item(response)
            # 保存数据
            self.save_item(rets)

        with open('qiubai.json', 'a') as f:
            f.write(']')


if __name__ == '__main__':
    ti = time.time()
    qiubai = QiuBaiSpider()
    qiubai.run()
    print('total_cost', time.time() - ti)