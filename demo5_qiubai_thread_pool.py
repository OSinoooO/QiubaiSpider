# coding=utf-8
import json
from multiprocessing.dummy import Pool
import requests
import time
from lxml import etree
from retrying import retry
from queue import Queue


class QiuBaiSpider(object):
    """糗事百科爬虫"""
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
        }
        self.url_queue = Queue()
        self.pool = Pool(5)
        self.is_running = True
        self.total_request_num = 0
        self.total_response_num = 0

        with open('qiubai.json', 'a') as f:
            f.write('[')

    def get_url_list(self):  # 构造url队列
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
            for url in url_list:
                self.url_queue.put(url)
                self.total_request_num += 1

    @retry(stop_max_attempt_number=3)
    def parse_url(self):  # 发送请求，获取响应
        try:
            url = self.url_queue.get()
            response = requests.get(url, headers=self.headers, timeout=10)
            print(response.url, response.status_code)
        except Exception as e:
            raise e
        else:
            self.url_queue.task_done()
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

    def _execete_request_content_item(self):  # 进行一次url地址提取数据和包村
        response = self.parse_url()
        # 提取数据
        rets = self.parse_item(response)
        # 保存数据
        self.save_item(rets)
        self.total_response_num += 1

    def _callback(self, temp):  # 递归调用
        if self.is_running:
            self.pool.apply_async(self._execete_request_content_item, callback=self._callback)

    def run(self):
        # 构造url队列
        self.get_url_list()
        # 递归调用
        for i in range(5):  # 设置并发数为5
            self.pool.apply_async(self._execete_request_content_item, callback=self._callback)
        # 阻止主线程结束
        while self.total_response_num < self.total_request_num:
            time.sleep(0.0001)

        with open('qiubai.json', 'a') as f:
            f.write(']')


if __name__ == '__main__':
    ti = time.time()
    qiubai = QiuBaiSpider()
    qiubai.run()
    print('total_cost', time.time() - ti)