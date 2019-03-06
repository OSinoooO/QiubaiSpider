# coding=utf-8
import json
from queue import Queue
import requests
import time
from lxml import etree
import threading


class QiuBaiSpider(object):
    """糗事百科爬虫"""
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.67 Safari/537.36'
        }
        self.url_queue = Queue()
        self.resp_queue = Queue()
        self.item_queue = Queue()

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
            for url in url_list:
                self.url_queue.put(url)

    def parse_url(self):  # 发送请求，获取响应
        while True:
            try:
                url = self.url_queue.get()
                response = requests.get(url, headers=self.headers, timeout=10)
                print(response.url, response.status_code)
                if response.status_code != 200:
                    self.url_queue.put(response.url)
            except Exception as e:
                raise e
            else:
                self.resp_queue.put(response.content.decode('utf-8', 'ignore'))
                self.url_queue.task_done()  # 计数-1

    def parse_item(self):
        while True:
            item = {}
            response = self.resp_queue.get()
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
                self.item_queue.put(item)
            self.resp_queue.task_done()

    def save_item(self):  # 保存数据
        with open('qiubai.json', 'a', encoding='utf-8') as f:
            f.write('[')
            while True:
                ret = self.item_queue.get()
                f.write(json.dumps(ret, ensure_ascii=False) + ',\n')
                self.item_queue.task_done()

    def run(self):
        thread_list = []
        # 构造url列表
        t_url_list = threading.Thread(target=self.get_url_list)
        t_url_list.setDaemon(True)
        t_url_list.start()
        t_url_list.join()

        # 发送请求，获取响应
        for i in range(3):
            ti_parse_url = threading.Thread(target=self.parse_url)
            thread_list.append(ti_parse_url)
        # 提取数据
        ti_parse_item = threading.Thread(target=self.parse_item)
        thread_list.append(ti_parse_item)
        # 保存数据
        ti_save_item = threading.Thread(target=self.save_item)
        thread_list.append(ti_save_item)

        for t in thread_list:
            t.setDaemon(True)  # 设置守护线程
            t.start()

        for q in [self.url_queue, self.resp_queue, self.item_queue]:
            q.join()  # 让主线程阻塞，队列没释放之前不能结束任务

        with open('qiubai.json', 'a') as f:
            f.write(']')


if __name__ == '__main__':
    ti = time.time()
    qiubai = QiuBaiSpider()
    qiubai.run()
    print('total_cost', time.time() - ti)
