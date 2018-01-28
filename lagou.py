# -*-coding:utf-8-*-

import requests
import json
import sys
import time
import uuid
import threading
import Queue
import mysql.connector


'''
    拉勾网多线程爬虫程序
    多线程代理并存入mysql数据库
    2018.1.28
'''


# 获得全局锁对象
lock = threading.Lock()


class MyThread (threading.Thread):

    """
        爬取线程处理类
    """

    # 构造函数负责初始化
    def __init__(self, q, proxy, url, conn):
        threading.Thread.__init__(self)
        self.q = q
        self.proxy = proxy
        self.url = url
        self.conn = conn

    # 线程运行函数
    def run(self):

        # 页数队列不为空就继续执行线程
        while not self.q.empty():

            try:
                # 获取页数
                pn = self.q.get()

                # 生成动态uuid
                uid = uuid.uuid1()

                # 设置请求头，利用实时生成的uuid伪造cookie
                headers = {
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Accept-Language": "zh-CN,zh;q=0.9",
                    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.84 Safari/537.36",
                    "X-Requested-With": "XMLHttpRequest",
                    "Connection": "keep-alive",
                    "Origin": "https://www.lagou.com",
                    "Host": "www.lagou.com",
                    "Referer": "https://www.lagou.com/jobs/list_",
                    "X-Anit-Forge-Code": "0",
                    "X-Anit-Forge-Token": "None",
                    "Cookie": "_ga=GA1.2.753392604.1516803640; user_trace_token=201801" + str(uid) + "; LGUID=201801" + str(uid) + "-1d03a66c-0112-11e8-ab93-5254005c3644; " +
                              "index_location_city=%E5%85%A8%E5%9B%BD; _gid=GA1.2.2037548825.1516968188; X_HTTP_TOKEN=c32c81c0809445229ec3d5b42f331c8a; JSESSIONID=ABAAABAACEBACDGA011945EB2093D529F4A119C2E2FBD54; _gat=1; " +
                              "LGSID=201801" + str(uid) + "; PRE_UTM=; PRE_HOST=www.baidu.com; PRE_SITE=https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3DJmfdC2dOCdNhHd6kwlPwZMvSz6MnICIsiC9NG3T02Ya%26wd%3D%26eqid%3Db6e77cef00039841000000035a6b57d2; " +
                              "PRE_LAND=https%3A%2F%2Fwww.lagou.com%2F; Hm_lvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1516803640,1516968188,1516984127; TG-TRACK-CODE=index_navigation; Hm_lpvt_4233e74dff0ae5bd0a3d81c6ccf756e6=1516984141; " +
                              "LGRID=201801" + str(uid) + "; SEARCH_ID=7bb3b9a282b449d1aca062b7deddb5e4"
                }

                # first设置为false，pn表示第几页，kd表示搜索关键词
                post_param = {"first": "true", "pn": pn, "kd": "java"}

                # 使用post方式，data里存放参数
                r = requests.post(self.url, data=post_param, headers=headers, proxies={"http": self.proxy})

                # json.dumps()方法要禁用ascii编码输出
                result = json.loads(r.text)

                with open(r'C:\Users\Administrator\Desktop\lagou.txt', 'a+') as f:
                    # 数据整理格式化
                    for num1 in range(15):
                        message = "职位名称：" + result["content"]["positionResult"]["result"][num1]["positionName"] + "  " + "公司简称：" + result["content"]["positionResult"]["result"][num1]["companyShortName"] + "  " + \
                                  "薪资：" + result["content"]["positionResult"]["result"][num1]["salary"] + "  " + "所在城市：" + result["content"]["positionResult"]["result"][num1]["city"] + "  " + \
                                  "经验要求：" + result["content"]["positionResult"]["result"][num1]["workYear"] + "  " + "学历要求：" + result["content"]["positionResult"]["result"][num1]["education"] + "  " + \
                                  "公司规模：" + result["content"]["positionResult"]["result"][num1]["companySize"] + "  " + "代理名称：" + self.getName() +"\n"
                        # 终端实时打印爬取结果
                        print(message)
                        f.write(message)

                # 将数据存入数据库
                try:
                    # 获取锁
                    lock.acquire()
                    # 获取光标对象
                    cursor = self.conn.cursor()

                    # 格式化sql语句
                    for num in range(15):
                        message = [result["content"]["positionResult"]["result"][num]["positionName"], result["content"]["positionResult"]["result"][num]["salary"],
                                   result["content"]["positionResult"]["result"][num]["jobNature"], result["content"]["positionResult"]["result"][num]["city"],
                                   result["content"]["positionResult"]["result"][num]["workYear"], result["content"]["positionResult"]["result"][num]["education"],
                                   result["content"]["positionResult"]["result"][num]["firstType"], result["content"]["positionResult"]["result"][num]["companyShortName"],
                                   result["content"]["positionResult"]["result"][num]["companySize"], result["content"]["positionResult"]["result"][num]["financeStage"]]
                        # 插入数据
                        cursor.execute('insert into java (positionName, salary, jobNature, city, workYear, education, firstType, companyShortName, companySize, financeStage) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', message)
                        self.conn.commit()

                    # 游标对象关闭连接
                    cursor.close()

                except:
                    pass

                finally:
                    # 释放锁:
                    lock.release()

                # 延时1秒避免ip被封锁
                time.sleep(1)

            # 取不出数据后进入下次爬取
            except IndexError:
                self.q.put(pn)
                continue

            except TypeError:
                # self.q.put(pn)
                # continue
                pass

            except ValueError:
                self.q.put(pn)
                continue


def main():

    # 以utf-8为默认处理字符集（python2默认ascii）
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # 代理列表
    proxies = ["http://122.114.31.177:808", "http://61.135.217.7:80", "http://114.228.26.2:8118", "http://218.77.226.143:8118", "http://122.225.17.123:8080"]

    # 设置爬取url地址
    ajax_url = "https://www.lagou.com/jobs/positionAjax.json"

    # 获取数据库连接对象
    conn = mysql.connector.connect(user='root', password='000000', database='lagou')
    # 获取光标对象
    cursor = conn.cursor()
    # 建表
    cursor.execute('create table java (positionName varchar(40),salary varchar(20),  jobNature varchar(20), city varchar(20), workYear varchar(20),education varchar(20), '
                   'firstType varchar(20), companyShortName varchar(40), companySize varchar(20), financeStage varchar(20))')
    # 提交
    conn.commit()

    # 定义数据页码爬取队列
    pn_queue = Queue.Queue()
    for page in range(1, 327):
        pn_queue.put(page)

    # 创建多个不同代理线程
    thread1 = MyThread(pn_queue, proxies[0], ajax_url, conn)
    thread2 = MyThread(pn_queue, proxies[1], ajax_url, conn)
    thread3 = MyThread(pn_queue, proxies[2], ajax_url, conn)
    thread4 = MyThread(pn_queue, proxies[3], ajax_url, conn)
    thread5 = MyThread(pn_queue, proxies[4], ajax_url, conn)

    # 运行多个线程
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread5.start()

    # 等待所有线程完成
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    thread5.join()

    # 关闭数据库连接
    conn.close()


# 程序入口
if __name__ == '__main__':
    main()
