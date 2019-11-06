#   @author:HuKai
#   @project:merge_tool
#   @file:ip_duibi.py
#   @ide:PyCharm
#   @time:2019-10-23 15:07
import json
import re
import requests
import sched
from study.tk_test import Gui

class IpDuiBi(object):

    def __init__(self):
        self.url = "http://pv.sohu.com/cityjson"
        self.res = "{.*?}"
        self.sch = sched.scheduler()

    def task(self):
        try:
            response = requests.get(self.url)
        except Exception as e:
            return
        val = response.content.decode(encoding="GBK")
        # var returnCitySN = {"cip": "202.110.105.124", "cid": "410100", "cname": "河南省郑州市"};
        re_val = re.compile(self.res, re.S).findall(val)[0]
        dict = json.loads(re_val)
        ip = dict["cip"]
        city = dict["cname"]
        print("本机的公网ip:{0},城市是:{1}".format(ip, city))
        return ip, city

    def perform(self):

        self.sch.enter(30,0,self.func,)
        try:
            ip, city = self.task()
        except Exception as e:
            return
        G = Gui(ip,city)
        G.main()

    def func(self):
        self.sch.enter(0, 1, self.perform, )
        self.sch.run()

if __name__ == '__main__':
    obj = IpDuiBi()
    obj.func()