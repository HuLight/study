#   @author:HuKai
#   @project:merge_tool
#   @file:xmlsql3.py
#   @ide:PyCharm
#   @time:2019-10-31 16:17
import re

from develop.link.version_2.content_sql import MySql

try:
    import xml.etree.cElementTree as ET
except:
    import xml.etree.ElementTree as ET

from concurrent.futures import ThreadPoolExecutor, as_completed


class GenetateData(object):
    def __init__(self):

        self.mysql = MySql()
        self.path = "D:\\project\\merge_tool\\test\\link\\huawei3g_1.xml"

    def calculate(self):
        """
        分析数据
        :return: 
        """
        try:
            tree = ET.parse(self.path)
        except Exception as e:
            print("数据非法字符，函数:calculate:error-%s" % e)
            newpath = self.duqu()
            tree = ET.parse(newpath)
        # 根节点
        root = tree.getroot()
        className = [attr.get("className") for attr in root.iter("MO")]  # classname 即数数据库表名称
        datas = []  # 第二级数据 TYPE --[{"classname":[]},]
        data = {key: [] for key in className}  # 第二层创建表数据 type{classname:[attr1,attr2]}
        result = [[], []]  # 第一级数据 字段名称、表名、数据
        nameneid = []  # 数据name、neid
        for child in root:  # 根
            for index, children in enumerate(child):  # 第二层，classname、。。。
                attr = children.attrib  # 属性
                if "className" in children.attrib.keys():  # 长度有两层,只有属性是className属性的进行循环
                    classname = attr["className"]
                    datas.append({classname: []})
                    data_len = len(datas) - 1
                    for childrens in children:
                        # 准备数据
                        key = childrens.attrib['name']
                        val = childrens.text
                        datas[data_len].get(classname).append({key: val})
                        datalist = data[classname]
                        if key in datalist:
                            pass
                        else:
                            datalist.append(key)
                else:
                    name = attr["name"]
                    if name == "className":
                        result.append(children.text)
                    result[0].append(name)
                    result[1].append(children.text)
                    if name == "name" or name == "neID":
                        nameneid.append({name: children.text})
        # 组合数据
        data[result[2]] = result[0]
        return data, datas, className, result, nameneid

    def create_table(self, data, className):
        """
        数据库表名查重，xml中每个classname字段数量不一定相同
        :return: 
        """
        return_data = self.duibi(className)
        values = [{key: tuple(value)} for key, value in data.items()]
        # 第一层和第二层表结构整合
        if return_data is None:  # 创建全部的表
            for val in values:
                keyvalue = tuple(val.items())[0]
                classname = keyvalue[0]
                sql = "create table {0}(id int unsigned primary key auto_increment not null," \
                      "name_root varchar(255) default null," \
                      "neid_root varchar(255) default null,".format(classname)
                length = len(keyvalue[1]) - 1
                for index, i in enumerate(keyvalue[1]):
                    if index < length:
                        sql += "%s varchar(255) default null," % i
                    else:
                        sql += "%s varchar(255) default null);" % i
                        # 创建列表
                self.mysql.update(sql)
        elif type(return_data) is list:  # 创建一部分数据
            for val_ in values:
                keyvalue = tuple(val_.items())[0]
                classname = keyvalue[0]
                sql = "create table {0}(id int unsigned primary key auto_increment not null," \
                      "name_root varchar(255) default null," \
                      "neid_root varchar(255) default null,".format(classname)
                if classname in return_data:
                    length = len(keyvalue[1]) - 1
                    for index, i in enumerate(keyvalue[1]):
                        if index < length:
                            sql += "%s varchar(255) default null," % i
                        else:
                            sql += "%s varchar(255) default null);" % i
                    self.mysql.update(sql)
                else:
                    pass
        else:
            pass

    def duibi(self, classname):
        """
        :param classname: xml所有的classname去重后的名称，也是即将创建数据库的表名
        :return: 

        """
        sql = "show tables;"
        # 数据库表
        tables = self.mysql.get_data(sql)
        tablesname = [table[0].upper() for table in tables]
        tablesname_set = set(tablesname)
        classname_set = set(classname)
        # 判断是否相等
        intersection = tablesname_set & classname_set
        if len(intersection) == 0:  # 交集为零 全部需要创建表
            return
        else:  # 交集不为零
            insertname = list(classname_set - intersection)
            if len(insertname) == 0:
                return {"status": 20002}
            else:
                return insertname

    def insertdata(self, datas, nameneid):

        for dts in datas:
            keyvalue = tuple(dts.items())[0]
            tablename = keyvalue[0]
            attrname = keyvalue[1]
            ziduan = "name_root,neid_root,"
            zhi = "\"{0}\",\"{1}\",".format(nameneid[0]["name"], nameneid[1]["neID"])
            length = len(attrname) - 1
            for index, attr in enumerate(attrname):
                keyvalues = tuple(attr.items())[0]
                if index < length:
                    ziduan += keyvalues[0] + ","

                    zhi += "\"%s\"" % keyvalues[1] + ","
                else:
                    ziduan += keyvalues[0]
                    zhi += keyvalues[1]
            sql = "insert into {0} ({1}) values({2});".format(tablename, ziduan, zhi)
            self.mysql.update(sql)

    def chaifen(self, value, nameneid):

        length = len(value)
        if length > 30:
            for val in range(0, length, 30):
                values = value[val:val + 30]
                self.insertdata(values, nameneid)
        else:
            self.insertdata(value, nameneid)

    # def get_file(self):
    #     import os
    #     path = "D:\\project\\merge_tool\\develop\\link\\docment"
    #     filename_ = os.listdir(path)
    #     with ThreadPoolExecutor(max_workers=3) as executor:
    #         all_task = [executor.submit(self.main, path + "\\" + name) for name in filename_]
    #         for future in as_completed(all_task):
    #             future = future.result()

    def duqu(self):
        """
        读物文件写如文件下级的 -- jiexifile 中
        :return: new文件的路径
        """
        import os
        regular = "([^<>/\\\|:""\*\?]+)\.\w+$"
        filename = re.search(regular,self.path).group()
        path = self.path.replace(filename,'')
        quhouzui = filename.replace('.xml','')
        print("文件名称:%s"%quhouzui)
        newfilename = path + "jiexifile\\" + quhouzui + "-jiexi.xml"
        try:
            os.remove(newfilename)
        except Exception as e:
            print("删除文件")

        with open(self.path, 'r', encoding='GBK') as f:
            while True:
                rdata = f.read(1024 * 1024)
                tihuandata = self.tihua(rdata)
                values = tihuandata.replace('GBK', 'utf-8')
                if not rdata:
                    f.close()
                    print(rdata)
                    break
                with open(newfilename, 'a', encoding='utf-8') as wf:
                    wf.write(values)
                    wf.close()
            return newfilename


    def tihua(self, tihuan):
        #替换非法字符
        regular = "[\\x00-\\x08\\x0b-\\x0c\\x0e-\\x1f]"
        values = re.sub(regular, "非法字符", tihuan)
        return values

    def main(self):
        #入口函数
        """
        0>xml数据乱码替换
        1>表名查重
        2>创建数据库表
        3>导入数据
        """
        data, datas, className, result, nameneid = self.calculate()  # 分析数据
        self.create_table(data, className)  # 创建数据库表
        self.chaifen(datas, nameneid)
        # 断开数据库连接
        return True


if __name__ == '__main__':
    obj = GenetateData()
    obj.main()
