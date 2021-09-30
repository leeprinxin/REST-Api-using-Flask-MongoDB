import sys
from flask import Flask, Response #  pip install Flask                                           ※架設伺服器套件
from flask_restful import Resource, Api, request    #  pip install flask_restful       ※RESTful伺服器套件
import ibis # pip install ibis-framework[impala]                                                    ※讀取Hadoop Impala套件
import numpy as np  #                                                                                          ※pandas所依賴的套件
import pandas as pd #                                                                                           ※Dataframe套件
import json

class Util():
    """擴充功能"""
    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)

    def DF_to_JSON(self, DF):
        """將Pandas的DataFrame轉換成JSON格式
        Args:
            DF: DataFrame
        Return:
            JSON格式的Dictionary
        """
        result = {}
        for row in DF.values:
            item = {}
            for header, value in zip(DF.columns, row):
                item[header] = value
            result[len(result)] = item
        return result
        #return DF.to_json(orient="index", force_ascii=False)

    def ParseCondition(self, condition):
        return condition.split("=")

class ImpalaADAS(Resource):
    """Impala的API
    Args:
            Resource: Flask RESTFul API 網址的input
    """
    def __init__(self, *args, **kwargs):
        # 宣告擴充功能
        self.Util = Util()
        # Connecting to Impala
        self.hdfs = ibis.hdfs_connect(host="node1", port=50070)
        self.client = ibis.impala.connect(host="node2", port=21050, database="adas", hdfs_client=self.hdfs)
        return super().__init__(*args, **kwargs)

    def get(self, Table, Condition=None):
        """根據PID取得資料
        Args:
            Table: 欲取得資料的表格
            Condition[Option]: 搜尋條件, EX: PersionID=0, 此程式將會自動轉為: PersionID='0'
        Return:
            JSON格式的資料
        """
        try:   # 若目前資料庫無指定之Table, 產生的Exception
            table = self.client.table(Table) # 取得該Table
        except:
            return "<Response [500]> No such the table"
        data = table.execute()  # 取得Table所有資料
        if(Condition != None): # 根據Condition取得資料
            Condition = self.Util.ParseCondition(Condition)   # 取得Condition
            data = data.loc[data[Condition[0]] == Condition[1]]
        data = data.to_dict(orient="index")
        return Response(json.dumps(data), mimetype='application/json')

    def post(self, Table, Condition=None):
        """直接新增資料, 若無該表格則新增表格
        Args:
            Table: 欲取得資料的表格
            #Condition: 目前無提供相應功能
        Return:
            <Response [200]> or <Response [500]>
        """
        # Condition若有用到再設定
        if(Condition != None):  pass        
        data = request.get_json()   # 取得Post的Json資料, 正確data格式: {"[Index]": {"Key": "Value"}}, 實際上Type為dict
        try:    # 若取得的Json格式錯誤, 產生的Exception
            DF = pd.DataFrame.from_dict(data, orient="index")   # 轉為DataFrame
        except:
            return "<Response [500]> Invalid json format"
        # 插入資料, SQL語法(沒使用): "INSERT INTO `member` (\"" + ('\",\"').join(data.keys()) + "\") VALUES (\"" + ('\",\"').join(data.values()) + "\")"
        try:    # 若目前資料庫無指定之Table, 產生的Exception
            table = self.client.table(Table)
        except:
            self.client.create_table(Table, DF) # 創建Table並將資料放入
            return "<Response [200]> Create table and insert complete"    # 沒有創建Table成功, 會回傳: <Response [500]>
        try:    # 若傳入的Json與現有資料庫格式不符, 產生的Exception
            table.insert(DF)
            return "<Response [200]> Insert complete"    # 沒有insert成功, 會回傳: <Response [500]>
        except:
            return "<Response [500]> The schema of Json file you post didn't match with the existing table"    # 資料與目前資料庫不符

    def put(self, Table, Condition=None):
        """更新資料
        Args:
            Table: 欲取得資料的表格
            Condition[Option]: 更新條件, EX: PersionID=0, 此程式將會自動轉為: PersionID='0'
        Return:
            <Response [200]> or <Response [500]>
        """        
        if(Condition == None):  # 一定要給定Condition才可以更改
            return "<Response [500]> No [Condition], couldn't  updata the data"
        data = request.get_json()   # 取得Post的資料, 並轉為DataFrame
        try:    # 若取得的Json格式錯誤, 產生的Exception
            DF = pd.DataFrame.from_dict(data, orient="index")   # 正確data格式: {"[Index]": {"Key": "Value"}}
        except:
            return "<Response [500]> Invalid json format"
        try:    # 若目前資料庫無指定之Table, 產生的Exception
            table = self.client.table(Table)
        except:
            return "<Response [500]> No such the table"
        data = table.execute()  # 取得Table資料
        Condition = self.Util.ParseCondition(Condition)   # 取得Condition
        try:    # 若傳入的Json與現有資料庫格式不符, 產生的Exception
            data.loc[data[Condition[0]] == Condition[1], DF.columns.values] = DF.values  # 更新資料到Dataframe, loc[符合條件的Row, 預計挑出的Column]
        except:
            return "<Response [500]> The column name of Json file you put didn't match with the existing table"    # 資料與目前資料庫不符
        try:    # 若有備份, 把之前的備份刪除
            table_backup = self.client.table(Table + "_backup")
            table_backup.drop()
        except: pass
        finally:    # 將原本表格重新命名, 備份, 也就等於刪除原本表格
            table.rename(Table + "_backup")    # 重新命名原本表格, 用來備份
        self.client.create_table(Table, data)   # 將更新後的資料放進資料庫
        return "<Response [200]> Update complete"    # 沒有insert成功, 會回傳: <Response [500]>

    def delete(self, Table, Condition=None):
        """根據Condition刪除資料
        Args:
            Table: 欲取得資料的表格
            Condition[Option]: 搜尋條件, EX: PersionID=0, 此程式將會自動轉為: PersionID='0'
        Return:
            <Response [200]> or <Response [500]>
        """
        try:    # 避免沒有該Table
            table = self.client.table(Table) # 取得該Table, 若無Table則觸發Except
        except :
            return "<Response [500]> Delete fail, (1)the table doesn't exist! or (2)The column name didn't match with the existing table"
        if(Condition != None):  # 當有條件時, 刪除表格內資料
            data = table.execute()    # 取得Table資料
            Condition = self.Util.ParseCondition(Condition)   # 取得Condition                
            data = data.loc[data[Condition[0]] != Condition[1]] # 取得Database資料, 並轉為DataFrame
        try:    # 若有備份, 把之前的備份刪除
            table_backup = self.client.table(Table + "_backup")
            table_backup.drop()
        except: pass    # 若沒有備份, Do nothing
        finally:    # 將原本表格重新命名, 備份, 也就等於刪除原本表格
            table.rename(Table + "_backup")    # 重新命名原本表格, 用來備份
        if(Condition != None):
            self.client.create_table(Table, data)   # 創建Table並將資料放入
        return "<Response [200]> Delete complete"
            
class RESTful_Service(object):
    """Flask實作RESTful Service"""
    def __init__(self, *args, **kwargs):
        return super().__init__(*args, **kwargs)
            
    def Run(self):
        self.app = Flask(__name__)
        self.api = Api(self.app)
        self.api.add_resource(ImpalaADAS, "/adas/<string:Table>/<string:Condition>", "/adas/<string:Table>", endpoint = "adas")
        self.app.config['JSON_AS_ASCII'] = False
        self.app.run(host="127.0.0.1", port=9999, debug=False)

def main(argv):
    RESTful = RESTful_Service()
    RESTful.Run()

if __name__ == "__main__":
    main(sys.argv[1:])
