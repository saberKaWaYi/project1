import os
import logging
from logging.handlers import RotatingFileHandler

log_dir="logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        RotatingFileHandler(
            os.path.join(log_dir,"app.log"),
            maxBytes=1024*1024*1024,
            backupCount=5
        ),
        logging.StreamHandler()
    ]
)

import atexit
from pymongo import MongoClient
import time
import pandas as pd
    
class Connect_Mongodb:

    def __init__(self):
        self.config={
            "connection":{
                "TIMES":1000,
                "TIME":0.1
            },
            "mongodb":{
                "HOST":"10.216.141.46",
                "PORT":27017,
                "USERNAME":"manager",
                "PASSWORD":"cds-cloud@2017"
            }
        }
        self.config={
            "connection":{
                "TIMES":3,
                "TIME":0.1
            },
            "mongodb":{
                "HOST":"localhost",
                "PORT":4000,
                "USERNAME":"manager",
                "PASSWORD":"cds-cloud@2017"
            }
        }
        self.client=self.login()
        self.db=self.get_database()
        atexit.register(self.close)

    def login(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                client=MongoClient(host=self.config["mongodb"]["HOST"],port=self.config["mongodb"]["PORT"])
                client.cds_cmdb.authenticate(self.config["mongodb"]["USERNAME"],self.config["mongodb"]["PASSWORD"])
                return client
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mongodb登录失败。")
        raise Exception("mongodb登录失败。")
    
    def get_database(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                return self.client.get_database("cds_cmdb")
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("cds_cmdb获取失败。")
        raise Exception("cds_cmdb获取失败。")
    
    def close(self):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                self.client.close()
                return
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error("mongodb关闭失败。")
        raise Exception("mongodb关闭失败。")

    def get_collection(self,name,condition1,condition2):
        for i in range(self.config["connection"]["TIMES"]):
            try:
                data=pd.DataFrame(self.db.get_collection(name).find(condition1,condition2)).astype(str)
                return data
            except:
                time.sleep(self.config["connection"]["TIME"])
        logging.error(f"{name}数据获取失败。")
        raise Exception(f"{name}数据获取失败。")

from rest_framework.decorators import api_view
from rest_framework.response import Response

@api_view(['GET'])
def test(request):
    return Response("Hello world")

from bson import ObjectId

@api_view(['GET'])
def get_info(request):
    zd={}
    zd["code"]=200;zd["message"]="";zd["data"]={}
    db_mongo=Connect_Mongodb()
    data_center=db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":{"$ne":ObjectId("60f66712e61a21f5aafd564a")}},{"_id":1,"data_center_name":1})
    data_center_zd=dict(zip(data_center["_id"].values.tolist(),data_center["data_center_name"].values.tolist()))
    room=db_mongo.get_collection("cds_ci_att_value_room",{"status":1},{"_id":1,"room_name":1})
    room_zd=dict(zip(room["_id"].values.tolist(),room["room_name"].values.tolist()))
    relationship=db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"room"},{"data_center_id":1,"room_id":1})[["data_center_id","room_id"]].values.tolist()
    for i in relationship:
        if not data_center_zd.get(i[0],None):
            continue
        if not room_zd.get(i[1],None):
            continue
        if data_center_zd[i[0]] not in zd["data"]:
            zd["data"][data_center_zd[i[0]]]={}
        if room_zd[i[1]] not in zd["data"][data_center_zd[i[0]]]:
            zd["data"][data_center_zd[i[0]]][room_zd[i[1]]]={}
    rack=db_mongo.get_collection("cds_ci_att_value_rack",{"status":1},{"_id":1,"rack_name":1})
    rack_zd=dict(zip(rack["_id"].values.tolist(),rack["rack_name"].values.tolist()))
    relationship=db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"rack"},{"data_center_id":1,"room_id":1,"rack_id":1})[["data_center_id","room_id","rack_id"]].values.tolist()
    for i in relationship:
        if not data_center_zd.get(i[0],None):
            continue
        if not room_zd.get(i[1],None):
            continue
        if not rack_zd.get(i[2],None):
            continue
        if rack_zd[i[2]] not in zd["data"][data_center_zd[i[0]]][room_zd[i[1]]]:
             zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]]={}
    return Response(zd)