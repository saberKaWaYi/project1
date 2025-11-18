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
        # self.config={
        #     "connection":{
        #         "TIMES":1000,
        #         "TIME":0.1
        #     },
        #     "mongodb":{
        #         "HOST":"localhost",
        #         "PORT":4000,
        #         "USERNAME":"manager",
        #         "PASSWORD":"cds-cloud@2017"
        #     }
        # }
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

def tool1(s):
    if "\'" in s:
        s=s[s.index("\'")+1:]
    if "\'" in s:
        s=s[:s.index("\'")]
    return s

from bson import ObjectId

def get_relationship(flag=False):
    zd={}
    zd["code"]=200;zd["message"]="";zd["data"]={}
    db_mongo=Connect_Mongodb()
    dict_detail=db_mongo.get_collection("cds_dict_detail",{"status":1},{"_id":1,"field_name":1})
    dict_detail=dict(zip(dict_detail["_id"].values.tolist(),dict_detail["field_name"].values.tolist()))
    data_center=db_mongo.get_collection("cds_ci_att_value_data_center",{"status":1,"data_center_status":{"$ne":ObjectId("60f66712e61a21f5aafd564a")}},{"_id":1,"data_center_name":1,"code":1})
    room=db_mongo.get_collection("cds_ci_att_value_room",{"status":1},{"_id":1,"room_name":1,"code":1})
    if flag:
        data_center_zd=dict(zip(data_center["_id"].values.tolist(),[i[0]+"|"+i[1] for i in data_center[["data_center_name","code"]].values.tolist()]))
        room_zd=dict(zip(room["_id"].values.tolist(),[i[0]+"|"+i[1] for i in room[["room_name","code"]].values.tolist()]))
    else:
        data_center_zd=dict(zip(data_center["_id"].values.tolist(),data_center["data_center_name"].values.tolist()))
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
    pipeline=[
        {
            '$match':{
                'status':1,
                'asset_status':{
                    '$in':[
                        ObjectId("5f964e31df0dfd65aaa716ec"),
                        ObjectId("5fcef6de94103c791bc2a471"),
                        ObjectId("5f964e424b328c52c8888d45"),
                    ]
                }
            }
        },
        {
            '$lookup':{
                'from':'cds_ci_location_detail',
                'localField':'_id',
                'foreignField':'device_id',
                'as':'location'
            }
        },
        {
            '$match':{
                'location.status':1
            }
        },
        {
            '$project':{
                "_id":1,
                "hostname":1,
                "asset_status":1
            }
        }
    ]
    network=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_network.aggregate(pipeline))).astype(str)
    network_zd=dict(zip(network["_id"].values.tolist(),network[["hostname","asset_status"]].values.tolist()))
    relationship=db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"network"},{"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["data_center_id","room_id","rack_id","device_id"]].values.tolist()
    for i in relationship:
        if not data_center_zd.get(i[0],None):
            continue
        if not room_zd.get(i[1],None):
            continue
        if not rack_zd.get(i[2],None):
            continue
        if not network_zd.get(i[3],None):
            continue
        if network_zd[i[3]][0] not in zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]]:
            zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]][network_zd[i[3]][0]]={"type":"network","asset_status":dict_detail.get(network_zd[i[3]][1],None)}
    server=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_server.aggregate(pipeline))).astype(str)
    server_zd=dict(zip(server["_id"].values.tolist(),server[["hostname","asset_status"]].values.tolist()))
    relationship=db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"server"},{"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["data_center_id","room_id","rack_id","device_id"]].values.tolist()
    for i in relationship:
        if not data_center_zd.get(i[0],None):
            continue
        if not room_zd.get(i[1],None):
            continue
        if not rack_zd.get(i[2],None):
            continue
        if not server_zd.get(i[3],None):
            continue
        if server_zd[i[3]][0] not in zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]]:
            zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]][server_zd[i[3]][0]]={"type":"network","asset_status":dict_detail.get(server_zd[i[3]][1],None)}
    storage=pd.DataFrame(list(db_mongo.db.cds_ci_att_value_storage.aggregate(pipeline))).astype(str)
    storage_zd=dict(zip(storage["_id"].values.tolist(),storage[["hostname","asset_status"]].values.tolist()))
    relationship=db_mongo.get_collection("cds_ci_location_detail",{"status":1,"ci_name":"storage"},{"data_center_id":1,"room_id":1,"rack_id":1,"device_id":1})[["data_center_id","room_id","rack_id","device_id"]].values.tolist()
    for i in relationship:
        if not data_center_zd.get(i[0],None):
            continue
        if not room_zd.get(i[1],None):
            continue
        if not rack_zd.get(i[2],None):
            continue
        if not storage_zd.get(i[3],None):
            continue
        if storage_zd[i[3]][0] not in zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]]:
            zd["data"][data_center_zd[i[0]]][room_zd[i[1]]][rack_zd[i[2]]][storage_zd[i[3]][0]]={"type":"network","asset_status":dict_detail.get(storage_zd[i[3]][1],None)}
    return zd

def transform_format(zd,iter_):
    iter_+=1
    lt=[]
    for i in zd:
        type_={1:"data_center",2:"room",3:"rack"}.get(iter_)
        if iter_<3:
            lt.append({"name":i.split("|")[0],"children":transform_format(zd[i],iter_),"type":type_,"code":i.split("|")[1]})
        else:
            lt.append({"name":i,"type":type_})
    return lt

@api_view(['GET'])
def get_info(request):
    zd={}
    zd["code"]=200;zd["message"]="";zd["data"]=transform_format(get_relationship(True)["data"],0)
    return Response(zd)