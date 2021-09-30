# -*- coding: utf-8 -*-

from flask import Flask, jsonify, url_for, redirect, request
from flask_pymongo import PyMongo
from flask_restful import Api, Resource
import socket

app = Flask(__name__)
app.config["MONGO_DBNAME"] = "car_DB"
mongo = PyMongo(app, config_prefix='MONGO')



def gethostbyname():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    host = s.getsockname()[0]
    s.close()
    return host

ipv4 = str(gethostbyname())
APP_URL = "http://%s:5000"%ipv4


class VehicleAnalysis_Collector(Resource):

    def getNextSequence(self,collection_name):
        id = mongo.db.counters.find_and_modify(query={'collection': collection_name},update={'$inc': {'seq': 1}},new=True).get('seq')
        return id

    def post(self):
            data = request.get_json()
            print('d:',data)
            if not data:
                data = {"response": "TES"}
                return jsonify(data)
            else:
                data['_id'] = self.getNextSequence('Result_table')
                mongo.db.Result_table.insert(data)
                
            print(data)

            return redirect(url_for("Results"))
    def put(self,id=None):
        data = request.get_json()
        mongo.db.Result_table.update({'_id': int(id)}, {'$set': data})
        return jsonify({"response": id})    #redirect(url_for("Results"))
       
        
    def get(self,id=None,limit=0):
        data = []
        
        if id and limit==0:
            Result_info = mongo.db.Result_table.find_one({"_id": int(id)}, {"image":0,"update_time": 0})
            if Result_info:
                Result_info['url'] = APP_URL + url_for('Results') + "/" + str(Result_info.get('_id'))
                return jsonify({"response": Result_info})
            else:

                return {"response": "no Result found for {}".format(id)}

        if id and limit:
            cursor = mongo.db.Result_table.find({'_id' : {'$lt' :int(id)+200, '$gte' : int(id)}},{"update_time": 0}).limit(150)
            if cursor:
                for Result in cursor: 
                  Result['url'] = APP_URL + url_for('Results') + "/" + str(Result.get('_id'))
                  data.append(Result)
                return jsonify({"response": data})
            else:
                return {"response": "no Result found for {}".format(id)}

        else:
            cursor = mongo.db.Result_table.find({}, {"image":0,"update_time": 0})

            for Result in cursor:
                Result['url'] = APP_URL + url_for('Results') + "/" + str(Result.get('_id'))
                data.append(Result)
            return jsonify({"response": data})

class Road_Collector(Resource):

    def getNextSequence(self,collection_name):
        id = mongo.db.counters.find_and_modify(query={'collection': collection_name},update={'$inc': {'seq': 1}},new=True).get('seq')
        return id

    def get(self,id=None,limit=0):
        data = []
        
        if id and limit==0:
            Road_info = mongo.db.Road_table.find_one({"_id": int(id)})
            if Road_info:
                Road_info['url'] = APP_URL + url_for('Roads') + "/" + str(Road_info.get('_id'))
                return jsonify({"response": Road_info})
            else:
                return {"response": "no Road found for {}".format(id)}

        if id and limit:
            cursor = mongo.db.Road_table.find({'_id' : {'$lt' :int(id)+50, '$gte' : int(id)}},{"update_time": 0})
            if cursor:
                for Road in cursor: 
                  Road['url'] = APP_URL + url_for('Roads') + "/" + str(Road.get('_id'))
                  data.append(Road)
                return jsonify({"response": data})
            else:
                return {"response": "no Road found for {}".format(id)}

        else:
            cursor = mongo.db.Road_table.find({}, {"update_time": 0}).limit(10)

            for Road in cursor:
                Road['url'] = APP_URL + url_for('Roads') + "/" + str(Road.get('_id'))
                data.append(Road)
            return jsonify({"response": data})

    def post(self):
            data = request.get_json()
            if not data:
                data = {"response": "ERROR"}
                return jsonify(data)
            else:
                data['_id'] = self.getNextSequence('Road_table')
                mongo.db.Road_table.insert(data)
                
            print(data)

            return redirect(url_for("Roads"))
    def delete(self, id=None):
          if id:
            mongo.db.Road_table.remove({'_id': id})
          else:
            mongo.db.Road_table.remove({})
          return redirect(url_for("GPSs"))

class OBD_Collector(Resource):
    def getNextSequence(self,collection_name):
        id = mongo.db.counters.find_and_modify(query={'collection': collection_name},update={'$inc': {'seq': 1}},new=True).get('seq')
        return id
    def get(self,id=None,limit=0,ticks=0):
        data = []
       
        if ticks:
            ticks = int(float(ticks))
            OBD_info = mongo.db.OBD_table.find_one({'ticks' : {'$lt' :ticks+500, '$gte' : ticks}},{"update_time": 0})
            #OBD_info = mongo.db.OBD_table.find_one({'ticks' : ticks},{"update_time": 0})
            if OBD_info:
              
              return jsonify({"response": OBD_info})
            else:
              return {"response": "no OBD found for {}".format(id)}

        elif id and limit==0:
            OBD_info = mongo.db.OBD_table.find_one({"_id": int(id)})
            
            if OBD_info:
                OBD_info['url'] = APP_URL + url_for('OBDs') + "/" + str(OBD_info.get('_id'))
                return jsonify({"status": "ok", "data": OBD_info})
            else:
                return {"response": "no OBD found for {}".format(id)}
        elif id and limit:
            OBD_info = mongo.db.OBD_table.find({'_id' : {'$lt' :int(id)+50, '$gte' : int(id)}},{"update_time": 0})
            if cursor:
                for OBD in cursor: 
                  OBD['url'] = APP_URL + url_for('OBDs') + "/" + str(OBD.get('_id'))
                  data.append(OBD)
                return jsonify({"response": data})
            else:
                return {"response": "no OBD found for {}".format(id)}
        else:
            cursor = mongo.db.OBD_table.find({}, {"update_time": 0}).limit(5)

            for OBD in cursor:
                print(OBD)
                OBD['url'] = APP_URL + url_for('OBDs') + "/" + str(OBD.get('id'))
                data.append(OBD)
            return jsonify({"response": data})

    def post(self):
        data = request.get_json()
  
        if not data:
            data = {"response": "ERROR"}
            return jsonify(data)
        else:
                data['_id'] = self.getNextSequence('OBD_table')
                mongo.db.OBD_table.insert(data)
            

        return redirect(url_for("OBDs"))

    def delete(self, id=None):
        if id:
          mongo.db.OBD_table.remove({'_id': id})
        else:
          mongo.db.OBD_table.remove({})
        return redirect(url_for("OBDs"))

class GPS_Collector(Resource):

    def getNextSequence(self,collection_name):
        id = mongo.db.counters.find_and_modify(query={'collection': collection_name},update={'$inc': {'seq': 1}},new=True).get('seq')
        return id
  
    def get(self, id=None, limit=0, ticks = 0):
        data = []
        if ticks:
            ticks = int(float(ticks))
            GPS_info = mongo.db.GPS_table.find_one({'ticks' : {'$lt' :ticks+50, '$gte' : ticks}},{"update_time": 0})
            #OBD_info = mongo.db.OBD_table.find_one({'ticks' : ticks},{"update_time": 0})
            if GPS_info:
              
              return jsonify({"response": GPS_info})#return 1 row
            else:
              return {"response": "no GPS found for {}".format(id)}

        elif id and limit==0:
            GPS_info = mongo.db.GPS_table.find_one({"_id": int(id)})
            if GPS_info:
                GPS_info['url'] = APP_URL + url_for('GPSs') + "/" + str(GPS_info.get('_id'))
                return jsonify({"status": "ok", "data": GPS_info})
            else:
                return {"response": "no GPS found for {}".format(id)}
        elif id and limit:
            cursor = mongo.db.GPS_table.find({'_id' : {'$lt' :int(id)+50, '$gte' : int(id)}},{"update_time": 0})
            if cursor:
                for GPS in cursor: 
                  GPS['url'] = APP_URL + url_for('GPSs') + "/" + str(GPS.get('_id'))
                  data.append(GPS)
                return jsonify({"response": data})
            else:
                return {"response": "no GPS found for {}".format(id)}
        else:
            cursor = mongo.db.GPS_table.find({}, {"update_time": 0}).limit(1000)

            for GPS in cursor:
                print(GPS)
                GPS['url'] = APP_URL + url_for('GPSs') + "/" + str(GPS.get('id'))
                data.append(GPS)

            return jsonify({"response": data})

    def post(self):
        data = request.get_json()
        if not data:
            data = {"response": "ERROR"}
            return jsonify(data)
        else:
            data['_id'] = self.getNextSequence('GPS_table')
            mongo.db.GPS_table.insert(data)
            

        return redirect(url_for("GPSs"))

    def delete(self, id=None):
        if id:
          mongo.db.GPS_table.remove({'_id': id})
        else:
          mongo.db.GPS_table.remove({})
        return redirect(url_for("GPSs"))




api = Api(app)
api.add_resource(GPS_Collector, "/api/GPS", endpoint="GPSs")
api.add_resource(GPS_Collector, "/api/GPS/<string:id>", endpoint="GPSid")
api.add_resource(GPS_Collector, "/api/GPS/<string:id>/<int:limit>", endpoint="GPSidlimit")
api.add_resource(GPS_Collector, "/api/GPS/ticks/<string:ticks>", endpoint="GPStickslimit")

api.add_resource(OBD_Collector, "/api/OBD", endpoint="OBDs")
api.add_resource(OBD_Collector, "/api/OBD/<string:id>", endpoint="OBDid")
api.add_resource(OBD_Collector, "/api/OBD/<string:id>/<int:limit>", endpoint="OBDidlimit")
api.add_resource(OBD_Collector, "/api/OBD/ticks/<string:ticks>", endpoint="OBDtickslimit")

api.add_resource(Road_Collector, "/api/Road", endpoint="Roads")
api.add_resource(Road_Collector, "/api/Road/<string:id>", endpoint="Roadid")
api.add_resource(Road_Collector, "/api/Road/<string:id>/<int:limit>", endpoint="Roadidlimit")

api.add_resource(VehicleAnalysis_Collector, "/api/Results", endpoint="Results")
api.add_resource(VehicleAnalysis_Collector, "/api/Results/<string:id>", endpoint="Resultid")
api.add_resource(VehicleAnalysis_Collector, "/api/Results/<string:id>/<int:limit>", endpoint="Resultsidlimit")

if __name__ == "__main__":
    app.run(host=ipv4,debug=True)
