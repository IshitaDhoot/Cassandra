from concurrent import futures
from collections import OrderedDict
import math
import station_pb2_grpc
import station_pb2
import grpc
import threading

from cassandra.cluster import Cluster
try:
    cluster = Cluster(['project-5-codelegends-db-1', 'project-5-codelegends-db-2', 'project-5-codelegends-db-3'])
    cass = cluster.connect()
except Exception as e:
    cass.execute("drop table if exists weather")

lock = threading.Lock()


class Station(station_pb2_grpc.StationServicer):
    
    def RecordTemps(self, request, context):
        insert_statement = cass.prepare("INSERT INTO weather.stations VALUES (%d, %d)", (request.tmin, re))
        insert_statement.consistency_level = ConsistencyLevel.ONE
            
    
    def StationMax(self, request, context):
        #TODO
    
def server():
    
