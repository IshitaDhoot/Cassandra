from concurrent import futures
from collections import OrderedDict
import math
import station_pb2_grpc
import station_pb2
import grpc
import threading

from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel


class Station(station_pb2_grpc.StationServicer):
    
    def __init__(self):
        self.cluster = Cluster(['project-5-codelegends-db-1', 'project-5-codelegends-db-2', 'project-5-codelegends-db-3'])
        self.session = self.cluster.connect()
        self.session.set_keyspace('weather')

    def RecordTemps(self, request, context):
        try:
            insert_statement = self.session.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, {tmin:?, tmax:?})")
            insert_statement.consistency_level = ConsistencyLevel.ONE
            self.session.execute(insert_statement, (request.station, request.date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error="")
        except (ValueError) as e:
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        query = "SELECT max(record.tmax) FROM weather.stations WHERE id=%s"
        rows = self.session.execute(query, (request.station,))
        max_tmax = rows[0][0] if rows[0][0] is not None else -1
        return station_pb2.StationMaxReply(tmax=max_tmax)
    
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=[("grpc.so_reuseport", 0)])
    station_pb2_grpc.add_StationServicer_to_server(Station(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    server()
