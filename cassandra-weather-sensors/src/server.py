import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
import os
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, trim
from cassandra.query import SimpleStatement, PreparedStatement, ConsistencyLevel
import cassandra

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        # Connect to Cassandra cluster
        # Get project prefix from environment variable
        prefix = os.environ.get('PROJECT', 'p6')
        
        # Connect to the Cassandra cluster
        cluster = Cluster([f'{prefix}-db-1', f'{prefix}-db-2', f'{prefix}-db-3'])
        self.session = cluster.connect()
        self.session.row_factory = dict_factory
        
        # Drop weather keyspace if it exists
        self.session.execute("DROP KEYSPACE IF EXISTS weather")
        
        # Create weather keyspace with 3x replication
        self.session.execute("""
            CREATE KEYSPACE weather 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)
        
        # Create station_record UDT
        self.session.execute("""
            CREATE TYPE weather.station_record (
                tmin int,
                tmax int
            )
        """)
        
        # Create stations table
        self.session.execute("""
            CREATE TABLE weather.stations (
                id text,
                date date,
                name text static,
                record frozen<weather.station_record>,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)
        
        # Prepare statements for RecordTemps and StationMax
        self.record_temps_prepared = self.session.prepare("""
            INSERT INTO weather.stations (id, date, record) 
            VALUES (?, ?, {tmin: ?, tmax: ?})
        """)
        
        self.station_max_prepared = self.session.prepare("""
            SELECT MAX(record.tmax) AS max_tmax 
            FROM weather.stations 
            WHERE id = ?
        """)
        
        # Create Spark session
        self.spark = SparkSession.builder.appName("p6").getOrCreate()
        
        # Load station data from ghcnd-stations.txt using Spark
        # According to NOAA readme, columns are:
        # ID: characters 1-11
        # STATE: characters 39-40 (for US stations)
        # NAME: characters 42-71
        stations_df = self.spark.read.text("ghcnd-stations.txt")
        
        # Extract ID, STATE, and NAME using substring
        stations_df = stations_df.select(
            trim(substring("value", 1, 11)).alias("id"),
            trim(substring("value", 39, 2)).alias("state"),
            trim(substring("value", 42, 30)).alias("name")
        )
        
        # Filter for Wisconsin stations (state = 'WI')
        wi_stations = stations_df.filter(stations_df.state == "WI").collect()
        
        # Insert Wisconsin stations into Cassandra
        for station in wi_stations:
            self.session.execute(
                """
                INSERT INTO weather.stations (id, name) 
                VALUES (%s, %s)
                """,
                (station["id"], station["name"])
            )
        
        # ============ Server Stated Successfully =============
        print("Server started") # Don't delete this line!

    def StationSchema(self, request, context):
        try:
            # Execute describe table query
            result = self.session.execute("DESCRIBE TABLE weather.stations")
            # Extract the create_statement from the result
            create_statement = result.one()['create_statement']
            return station_pb2.StationSchemaReply(schema=create_statement, error="")
        except Exception as e:
            return station_pb2.StationSchemaReply(schema="", error=str(e))

    def StationName(self, request, context):
        try:
            # Get station ID from request
            station_id = request.station
            
            # Query the database for the station name
            result = self.session.execute(
                """
                SELECT name FROM weather.stations 
                WHERE id = %s
                """,
                (station_id,)
            )
            
            # Get the first row (if any)
            row = result.one()
            
            if row:
                # Return the station name
                return station_pb2.StationNameReply(name=row["name"], error="")
            else:
                # Station not found
                return station_pb2.StationNameReply(name="", error=f"Station {station_id} not found")
        except Exception as e:
            # Error occurred
            return station_pb2.StationNameReply(name="", error=str(e))

    def RecordTemps(self, request, context):
        try:
            # Get data from request
            station_id = request.station
            date = request.date
            tmin = request.tmin
            tmax = request.tmax
            
            # Set consistency level to ONE for high write availability
            prepared = self.record_temps_prepared.bind((station_id, date, tmin, tmax))
            prepared.consistency_level = ConsistencyLevel.ONE
            
            # Insert data into Cassandra using prepared statement
            self.session.execute(prepared)
            
            # Return success
            return station_pb2.RecordTempsReply(error="")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable) as e:
            # Return specific error for unavailability
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
            # Return error for other exceptions
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        try:
            # Get station ID from request
            station_id = request.station
            
            # Set consistency level to THREE for read consistency when nodes are down
            prepared = self.station_max_prepared.bind((station_id,))
            prepared.consistency_level = ConsistencyLevel.THREE
            
            # Query the database for the maximum tmax for this station using prepared statement
            result = self.session.execute(prepared)
            
            # Get the result
            row = result.one()
            
            if row and row["max_tmax"] is not None:
                # Return the maximum temperature
                return station_pb2.StationMaxReply(tmax=row["max_tmax"], error="")
            else:
                # No data or no temperature record found
                return station_pb2.StationMaxReply(tmax=-1, error=f"No temperature data for station {station_id}")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable) as e:
            # Return specific error for unavailability
            return station_pb2.StationMaxReply(tmax=-1, error="unavailable")
        except Exception as e:
            # Return error for other exceptions
            return station_pb2.StationMaxReply(tmax=-1, error=str(e))

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
