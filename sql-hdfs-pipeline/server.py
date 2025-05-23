import grpc
import lender_pb2
import lender_pb2_grpc
from sqlalchemy import create_engine
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
import requests
import time
import os
from concurrent import futures

class LenderService(lender_pb2_grpc.LenderServicer):
    def DbToHdfs(self, request, context):
        """
        Part 1:
         - Connect to MySQL, join + filter
         - Write /hdma-wi-2021.parquet in HDFS
        """
        for _ in range(5):
            try:
                engine = create_engine("mysql+mysqlconnector://root:abc@mysql:3306/CS544")
                conn = engine.connect()
                break
            except:
                time.sleep(3)

        sql = """
            SELECT
                loans.*,
                loan_types.loan_type_name
            FROM loans
            INNER JOIN loan_types
              ON loans.loan_type_id = loan_types.id
            WHERE loan_amount BETWEEN 30000 AND 800000
        """
        df = pd.read_sql(sql, conn)

        table = pa.Table.from_pandas(df)
        parquet_path = "/hdma-wi-2021.parquet"
        hdfs = fs.HadoopFileSystem(host="boss", port=9000, default_block_size=1024*1024, replication=2)

        with hdfs.open_output_stream(parquet_path) as f:
            pq.write_table(table, f)

        return lender_pb2.StatusString(status="Data successfully written to /hdma-wi-2021.parquet")

    def BlockLocations(self, request, context):
        """
        Part 2:
         - GETFILEBLOCKLOCATIONS
        """
        file_path = request.path
        base_url = "http://boss:9870/webhdfs/v1"
        full_url = f"{base_url}{file_path}?op=GETFILEBLOCKLOCATIONS&offset=0&length=9999999999"

        resp = requests.get(full_url)
        if resp.status_code != 200:
            return lender_pb2.BlockLocationsResp(
                block_entries={},
                error=f"GETFILEBLOCKLOCATIONS HTTP {resp.status_code}"
            )

        data = resp.json()
        try:
            blocks_json = data["BlockLocations"]["BlockLocation"]
        except KeyError as e:
            return lender_pb2.BlockLocationsResp(
                block_entries={},
                error=f"Missing BlockLocations key in JSON: {e}"
            )

        block_map = {}
        for blk in blocks_json:
            for host in blk["hosts"]:
                block_map[host] = block_map.get(host, 0) + 1

        return lender_pb2.BlockLocationsResp(block_entries=block_map, error="")

    def CalcAvgLoan(self, request, context):
        """
        Part 3 & 4: Calculate average loan amount with fault tolerance
        - First try to use county-specific file
        - Fall back to main file if needed
        - Handle DataNode failures
        """
        # Get parameters from request
        county_id = request.county_code
        
        # Define file paths
        main_parquet = "/hdma-wi-2021.parquet"
        county_parquet = f"/partitions/{county_id}.parquet"
        
        # Ensure local directories exist
        try:
            os.makedirs("partitions", exist_ok=True)
        except:
            pass
            
        # Connect to HDFS
        hdfs_conn = fs.HadoopFileSystem("boss", 9000, replication=1)
        
        # Try three different paths for data access
        try:
            # Path 1: County file exists and is accessible
            with hdfs_conn.open_input_file(county_parquet) as f:
                data_table = pq.read_table(f)
                data_source = "reuse"
        except FileNotFoundError:
            # Path 2: County file doesn't exist yet
            with hdfs_conn.open_input_file(main_parquet) as f:
                data_table = pq.read_table(f, filters=[[("county_code"), "=", county_id]])
                pq.write_table(data_table, county_parquet, filesystem=hdfs_conn)
                data_source = "create"
        except OSError:
            # Path 3: DataNode failure - county file exists but is inaccessible
            with hdfs_conn.open_input_file(main_parquet) as f:
                data_table = pq.read_table(f, filters=[[("county_code"), "=", county_id]])
                pq.write_table(data_table, county_parquet, filesystem=hdfs_conn)
                data_source = "recreate"
        
        # Calculate average loan amount (must convert to pandas first)
        loan_series = data_table["loan_amount"].to_pandas()
        average = int(loan_series.mean())
        
        # Return result
        return lender_pb2.CalcAvgLoanResp(avg_loan=average, source=data_source)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lender_pb2_grpc.add_LenderServicer_to_server(LenderService(), server)
    server.add_insecure_port("[::]:5000")
    server.start()
    print("gRPC server started on port 5000 (Parts 1,2,3).")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
