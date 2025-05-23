import os
import grpc
import threading
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import concurrent.futures
from concurrent import futures

# Import the generated gRPC modules
import table_pb2
import table_pb2_grpc

# Global variables
file_list = []  # List to store paths to files
lock = threading.Lock()  # Lock for thread safety

class TableServicer(table_pb2_grpc.TableServicer):
    def Upload(self, request, context):
        # Create directories if they don't exist
        os.makedirs("csv_files", exist_ok=True)
        os.makedirs("parquet_files", exist_ok=True)
        
        # Generate file paths
        csv_path = f"csv_files/file_{len(file_list)}.csv"
        parquet_path = f"parquet_files/file_{len(file_list)}.parquet"
        
        try:
            # Write CSV data to file (without holding the lock)
            with open(csv_path, "wb") as f:
                f.write(request.csv_data)
            
            # Convert to parquet (without holding the lock)
            df = pd.read_csv(csv_path)
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_path)
            
            # Add file paths to our list (with lock)
            with lock:
                file_list.append((csv_path, parquet_path))
            
            return table_pb2.UploadResp(error="")
        except Exception as e:
            return table_pb2.UploadResp(error=str(e))
    
    def ColSum(self, request, context):
        column = request.column
        format_type = request.format
        total = 0
        
        # Get a snapshot of the file list with the lock
        with lock:
            files = file_list.copy()
        
        # Process each file (without holding the lock)
        try:
            for csv_path, parquet_path in files:
                if format_type == "csv":
                    # Sum column from CSV
                    df = pd.read_csv(csv_path)
                    if column in df.columns:
                        total += df[column].sum()
                else:
                    # Sum column from Parquet (only reading needed column)
                    if column in pq.read_schema(parquet_path).names:
                        table = pq.read_table(parquet_path, columns=[column])
                        df = table.to_pandas()
                        total += df[column].sum()
            
            return table_pb2.ColSumResp(total=total)
        except Exception as e:
            return table_pb2.ColSumResp(error=str(e))

def serve():
    # Create server with 8 worker threads
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8),
        options=[("grpc.so_reuseport", 0)]
    )
    
    table_pb2_grpc.add_TableServicer_to_server(TableServicer(), server)
    server.add_insecure_port("0.0.0.0:5440")
    server.start()
    print("Server started on port 5440")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()