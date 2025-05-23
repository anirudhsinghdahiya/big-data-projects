import grpc
import sys
import os

# Add the proto directory to the path
sys.path.append('proto')

# Now import the generated modules
import property_lookup_pb2
import property_lookup_pb2_grpc

def run_test(host, zip_code=53706, limit=5):
    # Create a gRPC channel
    channel = grpc.insecure_channel(host)
    
    # Create a stub (client)
    stub = property_lookup_pb2_grpc.PropertyLookupStub(channel)
    
    # Create a request
    request = property_lookup_pb2.ZipRequest(zip=zip_code, limit=limit)
    
    # Make the call
    try:
        response = stub.LookupByZip(request)
        print(f"Received {len(response.addresses)} addresses:")
        for i, addr in enumerate(response.addresses):
            print(f"  {i+1}. {addr}")
    except grpc.RpcError as e:
        print(f"RPC error: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        host = sys.argv[1]
    else:
        host = "localhost:5000"
    
    if len(sys.argv) > 2:
        zip_code = int(sys.argv[2])
    else:
        zip_code = 53706
    
    if len(sys.argv) > 3:
        limit = int(sys.argv[3])
    else:
        limit = 5
    
    run_test(host, zip_code, limit)
