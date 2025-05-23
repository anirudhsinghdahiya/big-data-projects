import os
import flask
import grpc
import time
import collections
from flask import Flask

# Import gRPC generated modules
import sys
sys.path.append('proto')
import property_lookup_pb2
import property_lookup_pb2_grpc

app = Flask("p2")

# Get the PROJECT environment variable for container names
PROJECT = os.environ.get('PROJECT', 'p2')
DATASET_1 = f"{PROJECT}-dataset-1:5000"
DATASET_2 = f"{PROJECT}-dataset-2:5000"

# Create LRU cache with size 3
class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = collections.OrderedDict()
    
    def get(self, key):
        if key in self.cache:
            # Move to end to mark as recently used
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        return None
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            # Remove the first item (least recently used)
            self.cache.popitem(last=False)
        self.cache[key] = value

# Create LRU cache with size 3
ZIP_CACHE = LRUCache(3)

# Track which dataset server to use next
next_server = 1

@app.route("/lookup/<zipcode>")
def lookup(zipcode):
    global next_server
    
    try:
        zipcode = int(zipcode)
        limit = flask.request.args.get("limit", default=4, type=int)
        
        error_msg = None
        source = None
        addresses = []
        
        # Check if we need data from cache
        if limit <= 8:
            # Try to get from cache
            cached_addresses = ZIP_CACHE.get(zipcode)
            if cached_addresses:
                addresses = cached_addresses[:limit]
                source = "cache"
            else:
                # Need to get from dataset and cache it
                addresses, source, error_msg = get_addresses_from_dataset(zipcode, 8)
                if addresses and not error_msg:
                    ZIP_CACHE.put(zipcode, addresses)
                    addresses = addresses[:limit]
        else:
            # Limit > 8, can't use cache effectively
            addresses, source, error_msg = get_addresses_from_dataset(zipcode, limit)
            # Still cache the first 8 if we got a successful response
            if addresses and not error_msg and len(addresses) >= 8:
                ZIP_CACHE.put(zipcode, addresses[:8])
        
        return flask.jsonify({
            "addrs": addresses,
            "source": source,
            "error": error_msg
        })
    
    except Exception as e:
        return flask.jsonify({
            "addrs": [],
            "source": None,
            "error": str(e)
        })

def get_addresses_from_dataset(zipcode, limit):
    global next_server
    
    # Determine which server to try first
    first_server = next_server
    if next_server == 1:
        servers = [DATASET_1, DATASET_2]
        next_server = 2
    else:
        servers = [DATASET_2, DATASET_1]
        next_server = 1
    
    # Try up to 5 times, alternating between servers
    max_attempts = 5
    attempt = 0
    last_error = None
    
    while attempt < max_attempts:
        server_index = attempt % 2
        server = servers[server_index]
        attempt += 1
        
        try:
            # Create gRPC connection
            with grpc.insecure_channel(server) as channel:
                stub = property_lookup_pb2_grpc.PropertyLookupStub(channel)
                
                # Make the RPC call
                request = property_lookup_pb2.ZipRequest(zip=zipcode, limit=limit)
                response = stub.LookupByZip(request)
                
                # Return the addresses and which server we used (1 or 2)
                source = "1" if (first_server == 1 and server_index == 0) or (first_server == 2 and server_index == 1) else "2"
                return list(response.addresses), source, None
        
        except grpc.RpcError as e:
            last_error = str(e)
            # Sleep for 100ms before retrying
            time.sleep(0.1)
    
    # If we reach here, all attempts failed
    return [], None, f"Failed after {max_attempts} attempts: {last_error}"

def main():
    app.run("0.0.0.0", port=8080, debug=False, threaded=False)

if __name__ == "__main__":
    main()
