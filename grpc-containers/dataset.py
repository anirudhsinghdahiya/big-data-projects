import grpc
import gzip
import csv
import concurrent.futures as futures
from google.protobuf.json_format import MessageToJson

# Import the generated classes from the proto file
# Note: These will be generated during Docker build
import property_lookup_pb2
import property_lookup_pb2_grpc

class PropertyLookupServicer(property_lookup_pb2_grpc.PropertyLookupServicer):
    def __init__(self):
        # Load data from CSV.gz file
        self.addresses_by_zip = {}
        self._load_data()
        
    def _load_data(self):
        with gzip.open('addresses.csv.gz', 'rt') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Extract address and zipcode from the row using the correct column names
                address = row.get('Address', '')
                zipcode = row.get('ZipCode', '')
                
                if zipcode and address:
                    try:
                        zipcode = int(zipcode)
                        if zipcode not in self.addresses_by_zip:
                            self.addresses_by_zip[zipcode] = []
                        self.addresses_by_zip[zipcode].append(address)
                    except ValueError:
                        # Skip rows where zipcode isn't a valid integer
                        pass
        
        # Sort addresses for each zipcode
        for zipcode in self.addresses_by_zip:
            self.addresses_by_zip[zipcode].sort()
    
    def LookupByZip(self, request, context):
        zip_code = request.zip
        limit = request.limit
        
        response = property_lookup_pb2.AddressResponse()
        
        if zip_code in self.addresses_by_zip:
            # Get addresses for the requested zipcode, limited by limit
            addresses = self.addresses_by_zip[zip_code][:limit]
            response.addresses.extend(addresses)
        
        return response

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), options=[("grpc.so_reuseport", 0)])
    property_lookup_pb2_grpc.add_PropertyLookupServicer_to_server(
        PropertyLookupServicer(), server
    )
    server.add_insecure_port("0.0.0.0:5000")
    server.start()
    print("Server started on port 5000")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
