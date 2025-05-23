FROM python:3.9-slim

WORKDIR /app

# Install dependencies needed for our server
RUN pip install --no-cache-dir grpcio==1.70.0 grpcio-tools==1.70.0 protobuf==5.29.3 pandas pyarrow

# Copy the proto file
COPY table.proto .

# Generate Python code from proto
RUN python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. table.proto

# Copy server code
COPY server.py .

# Copy client programs
COPY upload.py csvsum.py parquetsum.py bigdata.py ./

# Make an inputs directory
RUN mkdir -p /inputs

# Expose the port
EXPOSE 5440

# Run the server
CMD ["python", "server.py"]
