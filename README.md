# Big Data Systems Portfolio
*CS 544: Big Data Systems - University of Wisconsin-Madison*

A comprehensive collection of distributed systems and big data projects demonstrating expertise in modern data infrastructure, stream processing, and scalable architectures.

## 🏗️ Architecture Overview

This repository showcases end-to-end data engineering workflows using industry-standard technologies:
- **Distributed Storage**: HDFS, Cassandra
- **Stream Processing**: Kafka, Spark Streaming  
- **Data Processing**: Spark, Hive, SQL
- **Cloud Platforms**: Google Cloud (BigQuery, GCS, Dataform)
- **Communication**: gRPC, REST APIs
- **Containerization**: Docker, Docker Compose

## 📂 Project Portfolio

### 🔄 [Distributed gRPC Services](./grpc-containers/)
**Technologies**: gRPC, Docker, Load Balancing, LRU Caching
- Multi-container application with fault-tolerant architecture
- gRPC communication between storage and caching layers
- Automatic failover and retry mechanisms
- **Key Learning**: Service discovery, replication strategies, distributed system reliability

### 🧵 [Thread-Safe Data Processing](./threadsafe-tables/)
**Technologies**: gRPC, Parquet, CSV Processing, Concurrency
- Multi-threaded server handling concurrent CSV/Parquet operations
- Thread-safe data structures with proper locking mechanisms
- Memory-efficient processing under 512MB constraints
- **Key Learning**: Concurrency control, file format optimization, resource management

### 🏗️ [SQL-to-HDFS Data Pipeline](./sql-hdfs-pipeline/)
**Technologies**: SQL Server, HDFS, PyArrow, WebHDFS, Fault Tolerance
- ETL pipeline processing 447K+ loan records with complex joins
- HDFS integration with 2x replication and 1MB block sizing
- Fault-tolerant design handling DataNode failures
- **Key Learning**: ETL design patterns, distributed storage reliability, data partitioning

### ⚡ [Spark & Hive Analytics Platform](./spark-hive-analysis/)
**Technologies**: PySpark, Hive, MLlib, RDD/DataFrame APIs
- Large-scale analysis of competitive programming datasets
- Machine learning pipeline with decision trees (R² optimization)
- Multi-API approach: RDD, DataFrame, and Spark SQL
- **Key Learning**: Big data analytics, ML at scale, query optimization

### 🌡️ [Cassandra Weather Sensor Network](./cassandra-weather-sensors/) 
**Technologies**: Cassandra, gRPC, Spark, Consistency Levels
- Real-time weather data ingestion from multiple stations
- 3-node Cassandra cluster with configurable consistency (R+W>RF)
- Spark integration for data preprocessing and analysis
- **Key Learning**: NoSQL design patterns, CAP theorem tradeoffs, distributed consensus

### 🚀 [Kafka Real-Time Streaming](./kafka-streaming-pipeline/) *
**Technologies**: Kafka, MySQL, HDFS, Protobuf, Exactly-Once Processing
- Event-driven architecture with producer-consumer patterns
- Exactly-once semantics with atomic checkpoint management  
- Real-time data pipeline: MySQL → Kafka → Parquet/HDFS
- **Key Learning**: Stream processing, message queues, event sourcing

### ☁️ [Google Cloud Data Warehouse](./gcp-bigquery-analytics/) *
**Technologies**: BigQuery, GCS, Dataform, Geospatial Analysis
- Cloud-native data pipeline with BigQuery and Cloud Storage
- Dataform-managed ETL workflows with dependency graphs
- Geographic analysis of Wisconsin school data with spatial joins
- **Key Learning**: Cloud data architecture, infrastructure as code, geospatial processing

*\* Code recovered from 6 out of 8 projects - READMEs available for reference*

## 🎯 Key Technical Achievements

- **Scale**: Processed datasets ranging from 100K to 500K+ records
- **Fault Tolerance**: Implemented retry logic, replication, and graceful degradation
- **Performance**: Optimized for throughput with caching, partitioning, and parallel processing
- **Modern Stack**: Production-ready technologies used by major tech companies
- **Documentation**: Comprehensive technical documentation with deployment instructions

## 🚀 Quick Start

Each project directory contains:
- Complete source code with Docker configurations
- Architecture diagrams and system design documentation
- Sample data and comprehensive test cases
- Deployment scripts for easy local setup

```bash
# Clone and explore any project
git clone https://github.com/anirudhsinghdahiya/big-data-projects.git
cd big-data-projects/<project-folder>
docker-compose up -d
```
## 📈 Learning Progression

This portfolio demonstrates progression from basic distributed concepts to complex real-world systems:

1. **Foundation**: gRPC communication and containerization
2. **Concurrency**: Thread-safe programming and resource management  
3. **Storage**: SQL and distributed file systems (HDFS)
4. **Processing**: Large-scale analytics with Spark ecosystem
5. **NoSQL**: Cassandra design patterns and consistency tuning
6. **Streaming**: Real-time data pipelines with Kafka
7. **Cloud**: Modern cloud-native data architecture

---

*Built during CS 544: Big Data Systems at University of Wisconsin-Madison*  
*Showcasing production-ready distributed systems and data engineering skills*
