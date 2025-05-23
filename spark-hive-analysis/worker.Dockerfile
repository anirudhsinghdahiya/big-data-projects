FROM p5-base

WORKDIR /

# Set environment variables
ENV SPARK_HOME=/spark-3.5.5-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Expose Spark worker port
EXPOSE 8081

# Start Spark worker
CMD ["sh", "-c", "$SPARK_HOME/sbin/start-worker.sh spark://boss:7077 && tail -f $SPARK_HOME/logs/*"]