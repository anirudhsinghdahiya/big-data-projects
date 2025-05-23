FROM p5-base

WORKDIR /

# Set environment variables
ENV SPARK_HOME=/spark-3.5.5-bin-hadoop3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV SPARK_MASTER_HOST=boss
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Expose Spark master ports
EXPOSE 7077 8080

# Start Spark master
CMD ["sh", "-c", "$SPARK_HOME/sbin/start-master.sh && tail -f $SPARK_HOME/logs/*"]