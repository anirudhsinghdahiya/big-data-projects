FROM p5-base
RUN hdfs namenode -format -force
CMD ["bash", "-c", "hdfs namenode -format -force && hdfs namenode -fs hdfs://nn:9000"]
