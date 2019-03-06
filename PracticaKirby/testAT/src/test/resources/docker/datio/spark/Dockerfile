FROM gettyimages/spark:2.1.0-hadoop-2.7

ENV EPSILON_HOST=http://node:8080
ENV EPSILON_API_KEY=apiKey

#Add JCE Extension
RUN cd /tmp/ && \
    curl -LO "http://download.oracle.com/otn-pub/java/jce/8/jce_policy-8.zip" -H 'Cookie: oraclelicense=accept-securebackup-cookie' && \
    unzip jce_policy-8.zip && \
    rm jce_policy-8.zip && \
    yes |cp -v /tmp/UnlimitedJCEPolicyJDK8/*.jar /usr/java/jre/lib/security/

# Fix a bug. Hadoop common include avro 1.7.4 which is not compatible with Spark
# I've applied following solution https://github.com/SingularitiesCR/spark-docker/issues/5
RUN curl http://ftp.cuhk.edu.hk/pub/packages/apache.org/avro/avro-1.7.7/java/avro-1.7.7.jar -o /tmp/avro-1.7.7.jar \
    && rm $HADOOP_HOME/share/hadoop/common/lib/avro-1.7.4.jar \
    && rm $HADOOP_HOME/share/hadoop/mapreduce/lib/avro-1.7.4.jar \
    && cp /tmp/avro-1.7.7.jar $HADOOP_HOME/share/hadoop/common/lib \
    && mv /tmp/avro-1.7.7.jar $HADOOP_HOME/share/hadoop/mapreduce/lib

WORKDIR $SPARK_HOME