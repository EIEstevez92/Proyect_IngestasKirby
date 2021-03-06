#!/bin/bash

: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

# altering the core-site configuration
sed s/HOSTNAME/$HOSTNAME/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml
mv /usr/local/hadoop/etc/hadoop/hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml.bak
echo -e "<configuration>\n\t<property>\n\t\t<name>dfs.replication</name>\n\t\t<value>1</value>\n\t</property>\n\t<property>\n\t\t<name>dfs.permissions.enabled</name>\n\t\t<value>false</value>\n\t</property>\n</configuration>" > /usr/local/hadoop/etc/hadoop/hdfs-site.xml

service sshd start
$HADOOP_PREFIX/sbin/start-dfs.sh

$HADOOP_PREFIX/bin/hadoop dfsadmin -safemode leave && $HADOOP_PREFIX/bin/hdfs dfs -put /misc/tests /  && echo COPIED && tail -f /dev/null



if [[ $1 == "-d" ]]; then
  while true; do sleep 1000; done
fi