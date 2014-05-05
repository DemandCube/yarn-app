Install Hadoop
==============
Downloading hadoop + yarn
```
mkdir -p ~/programs
cd ~/programs
wget http://apache.mirrors.spacedump.net/hadoop/common/stable/hadoop-2.2.0.tar.gz
tar xvf hadoop-2.2.0.tar.gz --gzip
rm hadoop-2.2.0.tar.gz
```
The following lines should be added to your ~/.bashrc, make sure to change your_username by your current username.
```
export HADOOP_PREFIX="/home/your_username/programs/hadoop-2.2.0"
export HADOOP_HOME=$HADOOP_PREFIX
export HADOOP_COMMON_HOME=$HADOOP_PREFIX
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export HADOOP_HDFS_HOME=$HADOOP_PREFIX
export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
export HADOOP_YARN_HOME=$HADOOP_PREFIX
```
Change $HADOOP_PREFIX/etc/hadoop/hdfs-site.xml to have the following :
```
<configuration>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///home/your_username/programs/hadoop-2.2.0/hdfs/datanode</value>
    <description>Comma separated list of paths on the local filesystem of a DataNode where it should store its blocks.</description>
  </property>
 
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///home/your_username/programs/hadoop-2.2.0/hdfs/namenode</value>
    <description>Path on the local filesystem where the NameNode stores the namespace and transaction logs persistently.</description>
  </property>
</configuration>
```
Add the following to $HADOOP_PREFIX/etc/hadoop/core-site.xml to let the Hadoop modules know where the HDFS NameNode is located.
```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost/</value>
    <description>NameNode URI</description>
  </property>
</configuration>
```
Add the following to $HADOOP_PREFIX/etc/hadoop/yarn-site.xml.
```
<configuration>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>128</value>
    <description>Minimum limit of memory to allocate to each container request at the Resource Manager.</description>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>2048</value>
    <description>Maximum limit of memory to allocate to each container request at the Resource Manager.</description>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
    <description>The minimum allocation for every container request at the RM, in terms of virtual CPU cores. Requests lower than this won't take effect, and the specified value will get allocated the minimum.</description>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>2</value>
    <description>The maximum allocation for every container request at the RM, in terms of virtual CPU cores. Requests higher than this won't take effect, and will get capped to this value.</description>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>4096</value>
    <description>Physical memory, in MB, to be made available to running containers</description>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>4</value>
    <description>Number of CPU cores that can be allocated for containers.</description>
  </property>
  <property>
    <name>yarn.nodemanager.delete.debug-delay-sec</name>
    <value>10000000</value>
  </property>
</configuration>
```

Starting
```
## Start HDFS daemons
# Format the namenode directory (DO THIS ONLY ONCE, THE FIRST TIME)
$HADOOP_PREFIX/bin/hdfs namenode -format
# Start the namenode daemon
$HADOOP_PREFIX/sbin/hadoop-daemon.sh start namenode
# Start the datanode daemon
$HADOOP_PREFIX/sbin/hadoop-daemon.sh start datanode
 
## Start YARN daemons
# Start the resourcemanager daemon
$HADOOP_PREFIX/sbin/yarn-daemon.sh start resourcemanager
# Start the nodemanager daemon
$HADOOP_PREFIX/sbin/yarn-daemon.sh start nodemanager
```

Compile
=======

1. mvn package

2. Copy the yarn-0.0.1-SNAPSHOT.jar to Hadoop Folder, then copy it to hdfs.
```
$HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal yarn-0.0.1-SNAPSHOT.jar /
```

Run
===
```
$HADOOP_PREFIX/bin/hadoop jar yarn-0.0.1-SNAPSHOT.jar com.demandcube.yarn.Client -am_mem 300 -container_mem 300 --container_cnt 4 --hdfsjar /yarn-0.0.1-SNAPSHOT.jar --app_name foobar --command "echo" --am_class_name "com.demandcube.yarn.SampleAM"

```
