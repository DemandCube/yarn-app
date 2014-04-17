Compile
=======
1. mvn package


Runtime Rerequisite
===================
1. Install Hadoop2.0 + yarn
2. Edit yarn-site.xml. Make sure that yarn.nodemanager.delete.debug-delay-sec is set to something other than zero. Otherwise, Yarn will conveniently delete your local log files.
3. Copy the jar file to hdfs
```
hdfs dfs -copyFromLocal [name of the jar].jar /
```


Run
===
```
/usr/lib/hadoop/bin/hadoop jar yarn-0.0.1-SNAPSHOT.jar com.demandcube.yarn.Client -am_mem 300 -container_mem 300 --container_cnt 4 --hdfsjar /app/yarn-0.0.1-SNAPSHOT.jar --app_name foobar --command "echo" --am_class_name "com.demandcube.yarn.SampleAM"

```
