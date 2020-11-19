### mapreduce


# export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/hadoop/wangyf/mapreduce/lib/*
# hadoop jar mapreduce-1.0-SNAPSHOT.jar com.github.yafeiwang1240.ToolJob -libjars $(echo /home/hadoop/wangyf/mapreduce/lib/*.jar | tr ' ' ',')

# mapreduce.map.java.opts	-Xmx1638m	job.xml ⬅ mapred-site.xml
# mapreduce.reduce.java.opts	-Xmx3276m -Xrunjdwp:transport=dt_socket,server=y,address=10005,suspend=y job.xml ⬅ mapred-site.xml
