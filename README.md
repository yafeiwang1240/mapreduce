# mapreduce

// 设置JobClient环境变量
### export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/hadoop/wangyf/mapreduce/lib/*
// 执行
### hadoop jar mapreduce-1.0-SNAPSHOT.jar com.github.yafeiwang1240.ToolJob -libjars $(echo /home/hadoop/wangyf/mapreduce/lib/*.jar | tr ' ' ',')

// 设置map参数
### mapreduce.map.java.opts	-Xmx1638m	job.xml ⬅ mapred-site.xml
// 设置reduce调试参数
### mapreduce.reduce.java.opts	-Xmx3276m -Xrunjdwp:transport=dt_socket,server=y,address=10005,suspend=y job.xml ⬅ mapred-site.xml
