### mapreduce


export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/hadoop/wangyf/mapreduce/lib/*
hadoop jar mapreduce-1.0-SNAPSHOT.jar com.github.yafeiwang1240.ToolJob -libjars $(echo /home/hadoop/wangyf/mapreduce/lib/*.jar | tr ' ' ',')