# mapreduce

#### 设置JobClient环境变量

```shell
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/hadoop/wangyf/mapreduce/lib/*
```

#### 执行

```shell
hadoop jar mapreduce-1.0-SNAPSHOT.jar com.github.yafeiwang1240.ToolJob -libjars $(echo /home/hadoop/wangyf/mapreduce/lib/*.jar | tr ' ' ',')
```



#### 设置JobClient参数(调试)

```shell
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xrunjdwp:transport=dt_socket,server=y,address=10005,suspend=y -Djavax.security.auth.useSubjectCredsOnly=false -DSystemRuntimeEnvironment=online -Darea=formal"
```

#### 设置map参数(调试)

```shell
hadoop jar mapreduce-1.0-SNAPSHOT.jar com.github.yafeiwang1240.ToolJob -libjars $(echo /home/hadoop/wangyf/mapreduce/lib/*.jar | tr ' ' ',') -D mapreduce.map.java.opts='-Xmx3276m -Xrunjdwp:transport=dt_socket,server=y,address=10005,suspend=y'
```

#### 设置reduce参数(调试)

```shell
hadoop jar mapreduce-1.0-SNAPSHOT.jar com.github.yafeiwang1240.ToolJob -libjars $(echo /home/hadoop/wangyf/mapreduce/lib/*.jar | tr ' ' ',') -D mapreduce.reduce.java.opts='-Xmx3276m -Xrunjdwp:transport=dt_socket,server=y,address=10005,suspend=y'
```

