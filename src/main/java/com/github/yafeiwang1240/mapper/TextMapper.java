package com.github.yafeiwang1240.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * read text
 * @author wangyafei
 */
public class TextMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Assume the Text file has type: struct<s:string,i:int>
    @Override
    public void map(LongWritable key, Text value,
                    Context output) throws IOException, InterruptedException {
        // take the first field as the key and the second field as the value
        String[] kv = value.toString().split(",");
        output.write(new Text(kv[0]), new IntWritable(Integer.valueOf(kv[1])));
    }
}
