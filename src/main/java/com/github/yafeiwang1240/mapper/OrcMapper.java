package com.github.yafeiwang1240.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

/**
 * read orc
 * @author wangyafei
 */
public class OrcMapper extends Mapper<NullWritable, OrcStruct, Text, IntWritable> {

    // Assume the ORC file has type: struct<s:string,i:int>
    @Override
    public void map(NullWritable key, OrcStruct value,
                    Context output) throws IOException, InterruptedException {
        // take the first field as the key and the second field as the value
        output.write((Text) value.getFieldValue(0),
                (IntWritable) value.getFieldValue(1));
    }
}
