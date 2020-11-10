package com.github.yafeiwang1240.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reduce to text
 * @author wangyafei
 */
public class TextReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        for (IntWritable writable : values) {
            max = Math.max(writable.get(), max);
        }
        context.write(key, new IntWritable(max));
    }
}
