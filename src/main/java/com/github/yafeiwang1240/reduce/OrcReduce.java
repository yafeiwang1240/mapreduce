package com.github.yafeiwang1240.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;

/**
 * reduce to orc
 * @author wangyafei
 */
public class OrcReduce extends Reducer<Text, IntWritable, NullWritable, OrcStruct> {

    private TypeDescription schema =
            TypeDescription.fromString("struct<name:string,age:int>");

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        for (IntWritable writable : values) {
            max = Math.max(writable.get(), max);
        }
        OrcStruct pair = (OrcStruct) OrcStruct.createValue(schema);
        pair.setFieldValue(0, key);
        pair.setFieldValue(1, new IntWritable(max));
        context.write(NullWritable.get(), pair);
    }
}
