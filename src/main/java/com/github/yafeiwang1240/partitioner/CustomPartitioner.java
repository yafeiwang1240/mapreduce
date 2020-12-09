package com.github.yafeiwang1240.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义int分区
 * @param <VALUE>
 */
public class CustomPartitioner<VALUE> extends Partitioner<IntWritable, VALUE> {

    @Override
    public int getPartition(IntWritable intWritable, VALUE value, int numPartitions) {
        return Math.abs(intWritable.get()) % numPartitions;
    }
}
