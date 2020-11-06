package com.github.yafeiwang1240;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;

import java.io.IOException;

/**
 * map reduce
 * @author wangyafei
 */
public class MRJob {

    public static class ORCMapper extends Mapper<NullWritable, OrcStruct, Text, IntWritable> {

        // Assume the ORC file has type: struct<s:string,i:int>
        @Override
        public void map(NullWritable key, OrcStruct value,
                        Context output) throws IOException, InterruptedException {
            // take the first field as the key and the second field as the value
            output.write((Text) value.getFieldValue(0),
                    (IntWritable) value.getFieldValue(1));
        }
    }

    public static class TextMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Assume the Text file has type: struct<s:string,i:int>
        @Override
        public void map(LongWritable key, Text value,
                        Context output) throws IOException, InterruptedException {
            // take the first field as the key and the second field as the value
            String[] kv = value.toString().split(",");
            output.write(new Text(kv[0]), new IntWritable(Integer.valueOf(kv[1])));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        textRead(args);
    }

    private static boolean textRead(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MRJob.class);
        job.setJobName("textReader");
//        job.setMapperClass(TextMapper.class);
//        TextInputFormat.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"));
//        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"),
                TextInputFormat.class, TextMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/temp/text"));

        return job.waitForCompletion(true);
    }

    private static boolean orcRead(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MRJob.class);
        job.setJobName("orcReader");
//        job.setMapperClass(ORCMapper.class);
//        OrcInputFormat.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"));
//        job.setInputFormatClass(OrcInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"),
                OrcInputFormat.class, ORCMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/temp/text"));

        return job.waitForCompletion(true);
    }
}
