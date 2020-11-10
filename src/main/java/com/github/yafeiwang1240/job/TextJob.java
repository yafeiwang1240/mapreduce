package com.github.yafeiwang1240.job;

import com.github.yafeiwang1240.MRJob;
import com.github.yafeiwang1240.mapper.TextMapper;
import com.github.yafeiwang1240.reduce.TextReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * read and write text
 * @author wangyafei
 */
public class TextJob {

    public static boolean readAndWrite(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MRJob.class);
        job.setJobName("textReader");
//        job.setMapperClass(TextMapper.class);
//        TextInputFormat.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"));
//        job.setInputFormatClass(TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"),
                TextInputFormat.class, TextMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(TextReduce.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/temp/text"));

        return job.waitForCompletion(true);
    }
}
