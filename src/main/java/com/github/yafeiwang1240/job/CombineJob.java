package com.github.yafeiwang1240.job;

import com.github.yafeiwang1240.MRJob;
import com.github.yafeiwang1240.mapper.TextMapper;
import com.github.yafeiwang1240.reduce.Combiner;
import com.github.yafeiwang1240.reduce.TextReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * read and write text
 * @author wangyafei
 */
public class CombineJob {

    public static boolean readAndWrite(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(MRJob.class);
        job.setJobName("combiner");
        job.setMapperClass(TextMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setCombinerClass(Combiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"));
        job.setReducerClass(TextReduce.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path("/temp/text"));

        return job.waitForCompletion(true);
    }
}
