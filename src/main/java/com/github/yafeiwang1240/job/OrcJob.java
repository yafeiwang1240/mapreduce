package com.github.yafeiwang1240.job;

import com.github.yafeiwang1240.MRJob;
import com.github.yafeiwang1240.mapper.OrcMapper;
import com.github.yafeiwang1240.reduce.OrcReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.orc.OrcConf;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

/**
 * read and write orc
 * @author wangyafei
 */
public class OrcJob {

    public static boolean readAndWrite(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(configuration,"struct<name:string,age:int>");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MRJob.class);
        job.setJobName("orcReader");
//        job.setMapperClass(ORCMapper.class);
//        OrcInputFormat.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"));
//        job.setInputFormatClass(OrcInputFormat.class);
        MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"),
                OrcInputFormat.class, OrcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(OrcReduce.class);
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(OrcOutputFormat.class);
        OrcOutputFormat.setOutputPath(job, new Path("/temp/text"));

        return job.waitForCompletion(true);
    }
}
