package com.github.yafeiwang1240;

import com.github.yafeiwang1240.mapper.OrcMapper;
import com.github.yafeiwang1240.reduce.OrcReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.orc.OrcConf;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;

public class ToolJob implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            Path out = new Path("/temp/test");
            if (fileSystem.exists(out)) {
                fileSystem.delete(out, true);
            }
            OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf,"struct<name:string,age:int>");
            Job job = Job.getInstance(conf);
            job.setJarByClass(MRJob.class);
            job.setJobName("orcReader");
            job.setMapperClass(OrcMapper.class);
            OrcInputFormat.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"));
            job.setInputFormatClass(OrcInputFormat.class);
//            MultipleInputs.addInputPath(job, new Path("/user/hive/warehouse/pub_insert_test"),
//                    OrcInputFormat.class, OrcMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setReducerClass(OrcReduce.class);
            job.setNumReduceTasks(1);
            job.setOutputFormatClass(OrcOutputFormat.class);
            OrcOutputFormat.setOutputPath(job, out);

            if (job.waitForCompletion(true)) {
                return 0;
            }
            return -1;
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        System.setProperty("SystemRuntimeEnvironment", conf.get("SystemRuntimeEnvironment", "online"));
        System.setProperty("area", conf.get("area", "formal"));
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new ToolJob(), args);
        System.exit(run);
    }
}
