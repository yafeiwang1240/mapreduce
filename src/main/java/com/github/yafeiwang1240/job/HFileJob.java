package com.github.yafeiwang1240.job;

import com.github.yafeiwang1240.mapper.HFileMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.orc.OrcConf;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;

/**
 * mr读取HFile，写入orc
 * @author wangyafei
 */
public class HFileJob {

    public static boolean readAndWrite(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        OrcConf.MAPRED_OUTPUT_SCHEMA.setString(configuration,"struct<id:string,name:string>");
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("f"));
        scan.setCacheBlocks(false);
        Path path = new Path("hdfs://lynxcluster/tmp/snapshot");
        String snapName ="test_snapshot";
        Job job = Job.getInstance(configuration, "HFileJob");
        job.setJarByClass(HFileJob.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableSnapshotMapperJob(
                snapName, scan,
                HFileMapper.class,
                NullWritable.class, OrcStruct.class,
                job, true, path);
        job.setOutputFormatClass(OrcOutputFormat.class);
        OrcOutputFormat.setOutputPath(job, new Path("/temp/test"));

        return job.waitForCompletion(true);
    }
}
