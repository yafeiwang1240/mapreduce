package com.github.yafeiwang1240;

import com.github.yafeiwang1240.job.HFileJob;

import java.io.IOException;

/**
 * map reduce
 * @author wangyafei
 */
public class MRJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (!HFileJob.readAndWrite(args)) {
            System.exit(-1);
        }
    }

}
