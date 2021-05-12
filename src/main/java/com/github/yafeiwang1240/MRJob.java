package com.github.yafeiwang1240;

import com.github.yafeiwang1240.job.HFileJob;

import java.io.IOException;
import java.text.ParseException;

/**
 * map reduce
 * @author wangyafei
 */
public class MRJob {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException {
        if (!HFileJob.readAndWrite(args)) {
            System.exit(-1);
        }
    }

}
