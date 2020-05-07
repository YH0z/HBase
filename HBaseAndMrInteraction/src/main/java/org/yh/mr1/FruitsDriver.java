/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: FruitsDriver.java
 * File type: JAVA
 * Create time: 5/6/2020 7:44 PM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.mr1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

/**
 * @className FruitsDriver
 * @author YUAN_HAO
 * @time 5/6/2020 7:44 PM
 * @category 
 * @description  
 * @version 1.0
 */
public class FruitsDriver implements Serializable, Tool {
    static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(FruitsDriver.class);
    private static final long serialVersionUID = 1L;
    // Defend a org.apache.hadoop.conf.Configuration object
    private Configuration configuration;

    /**
     * @methodName run
     * @author YUAN_HAO
     * @date 5/7/2020 8:45 AM
     * @description
     * @param args
     * @return int
     */
    @Override
    public int run(String[] args) throws Exception {
        // Get Job object
        Job job= Job.getInstance(configuration);

        // Set drive class path
        job.setJarByClass(this.getClass());

        // Set Mapper and Reduce class
        job.setMapperClass(FruitsMapper.class);
        // args1: tableName, args2: reducerClass, args3: jobObject
        TableMapReduceUtil.initTableReducerJob(args[1], FruitsReducer.class, job);

        // Set the key and value types of output data in map phase
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Set input parameter; args: Source file path
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // Submit job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    /**
     * @methodName main
     * @author YUAN_HAO
     * @date 5/7/2020 8:44 AM
     * @description main method
     * @param args
     * @return void
     */
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        // Set column family and column qualifier
        configuration.set("columnFamily_columnQualifier", args[2]);

        try {
            int run = ToolRunner.run(configuration, new FruitsDriver(), args);

            System.exit(run);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

}
