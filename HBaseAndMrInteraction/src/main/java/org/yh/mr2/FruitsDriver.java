/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: FruitsDriver.java
 * File type: JAVA
 * Create time: 5/7/2020 10:49 AM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

/**
 * @author YUAN_HAO
 * @version 1.0
 * @className FruitsDriver
 * @time 5/7/2020 10:49 AM
	 * @category
 * @description
 */
public class FruitsDriver implements Serializable, Tool {
	static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog(FruitsDriver.class);
	private static final long serialVersionUID = 1L;
	// Defend configuration information
	private Configuration configuration;

	@Override
	public int run(String[] args) throws Exception {
		// 1.Get Job object
		Job job = Job.getInstance();

		// 2.Set drive class path
		job.setJarByClass(FruitsDriver.class);

		// 3.Set Mapper and KV types of output
		// args1: input table name, args2: Scan object,args3: FruitsMapper class, args4: output key class, args5: output value class, args6: job object
		TableMapReduceUtil.initTableMapperJob(args[0],
				new Scan(),
				FruitsMapper.class,
				ImmutableBytesWritable.class,
				Put.class,
				job);

		// 4.Set Reducer and table of output
		// args1: output table name, args2: FruitsReduces class, args3: job object
		TableMapReduceUtil.initTableReducerJob(args[1],
				FruitsReduces.class,
				job);

		// 5.Submit job
		return job.waitForCompletion(true) ? 0 : 1;
	}

	@Override
	public void setConf(Configuration conf) {
		this.configuration = conf;
	}

	@Override
	public Configuration getConf() {
		return configuration;
	}

	public static void main(String[] args) {
		Configuration configuration = new Configuration();

		try {
			ToolRunner.run(configuration, new FruitsDriver(), args);
		} catch (Exception e) {
			log.warn(e.getMessage(), e);
		}
	}
}
