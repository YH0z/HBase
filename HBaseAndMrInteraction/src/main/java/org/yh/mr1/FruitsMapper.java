/**
 * Copyright (C) 2020 The YuanHao Authors
 * File name: FruitsMapper.java
 * File type: JAVA
 * Create time: 5/6/2020 7:43 PM
 * Address: Chengdu, China
 * Email: 18228869131@163.com
 */
package org.yh.mr1;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Serializable;

/**
 * @className FruitsMapper
 * @author YUAN_HAO
 * @time 5/6/2020 7:43 PM
 * @category 
 * @description  
 * @version 1.0
 */
public class FruitsMapper extends Mapper<LongWritable, Text, LongWritable, Text> implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * @methodName map
     * @author YUAN_HAO
     * @date 5/6/2020 7:57 PM
     * @description
     * @param key
     * @param value
     * @param context
     * @return void
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
